// Copyright (c) 2024-present, fjall-rs 
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{
    alloc::Layout,
    mem::offset_of,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Mutex,
    },
};

// DEFAULT_BUFFER_SIZE needs to be at least big enough for one fullly-aligned node
// for the crate to work correctly. Anything larger than that will work.
//
// TODO: Justify this size.
const DEFAULT_BUFFER_SIZE: usize = (32 << 10) - size_of::<AtomicUsize>();

impl<const BUFFER_SIZE: usize> Default for Arenas<BUFFER_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<const N: usize> Send for Arenas<N> {}
unsafe impl<const N: usize> Sync for Arenas<N> {}

pub(crate) struct Arenas<const BUFFER_SIZE: usize = DEFAULT_BUFFER_SIZE> {
    // The current set of Arenas
    arenas: Mutex<Vec<*mut Buffer<BUFFER_SIZE>>>,
    // Cache of the currently open Arena. It'll be the last item in the buffers
    // vec. This atomic is only ever written while holding the buffers Mutex.
    open_arena: AtomicPtr<Buffer<BUFFER_SIZE>>,
}

impl<const BUFFER_SIZE: usize> Arenas<BUFFER_SIZE> {
    pub(crate) fn new() -> Self {
        Self {
            arenas: Default::default(),
            open_arena: AtomicPtr::default(),
        }
    }
}

impl<const BUFFER_SIZE: usize> Arenas<BUFFER_SIZE> {
    pub(crate) fn alloc(&self, layout: Layout) -> *mut u8 {
        loop {
            let buffer_tail = self.open_arena.load(Ordering::Acquire);
            if !buffer_tail.is_null() {
                if let Some(offset) = try_alloc(buffer_tail, layout) {
                    return offset;
                }
            }
            let mut buffers = self.arenas.lock().unwrap();
            let buffer = buffers.last().unwrap_or(&std::ptr::null_mut());
            if *buffer != buffer_tail {
                // Lost the race with somebody else.
                continue;
            }
            let new_buffer: Box<Buffer<BUFFER_SIZE>> = Box::new(Buffer::default());
            let new_buffer = Box::into_raw(new_buffer);
            self.open_arena.store(new_buffer, Ordering::Release);
            buffers.push(new_buffer);
        }
    }
}

struct Buffer<const N: usize> {
    offset: AtomicUsize,
    data: [u8; N],
}

impl<const N: usize> Default for Buffer<N> {
    fn default() -> Self {
        Self {
            offset: Default::default(),
            data: [0; N],
        }
    }
}

impl<const N: usize> Drop for Arenas<N> {
    fn drop(&mut self) {
        let mut buffers = self.arenas.lock().unwrap();
        for buffer in buffers.drain(..) {
            drop(unsafe { Box::from_raw(buffer) })
        }
    }
}

fn try_alloc<const N: usize>(buf: *mut Buffer<N>, layout: Layout) -> Option<*mut u8> {
    let mut cur_offset = unsafe { &(*buf).offset }.load(Ordering::Relaxed);
    loop {
        let buf_start = unsafe { buf.byte_add(offset_of!(Buffer<N>, data)) as *mut u8 };
        let free_start = unsafe { buf_start.byte_add(cur_offset) };
        let start_addr = unsafe { free_start.byte_add(free_start.align_offset(layout.align())) };
        let new_offset = ((start_addr as usize) + layout.size()) - (buf_start as usize);
        if new_offset > N {
            return None;
        }

        // Note that we can get away with using relaxed ordering here because we're not
        // asserting anything about the contents of the buffer. We're just trying to
        // allocate a new node.
        match unsafe { &(*buf).offset }.compare_exchange(
            cur_offset,
            new_offset,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_offset) => return Some(start_addr),
            Err(offset) => cur_offset = offset,
        }
    }
}
