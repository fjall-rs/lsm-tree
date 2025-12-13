use crate::table::filter::block::FilterBlock;
use crate::table::{Block, BlockHandle, Io, Output};
use crate::tree::multi_get_linux::iouring::{
    increase_request_id, push_multi_get_filter_read_block, submit_and_wait, CompletionOutput,
};
use crate::version::Version;
use crate::{CompressionType, InternalValue, SeqNo, Slice, Table};
use byteview::{Builder, ByteView};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Error;
use std::sync::Arc;

pub enum PendingIoVariant {
    FilterBlockOpenFd {
        block_handle: BlockHandle,
    },
    FilterBlockRead {
        block_handle: BlockHandle,
        file: Arc<File>,
        buf: Builder,
        read: u32,
    },
    PointRead {},
}

pub struct PendingIo<'a> {
    table: &'a Table,
    submitted: bool,
    variant: PendingIoVariant,
}

pub(crate) fn multi_get(
    version: &Version,
    keys_and_indices: &[(usize, &[u8])],
    seqno: SeqNo,
    mut resolve: impl FnMut(InternalValue, usize),
) -> crate::Result<()> {
    let num_keys = keys_and_indices.len();
    if num_keys > u16::MAX as usize {
        panic!("too many keys to multi-get"); // todo return normal error
    }
    let mut resolved = vec![false; num_keys];
    let mut io_queues: Vec<VecDeque<PendingIo>> = {
        let mut v = Vec::with_capacity(num_keys);
        v.extend(std::iter::repeat_with(VecDeque::new).take(num_keys));
        v
    };
    let mut resolved_count: usize = 0;
    let mut readings_in_flight: usize = 0;
    let mut table_iter = version.iter_levels().flat_map(|lvl| lvl.iter());
    increase_request_id();
    while !resolved
        .iter()
        .zip(&io_queues)
        .all(|(&resolved, queue)| resolved || !queue.is_empty())
    {
        let Some(table) = table_iter.next() else {
            wait_all_pending(resolved, &mut io_queues, &mut resolved_count, resolve);
            return Ok(());
        };
        // Probe table for EVERY unresolved key and queue if it's a candidate
        // We must do this for all keys because we won't get another chance
        // when this table iterator position is consumed
        for (((external_idx, key), io_queue), key_is_resolved) in keys_and_indices
            .iter()
            .zip(io_queues.iter_mut())
            .zip(resolved.iter_mut())
        {
            // First check if table may contain the key
            let Some(table) = table.get_for_key(key) else {
                continue;
            };
            // NOTE: Create key hash for hash sharing
            // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
            let key_hash = crate::table::filter::standard_bloom::Builder::get_hash(key);
            match table.pure_get(key, seqno, key_hash)? {
                Output::Pure(Some(value)) => {
                    *key_is_resolved = true;
                    resolved_count += 1;
                    resolve(value, *external_idx);
                    // clear any queued IO for that key
                    io_queue.clear();
                }
                Output::Pure(None) => {
                    continue;
                    // Table was a candidate but doesn't contain key, new candidate table will be queried in next iteration
                }
                Output::Io(Io::FilterBlockFd { block_handle }) => {
                    io_queue.push_back(PendingIo {
                        table,
                        submitted: false,
                        variant: PendingIoVariant::FilterBlockOpenFd { block_handle },
                    });
                }
                Output::Io(Io::FilterBlockRead { block_handle, file }) => {
                    io_queue.push_back(PendingIo {
                        table,
                        variant: PendingIoVariant::FilterBlockRead {
                            block_handle,
                            file,
                            buf: unsafe { Slice::builder_unzeroed(block_handle.size() as usize) },
                            read: 0,
                        },
                        submitted: false,
                    });
                }
                Output::Io(Io::PointRead) => {
                    io_queue.push_back(PendingIo {
                        table,
                        variant: PendingIoVariant::PointRead {},
                        submitted: false,
                    });
                }
            }
        }

        if resolved_count < num_keys {
            match batch_io(
                keys_and_indices,
                &mut resolved,
                &mut io_queues,
                &mut resolved_count,
                &mut readings_in_flight,
            ) {
                Ok(BatchAction::AllResolved) => break,
                Ok(BatchAction::NeedsTable(_usize)) => continue,
                Err(err) => {
                    todo!("wait for all pending IO reads before returning from the function to ensure no vectors passed to iouring are not dropped before completion");
                    return Err(err.into());
                }
            }
        }
    }

    Ok(())
}

fn wait_all_pending(
    _resolved: Vec<bool>,
    _io_queues: &mut Vec<VecDeque<PendingIo>>,
    _resolved_count: &mut usize,
    _resolve: impl FnMut(InternalValue, usize),
) {
    todo!()
}

enum BatchAction {
    NeedsTable(usize),
    AllResolved,
}

fn batch_io(
    keys_and_indices: &[(usize, &[u8])],
    resolved: &mut [bool],
    io_queues: &mut [VecDeque<PendingIo>],
    resolved_count: &mut usize,
    readings_in_flight: &mut usize,
) -> crate::Result<BatchAction> {
    loop {
        let mut need_submit = false;
        for (q, idx) in io_queues
            .iter_mut()
            .zip(keys_and_indices.iter())
            .zip(resolved.iter())
            .enumerate()
            .filter_map(|(idx, ((q, (_external_idx, _key)), resolved))| {
                (!resolved && q.front().is_some_and(|q| !q.submitted)).then_some((q, idx))
            })
        {
            let Some(q) = q.front_mut() else {
                return Ok(BatchAction::NeedsTable(idx));
            };
            match q {
                PendingIo {
                    table,
                    submitted,
                    variant: PendingIoVariant::FilterBlockOpenFd { .. },
                } => {
                    if iouring::push_multi_get_filter_table_open_fd(idx as u16, &table.path).is_ok()
                    {
                        *submitted = true;
                        need_submit = true;
                    }
                }
                PendingIo {
                    table: _,
                    submitted,
                    variant:
                        PendingIoVariant::FilterBlockRead {
                            block_handle,
                            file,
                            buf,
                            read,
                        },
                } => {
                    if push_multi_get_filter_read_block(
                        idx as u16,
                        &file,
                        block_handle.offset().0 + *read as u64,
                        &mut buf[*read as usize..],
                    )
                    .is_ok()
                    {
                        *submitted = true;
                        *readings_in_flight += 1;
                        need_submit = true;
                    }
                }

                _ => unimplemented!(),
            }
        }
        let mut batch_status = None;
        let mut err: Option<crate::Error> = None;
        let mut continue_top = false;
        while batch_status.is_none() && err.is_none() && !continue_top {
            if need_submit {
                match submit_and_wait(1)? {
                    iouring::SubmitStatus::Submitted => need_submit = false,
                    iouring::SubmitStatus::NeedDrainCompletion => {}
                }
            }
            iouring::on_completion(|output| match output {
                CompletionOutput::MultiGetFilterTableOpenFd { key_idx, fd } => match fd {
                    Err(error) => err = Some(error.into()),
                    Ok(fd) => {
                        let slot = io_queues[key_idx as usize].front_mut().unwrap();
                        let PendingIoVariant::FilterBlockOpenFd { block_handle } = slot.variant
                        else {
                            unreachable!()
                        };

                        let file = Arc::new(fd);
                        slot.table
                            .descriptor_table
                            .insert_for_table(slot.table.global_id(), file.clone());

                        let buf = unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                        slot.submitted = false;
                        slot.variant = PendingIoVariant::FilterBlockRead {
                            block_handle,
                            file,
                            buf,
                            read: 0,
                        };
                        continue_top = true;
                    }
                },
                CompletionOutput::MultiGetFilterReadBlock { key_idx, read } => {
                    *readings_in_flight -= 1;
                    match read {
                        Err(error) => err = Some(error.into()),
                        Ok(comp_read) => {
                            let slot = io_queues[key_idx as usize].front_mut().unwrap();
                            let PendingIoVariant::FilterBlockRead {
                                block_handle,
                                buf,
                                read,
                                ..
                            } = &mut slot.variant
                            else {
                                unreachable!()
                            };
                            *read += comp_read;
                            if block_handle.size() != *read {
                                slot.submitted = false;
                                continue_top = true;
                                return;
                            }
                            let builder = std::mem::replace(buf, Builder::new(ByteView::new(&[])));
                            let slice = builder.freeze().into();
                            // NOTE: We never write a filter block with compression
                            match Block::from_slice(slice, *block_handle, CompressionType::None) {
                                Err(error) => err = Some(error),
                                Ok(block) => {
                                    slot.table.cache.insert_block(
                                        slot.table.global_id(),
                                        block_handle.offset(),
                                        block.clone(),
                                    );
                                    let block = FilterBlock::new(block);
                                    // NOTE: Create key hash for hash sharing
                                    // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
                                    let key_hash =
                                        crate::table::filter::standard_bloom::Builder::get_hash(
                                            keys_and_indices[key_idx as usize].1,
                                        );
                                    match block.maybe_contains_hash(key_hash) {
                                        Err(error) => err = Some(error.into()),
                                        Ok(false) => {
                                            #[cfg(feature = "metrics")]
                                            self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
                                            io_queues[key_idx as usize].pop_front();
                                            continue_top = true;
                                        }
                                        Ok(true) => {
                                            todo!("point read")
                                        }
                                    }
                                }
                            };
                        }
                    }
                }
            });
            iouring::sync_completion();
        }
        match (batch_status, err, continue_top) {
            (_, Some(error), _) => return Err(error),
            (Some(status), _, _) => return Ok(status),
            (_, _, true) => continue, // continues top loop
            _ => unreachable!(),
        }
    }
}

mod iouring {
    use rustix::fs::CWD;
    use rustix::io::Errno;
    use rustix_uring::squeue::PushError;
    use rustix_uring::types::OFlags;
    use rustix_uring::{opcode, types, IoUring};
    use std::cell::{Cell, LazyCell};
    use std::fs::File;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    #[repr(u8)]
    pub enum Op {
        OpMultiGetFilterTableOpenFd = 0,
        OpMultiGetFilterReadBlock = 1,
    }

    impl Op {
        const fn from_u8(v: u8) -> Option<Self> {
            match v {
                0 => Some(Op::OpMultiGetFilterTableOpenFd),
                1 => Some(Op::OpMultiGetFilterReadBlock),
                _ => None,
            }
        }
    }

    pub enum CompletionOutput {
        MultiGetFilterTableOpenFd {
            key_idx: u16,
            fd: Result<std::fs::File, std::io::Error>,
        },
        MultiGetFilterReadBlock {
            key_idx: u16,
            read: Result<u32, std::io::Error>,
        },
    }

    pub enum SubmitStatus {
        Submitted,
        NeedDrainCompletion,
    }

    // Define a thread-local variable that holds a Lazy<IoUring>
    std::thread_local! {
        static IO_URING: LazyCell<IoUring> = LazyCell::new(|| {
            IoUring::new(256).expect("Failed to create io_uring instance")
        });
        static MULTI_GET_REQUEST_ID: Cell<u32> = Cell::new(0);
    }

    pub fn increase_request_id() -> u32 {
        MULTI_GET_REQUEST_ID.with(|id| {
            let res = id.get();
            id.replace(res.wrapping_add(1));
            res
        })
    }

    fn request_id() -> u32 {
        MULTI_GET_REQUEST_ID.get() - 1
    }

    pub fn submit() -> std::io::Result<SubmitStatus> {
        submit_and_wait(0)
    }

    pub fn submit_and_wait(want: usize) -> std::io::Result<SubmitStatus> {
        IO_URING.with(|ring| match ring.submitter().submit_and_wait(want) {
            Ok(_) => Ok(SubmitStatus::Submitted),

            Err(e) if e == Errno::BUSY => Ok(SubmitStatus::NeedDrainCompletion),

            Err(e) if e == Errno::INTR => Ok(SubmitStatus::Submitted),

            Err(e) => Err(std::io::Error::from_raw_os_error(e.raw_os_error())),
        })
    }

    pub fn push_multi_get_filter_table_open_fd(
        key_idx: u16,
        path: &PathBuf,
    ) -> Result<(), PushError> {
        IO_URING.with(|io_uring| {
            let key_idx = key_idx.to_le_bytes();
            let request_id = request_id().to_le_bytes();
            let user_data = [
                Op::OpMultiGetFilterTableOpenFd as u8,
                request_id[0],
                request_id[1],
                request_id[2],
                request_id[3],
                key_idx[0],
                key_idx[1],
                0,
            ];
            let user_data = u64::from_le_bytes(user_data);
            let open_sqe = opcode::OpenAt::new(
                types::Fd(CWD.as_raw_fd()),
                path.as_os_str().as_bytes().as_ptr().cast(),
            )
            .flags(OFlags::RDONLY)
            .build()
            .user_data(user_data);
            unsafe { io_uring.submission_shared().push(&open_sqe) }
        })
    }

    pub fn push_multi_get_filter_read_block(
        key_idx: u16,
        file: &File,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), PushError> {
        IO_URING.with(|io_uring| {
            let key_idx = key_idx.to_le_bytes();
            let request_id = request_id().to_le_bytes();
            let user_data = [
                Op::OpMultiGetFilterReadBlock as u8,
                request_id[0],
                request_id[1],
                request_id[2],
                request_id[3],
                key_idx[0],
                key_idx[1],
                0,
            ];
            let user_data = u64::from_le_bytes(user_data);
            let open_sqe = opcode::Read::new(
                types::Fd(file.as_raw_fd()),
                buf.as_mut_ptr(),
                buf.len() as u32,
            )
            .offset(offset)
            .build()
            .user_data(user_data);
            unsafe { io_uring.submission_shared().push(&open_sqe) }
        })
    }

    pub fn on_completion(mut cb: impl FnMut(CompletionOutput)) {
        IO_URING.with(|io_uring| {
            unsafe { io_uring.completion_shared() }.for_each(|cqe| {
                let user_data = cqe.user_data().u64_().to_le_bytes();
                let op = Op::from_u8(user_data[0]).expect("unknown completion"); // todo must be revisit if the same ring is used across codebase {
                match op {
                    Op::OpMultiGetFilterTableOpenFd => {
                        let loc_request_id =
                            u32::from_le_bytes(*user_data[1..5].first_chunk().unwrap());
                        // skip completions from previous requests
                        if loc_request_id != request_id() {
                            return;
                        }

                        let key_idx = u16::from_le_bytes(user_data[5..7].try_into().unwrap());
                        let res = cqe.raw_result();
                        let fd = if cqe.raw_result() >= 0 {
                            Ok(unsafe { std::fs::File::from_raw_fd(res) })
                        } else {
                            Err(std::io::Error::from_raw_os_error(-res))
                        };
                        cb(CompletionOutput::MultiGetFilterTableOpenFd { key_idx, fd })
                    }
                    Op::OpMultiGetFilterReadBlock => {
                        let loc_request_id =
                            u32::from_le_bytes(*user_data[1..5].first_chunk().unwrap());

                        // skip completions from previous requests
                        if loc_request_id != request_id() {
                            return;
                        }

                        let key_idx = u16::from_le_bytes(user_data[5..7].try_into().unwrap());
                        let res = cqe.raw_result();

                        if res >= 0 {
                            cb(CompletionOutput::MultiGetFilterReadBlock {
                                key_idx,
                                read: Ok(res as u32),
                            })
                        } else {
                            cb(CompletionOutput::MultiGetFilterReadBlock {
                                key_idx,
                                read: Err(std::io::Error::from_raw_os_error(-res)),
                            });
                        }
                    }
                }
            })
        })
    }

    pub fn sync_completion() {
        IO_URING.with(|ring| unsafe { ring.completion_shared().sync() })
    }
}
