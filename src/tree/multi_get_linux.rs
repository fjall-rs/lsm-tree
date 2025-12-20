use crate::table::filter::block::FilterBlock;
use crate::table::{Block, BlockHandle, PureGetIo, PureGetOutput};
use crate::tree::multi_get_linux::iouring::{
    push_multi_get_filter_read_block, submit_and_wait, CompletionOutput,
};
use crate::value::{UserKey, UserValue};
use crate::version::Version;
use crate::{CompressionType, InternalValue, SeqNo, Slice, Table};
use byteview::{Builder, ByteView};
use std::collections::VecDeque;
use std::fs::File;
use std::sync::Arc;

pub enum PendingOpVariant {
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
    ReadyValue {
        value: InternalValue,
    },
}

pub struct PendingOp<'a> {
    table: &'a Table,
    submitted: bool,
    variant: PendingOpVariant,
}

enum KeyState {
    Searching,            // Still looking, need more table candidates
    Found(InternalValue), // Value found and resolved
    NotFound,             // Exhausted all candidates, not found
    Error(crate::Error),  // Error occurred
}

impl KeyState {
    fn is_finished(&self) -> bool {
        matches!(
            self,
            KeyState::Found(_) | KeyState::NotFound | KeyState::Error(_)
        )
    }
}

pub(crate) fn multi_get(
    version: &Version,
    keys_and_indices: &[(usize, &[u8])],
    seqno: SeqNo,
    resolve: impl FnMut(InternalValue, usize),
) -> crate::Result<()> {
    let num_keys = keys_and_indices.len();
    if num_keys > u32::MAX as usize {
        panic!("too many keys to multi-get"); // todo return normal error
    }

    let mut key_states: Vec<KeyState> = {
        let mut v = Vec::with_capacity(num_keys);
        v.extend(std::iter::repeat_with(|| KeyState::Searching).take(num_keys));
        v
    };
    let mut op_queues: Vec<VecDeque<PendingOp>> = {
        let mut v = Vec::with_capacity(num_keys);
        v.extend(std::iter::repeat_with(VecDeque::new).take(num_keys));
        v
    };
    let mut ops_in_flight: usize = 0;
    let mut table_iter = version.iter_levels().flat_map(|lvl| lvl.iter());

    loop {
        // Process queues
        match batch_process(
            keys_and_indices,
            &mut key_states,
            &mut op_queues,
            &mut ops_in_flight,
            false,
        ) {
            Ok(BatchAction::AllResolved) => {
                assert_eq!(
                    ops_in_flight, 0,
                    "Some operations still in flight while all keys are resolved"
                );
                return finalize_results(key_states, keys_and_indices, resolve);
            }
            Ok(BatchAction::NeedsTable(_key_idx)) => {
                // Some keys need table candidates - continue to enrichment
            }
            Err(BatchError::Submit(err)) => {
                drain_in_flight(&mut op_queues, &mut ops_in_flight);
                return Err(err.into());
            }
            Err(BatchError::Completion) => {
                drain_in_flight(&mut op_queues, &mut ops_in_flight);
                return finalize_results(key_states, keys_and_indices, resolve);
            }
        }

        // Try to get next table for enrichment
        let Some(table) = table_iter.next() else {
            // No more tables - wait for remaining operations
            return match wait_all_pending(
                keys_and_indices,
                &mut key_states,
                &mut op_queues,
                &mut ops_in_flight,
            ) {
                Ok(_) | Err(BatchError::Completion) => {
                    finalize_results(key_states, keys_and_indices, resolve)
                }
                Err(BatchError::Submit(err)) => Err(err.into()),
            };
        };

        // Probe table for keys that need enrichment
        for ((_external_idx, key), (op_queue, key_state)) in keys_and_indices
            .iter()
            .zip(op_queues.iter_mut().zip(key_states.iter_mut()))
        {
            // Skip if already finished
            if key_state.is_finished() {
                continue;
            }

            // Skip if queue already has a ready value at the end
            if let Some(last) = op_queue.back() {
                if matches!(last.variant, PendingOpVariant::ReadyValue { .. }) {
                    continue;
                }
            }

            // Check if table may contain the key
            let Some(table) = table.get_for_key(key) else {
                continue;
            };

            let key_hash = crate::table::filter::standard_bloom::Builder::get_hash(key);
            match table.pure_get(key, seqno, key_hash)? {
                PureGetOutput::Pure(Some(value)) => {
                    // Queue the ready value
                    op_queue.push_back(PendingOp {
                        table,
                        submitted: true,
                        variant: PendingOpVariant::ReadyValue { value },
                    });
                }
                PureGetOutput::Pure(None) => {
                    continue;
                }
                PureGetOutput::Io(PureGetIo::FilterBlockFd { block_handle }) => {
                    op_queue.push_back(PendingOp {
                        table,
                        submitted: false,
                        variant: PendingOpVariant::FilterBlockOpenFd { block_handle },
                    });
                }
                PureGetOutput::Io(PureGetIo::FilterBlockRead { block_handle, file }) => {
                    op_queue.push_back(PendingOp {
                        table,
                        variant: PendingOpVariant::FilterBlockRead {
                            block_handle,
                            file,
                            buf: unsafe { Slice::builder_unzeroed(block_handle.size() as usize) },
                            read: 0,
                        },
                        submitted: false,
                    });
                }
                PureGetOutput::Io(PureGetIo::PointRead(point_read_io)) => {
                    todo!()
                }

                // PureGetOutput::Io(Io::PointRead) => {
                //
                //     op_queue.push_back(PendingOp {
                //         table,
                //         variant: PendingOpVariant::PointRead {},
                //         submitted: false,
                //     });
                // }
            }
        }
    }
}

fn drain_in_flight(op_queues: &mut [VecDeque<PendingOp>], ops_in_flight: &mut usize) {
    while *ops_in_flight > 0 {
        iouring::on_completion(|output| match output {
            CompletionOutput::MultiGetFilterTableOpenFd { key_idx, fd } => {
                *ops_in_flight -= 1;
                if let Ok(fd) = fd {
                    if let Some(slot) = op_queues[key_idx as usize].front() {
                        let file = Arc::new(fd);
                        slot.table
                            .descriptor_table
                            .insert_for_table(slot.table.global_id(), file);
                    }
                }
            }
            CompletionOutput::MultiGetFilterReadBlock { .. } => {
                *ops_in_flight -= 1;
            }
        });
        iouring::sync_completion();
    }
}

fn finalize_results(
    key_states: Vec<KeyState>,
    keys_and_indices: &[(usize, &[u8])],
    mut resolve: impl FnMut(InternalValue, usize),
) -> crate::Result<()> {
    // Process final states and resolve any values we haven't resolved yet
    for (state, (external_idx, _)) in key_states.into_iter().zip(keys_and_indices.iter()) {
        match state {
            KeyState::Found(value) => {
                resolve(value, *external_idx);
            }
            KeyState::NotFound => {
                // Key not found - caller handles this
            }
            KeyState::Error(err) => {
                // Return first error
                return Err(err);
            }
            KeyState::Searching => {
                // Should not happen - all Searching should become NotFound or Error
                unreachable!("Key still in Searching state at finalization")
            }
        }
    }
    Ok(())
}

enum BatchError {
    Submit(std::io::Error),
    Completion,
}

fn wait_all_pending(
    keys_and_indices: &[(usize, &[u8])],
    key_states: &mut [KeyState],
    op_queues: &mut Vec<VecDeque<PendingOp>>,
    ops_in_flight: &mut usize,
) -> Result<(), BatchError> {
    loop {
        match batch_process(keys_and_indices, key_states, op_queues, ops_in_flight, true) {
            Ok(BatchAction::AllResolved) => return Ok(()),
            Ok(BatchAction::NeedsTable(_key_idx)) => {
                unreachable!()
            }
            Err(err) => {
                drain_in_flight(op_queues, ops_in_flight);
                return Err(err);
            }
        }
    }
}

enum BatchAction {
    NeedsTable(usize),
    AllResolved,
}

fn batch_process(
    keys_and_indices: &[(usize, &[u8])],
    key_states: &mut [KeyState],
    op_queues: &mut [VecDeque<PendingOp>],
    ops_in_flight: &mut usize,
    no_more_tables: bool,
) -> Result<BatchAction, BatchError> {
    loop {
        let mut need_submit = false;
        let mut finished_count = 0;

        for (idx, (queue, state)) in op_queues.iter_mut().zip(key_states.iter_mut()).enumerate() {
            if state.is_finished() {
                finished_count += 1;
                continue;
            }

            let pending = match queue.front_mut() {
                None if no_more_tables => {
                    *state = KeyState::NotFound;
                    finished_count += 1;
                    continue;
                }
                // If queue is empty, need more table candidates
                None => return Ok(BatchAction::NeedsTable(idx)),
                Some(pending) => pending,
            };

            // Handle ReadyValue - only resolve if it's at front of queue
            if let PendingOpVariant::ReadyValue { value } = &mut pending.variant {
                *state = KeyState::Found(std::mem::replace(
                    value,
                    InternalValue {
                        key: crate::key::InternalKey {
                            user_key: UserKey::empty(),
                            seqno: 0,
                            value_type: crate::ValueType::Value,
                        },
                        value: UserValue::empty(),
                    },
                ));
                finished_count += 1;
                continue;
            }

            if pending.submitted {
                continue;
            }

            match pending {
                PendingOp {
                    table,
                    submitted,
                    variant: PendingOpVariant::FilterBlockOpenFd { .. },
                } => {
                    if iouring::push_multi_get_filter_table_open_fd(idx as u32, &table.path).is_ok()
                    {
                        *submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                PendingOp {
                    submitted,
                    variant:
                        PendingOpVariant::FilterBlockRead {
                            block_handle,
                            file,
                            buf,
                            read,
                        },
                    ..
                } => {
                    if push_multi_get_filter_read_block(
                        idx as u32,
                        file,
                        block_handle.offset().0 + *read as u64,
                        &mut buf[*read as usize..],
                    )
                    .is_ok()
                    {
                        *submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                _ => unimplemented!(),
            }
        }
        if finished_count == keys_and_indices.len() {
            return Ok(BatchAction::AllResolved);
        }

        let mut batch_error = None;
        let mut break_completion_loop = false;

        while batch_error.is_none() && !break_completion_loop {
            if need_submit {
                match submit_and_wait(1) {
                    Err(err) => return Err(BatchError::Submit(err)),
                    Ok(iouring::SubmitStatus::Submitted) => need_submit = false,
                    Ok(iouring::SubmitStatus::NeedDrainCompletion) => {}
                }
            }

            iouring::on_completion(|output| match output {
                CompletionOutput::MultiGetFilterTableOpenFd { key_idx, fd } => {
                    *ops_in_flight -= 1;
                    match fd {
                        Err(error) => {
                            key_states[key_idx as usize] = KeyState::Error(error.into());
                            batch_error = Some(BatchError::Completion)
                        }
                        Ok(fd) => {
                            let slot = op_queues[key_idx as usize].front_mut().unwrap();
                            let PendingOpVariant::FilterBlockOpenFd { block_handle } = slot.variant
                            else {
                                unreachable!()
                            };

                            let file = Arc::new(fd);
                            slot.table
                                .descriptor_table
                                .insert_for_table(slot.table.global_id(), file.clone());

                            let buf =
                                unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                            slot.submitted = false;
                            slot.variant = PendingOpVariant::FilterBlockRead {
                                block_handle,
                                file,
                                buf,
                                read: 0,
                            };
                            break_completion_loop = true;
                        }
                    }
                }
                CompletionOutput::MultiGetFilterReadBlock { key_idx, read } => {
                    *ops_in_flight -= 1;
                    match read {
                        Err(error) => {
                            key_states[key_idx as usize] = KeyState::Error(error.into());
                            batch_error = Some(BatchError::Completion)
                        }
                        Ok(comp_read) => {
                            let slot = op_queues[key_idx as usize].front_mut().unwrap();
                            let PendingOpVariant::FilterBlockRead {
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
                                break_completion_loop = true;
                                return;
                            }
                            let builder = std::mem::replace(buf, Builder::new(ByteView::new(&[])));
                            let slice = builder.freeze().into();
                            match Block::from_slice(slice, *block_handle, CompressionType::None) {
                                Err(error) => {
                                    key_states[key_idx as usize] = KeyState::Error(error.into());
                                    batch_error = Some(BatchError::Completion)
                                }
                                Ok(block) => {
                                    slot.table.cache.insert_block(
                                        slot.table.global_id(),
                                        block_handle.offset(),
                                        block.clone(),
                                    );
                                    let block = FilterBlock::new(block);
                                    let key_hash =
                                        crate::table::filter::standard_bloom::Builder::get_hash(
                                            keys_and_indices[key_idx as usize].1,
                                        );
                                    match block.maybe_contains_hash(key_hash) {
                                        Err(error) => {
                                            key_states[key_idx as usize] =
                                                KeyState::Error(error.into());
                                            batch_error = Some(BatchError::Completion)
                                        }
                                        Ok(false) => {
                                            #[cfg(feature = "metrics")]
                                            slot.table
                                                .metrics
                                                .io_skipped_by_filter
                                                .fetch_add(1, Relaxed);
                                            op_queues[key_idx as usize].pop_front();
                                            break_completion_loop = true;
                                        }
                                        Ok(true) => {
                                            std::hint::black_box(&mut batch_error);
                                            std::hint::black_box(&mut break_completion_loop);
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

        match (batch_error, break_completion_loop) {
            (Some(err), _) => return Err(err),
            (_, true) => continue,
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
    use std::cell::LazyCell;
    use std::fs::File;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    #[repr(u8)]
    pub enum Domain {
        MultiGet = 0,
    }

    impl Domain {
        const fn from_u8(v: u8) -> Option<Self> {
            match v {
                0 => Some(Domain::MultiGet),
                _ => None,
            }
        }
    }

    #[repr(u8)]
    pub enum MultiGetOp {
        FilterTableOpenFd = 0,
        FilterReadBlock = 1,
    }

    impl MultiGetOp {
        const fn from_u8(v: u8) -> Option<Self> {
            match v {
                0 => Some(MultiGetOp::FilterTableOpenFd),
                1 => Some(MultiGetOp::FilterReadBlock),
                _ => None,
            }
        }
    }

    pub enum CompletionOutput {
        MultiGetFilterTableOpenFd {
            key_idx: u32,
            fd: Result<std::fs::File, std::io::Error>,
        },
        MultiGetFilterReadBlock {
            key_idx: u32,
            read: Result<u32, std::io::Error>,
        },
    }

    pub enum SubmitStatus {
        Submitted,
        NeedDrainCompletion,
    }

    std::thread_local! {
        static IO_URING: LazyCell<IoUring> = LazyCell::new(|| {
            IoUring::new(256).expect("Failed to create io_uring instance")
        });
    }

    #[allow(unused)]
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

    fn pack_user_data(domain: Domain, op: MultiGetOp, key_idx: u32) -> u64 {
        let domain = domain as u64;
        let op = op as u64;
        let key_idx = key_idx as u64;

        (key_idx << 32) | (op << 8) | domain
    }

    fn parse_user_data<F, T>(user_data: u64, mut f: F) -> T
    where
        F: FnMut(Domain, u64) -> T,
    {
        let domain = Domain::from_u8((user_data & 0xFF) as u8).expect("unknown domain");
        f(domain, user_data)
    }

    pub fn push_multi_get_filter_table_open_fd(
        key_idx: u32,
        path: &PathBuf,
    ) -> Result<(), PushError> {
        IO_URING.with(|io_uring| {
            let user_data =
                pack_user_data(Domain::MultiGet, MultiGetOp::FilterTableOpenFd, key_idx);

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
        key_idx: u32,
        file: &File,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), PushError> {
        IO_URING.with(|io_uring| {
            let user_data = pack_user_data(Domain::MultiGet, MultiGetOp::FilterReadBlock, key_idx);

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
                let user_data = cqe.user_data().u64_();

                parse_user_data(user_data, |domain, user_data| match domain {
                    Domain::MultiGet => {
                        let op = MultiGetOp::from_u8(((user_data >> 8) & 0xFF) as u8)
                            .expect("unknown op");
                        let key_idx = (user_data >> 32) as u32;

                        match op {
                            MultiGetOp::FilterTableOpenFd => {
                                let res = cqe.raw_result();
                                let fd = if res >= 0 {
                                    Ok(unsafe { std::fs::File::from_raw_fd(res) })
                                } else {
                                    Err(std::io::Error::from_raw_os_error(-res))
                                };
                                cb(CompletionOutput::MultiGetFilterTableOpenFd { key_idx, fd })
                            }
                            MultiGetOp::FilterReadBlock => {
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
                    }
                });
            })
        })
    }

    pub fn sync_completion() {
        IO_URING.with(|ring| unsafe { ring.completion_shared().sync() })
    }
}
