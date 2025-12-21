use crate::iouring::{self, submit_and_wait, CompletionOutput};
use crate::table::block::BlockType;
use crate::table::block_index::{BlockIndexPureIter, BlockIndexPureIterImpl};
use crate::table::filter::block::FilterBlock;
use crate::table::pure::{PointReadIo, PointReadPureOutput};
use crate::table::util::BlockOutput;
use crate::table::DataBlock;
use crate::table::KeyedBlockHandle;
use crate::table::{Block, BlockHandle, PureGetIo, PureGetOutput};
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
    PointIndexOpen {
        pure_iter: Option<Box<BlockIndexPureIterImpl>>,
    },
    PointIndexRead {
        pure_iter: Option<Box<BlockIndexPureIterImpl>>,
        block_handle: BlockHandle,
        file: Arc<File>,
        buf: Builder,
        read: u32,
    },
    PointDataOpen {
        pure_iter: Option<Box<BlockIndexPureIterImpl>>,
        block_handle: KeyedBlockHandle,
    },
    PointDataRead {
        pure_iter: Option<Box<BlockIndexPureIterImpl>>,
        block_handle: KeyedBlockHandle,
        file: Arc<File>,
        buf: Builder,
        read: u32,
    },
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
            seqno,
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
                seqno,
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
                PureGetOutput::Io(PureGetIo::PointRead(point_io)) => {
                    let variant = match point_io {
                        PointReadIo::ExpectIndexFileOpen { pure_iter } => {
                            PendingOpVariant::PointIndexOpen {
                                pure_iter: Some(Box::new(pure_iter)),
                            }
                        }
                        PointReadIo::ExpectIndexBlockRead {
                            pure_iter,
                            block_handle,
                            file,
                        } => {
                            let buf =
                                unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                            PendingOpVariant::PointIndexRead {
                                pure_iter: Some(Box::new(pure_iter)),
                                block_handle,
                                file,
                                buf,
                                read: 0,
                            }
                        }
                        PointReadIo::ExpectDataFileOpen {
                            pure_iter,
                            block_handle,
                        } => PendingOpVariant::PointDataOpen {
                            pure_iter: Some(Box::new(pure_iter)),
                            block_handle,
                        },
                        PointReadIo::ExpectDataBlockRead {
                            pure_iter,
                            block_handle,
                            file,
                        } => {
                            let buf =
                                unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                            PendingOpVariant::PointDataRead {
                                pure_iter: Some(Box::new(pure_iter)),
                                block_handle,
                                file,
                                buf,
                                read: 0,
                            }
                        }
                    };
                    op_queue.push_back(PendingOp {
                        table,
                        submitted: false,
                        variant,
                    });
                }
            }
        }
    }
}

fn drain_in_flight(op_queues: &mut [VecDeque<PendingOp>], ops_in_flight: &mut usize) {
    while *ops_in_flight > 0 {
        iouring::on_completion(|output| {
            match output {
            CompletionOutput::MultiGetOpenFd { key_idx, fd } => {
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
            CompletionOutput::MultiGetReadBlock { read: Err(_), .. } => {
                *ops_in_flight -= 1;
            }
            CompletionOutput::MultiGetReadBlock {
                key_idx,
                read: Ok(read),
            } => {
                let Some(slot) = op_queues[key_idx as usize].pop_front() else {
                    unreachable!()
                };
                match slot.variant {
                    PendingOpVariant::FilterBlockOpenFd { .. }
                    | PendingOpVariant::PointIndexOpen { .. }
                    | PendingOpVariant::PointDataOpen { .. }
                    | PendingOpVariant::ReadyValue { .. } => unreachable!(),

                    PendingOpVariant::FilterBlockRead {
                        block_handle,
                        buf,
                        read: curr_read,
                        ..
                    } if curr_read + read == block_handle.size() => {
                        let slice = buf.freeze().into();
                        if let Ok(block) =
                            Block::from_slice(slice, block_handle, CompressionType::None)
                        {
                            slot.table.cache.insert_block(
                                slot.table.global_id(),
                                block_handle.offset(),
                                block.clone(),
                            );
                        }
                    }
                    PendingOpVariant::PointDataRead {
                        block_handle,
                        buf,
                        read: curr_read,
                        ..
                    } if curr_read + read == block_handle.size() => {
                        let slice = buf.freeze().into();
                        let offset = block_handle.offset();
                        if let Ok(block) = Block::from_slice(
                            slice,
                            block_handle.into_inner(),
                            slot.table.metadata.data_block_compression,
                        ) {
                            slot.table.cache.insert_block(
                                slot.table.global_id(),
                                offset,
                                block.clone(),
                            );
                        }
                    }
                    PendingOpVariant::PointIndexRead {
                        block_handle,
                        buf,
                        read: curr_read,
                        ..
                    } if curr_read + read == block_handle.size() => {
                        let slice = buf.freeze().into();
                        if let Ok(block) = Block::from_slice(
                            slice,
                            block_handle,
                            slot.table.metadata.index_block_compression,
                        ) {
                            slot.table.cache.insert_block(
                                slot.table.global_id(),
                                block_handle.offset(),
                                block.clone(),
                            );
                        }
                    }
                    _ => {}
                }
                *ops_in_flight -= 1;
            }
            _ => panic!("Unexpected completion output, other domain completions should not occur during multi-get processing"), // todo we may process blob reads right after key resolution
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
    seqno: SeqNo,
    key_states: &mut [KeyState],
    op_queues: &mut Vec<VecDeque<PendingOp>>,
    ops_in_flight: &mut usize,
) -> Result<(), BatchError> {
    loop {
        match batch_process(
            keys_and_indices,
            seqno,
            key_states,
            op_queues,
            ops_in_flight,
            true,
        ) {
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
    seqno: SeqNo,
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

            match &mut pending.variant {
                PendingOpVariant::FilterBlockOpenFd { .. }
                | PendingOpVariant::PointIndexOpen { .. }
                | PendingOpVariant::PointDataOpen { .. } => {
                    if iouring::push_multi_get_open_fd(idx as u32, &pending.table.path).is_ok() {
                        pending.submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                PendingOpVariant::FilterBlockRead {
                    block_handle,
                    file,
                    buf,
                    read,
                    ..
                } => {
                    if iouring::push_multi_get_read_block(
                        idx as u32,
                        file,
                        block_handle.offset().0 + *read as u64,
                        &mut buf[*read as usize..],
                    )
                    .is_ok()
                    {
                        pending.submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                PendingOpVariant::PointIndexRead {
                    block_handle,
                    file,
                    buf,
                    read,
                    ..
                } => {
                    if iouring::push_multi_get_read_block(
                        idx as u32,
                        file,
                        block_handle.offset().0 + *read as u64,
                        &mut buf[*read as usize..],
                    )
                    .is_ok()
                    {
                        pending.submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                PendingOpVariant::PointDataRead {
                    block_handle,
                    file,
                    buf,
                    read,
                    ..
                } => {
                    if iouring::push_multi_get_read_block(
                        idx as u32,
                        file,
                        block_handle.offset().0 + *read as u64,
                        &mut buf[*read as usize..],
                    )
                    .is_ok()
                    {
                        pending.submitted = true;
                        *ops_in_flight += 1;
                        need_submit = true;
                    }
                }
                _ => unreachable!(),
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

            iouring::on_completion(|output| {
                match output {
                CompletionOutput::MultiGetOpenFd { key_idx, fd } => {
                    *ops_in_flight -= 1;
                    let key_idx = key_idx as usize;
                    let slot = op_queues[key_idx].front_mut().unwrap();
                    let key = keys_and_indices[key_idx].1;
                    let table = slot.table;
                    let local_seqno = seqno.saturating_sub(table.global_seqno());
                    match fd {
                        Err(error) => {
                            key_states[key_idx] = KeyState::Error(error.into());
                            batch_error = Some(BatchError::Completion)
                        }
                        Ok(fd) => {
                            let file = Arc::new(fd);
                            match &mut slot.variant {
                                PendingOpVariant::FilterBlockOpenFd { block_handle } => {
                                    table
                                        .descriptor_table
                                        .insert_for_table(table.global_id(), file.clone());
                                    let buf = unsafe {
                                        Slice::builder_unzeroed(block_handle.size() as usize)
                                    };
                                    slot.variant = PendingOpVariant::FilterBlockRead {
                                        block_handle: *block_handle,
                                        file,
                                        buf,
                                        read: 0,
                                    };
                                    slot.submitted = false;
                                    break_completion_loop = true;
                                }
                                PendingOpVariant::PointIndexOpen { pure_iter } => {
                                    pure_iter.as_mut().unwrap().supply_file(file);
                                    let res = table.resume_point_read_pure(
                                        *std::mem::take(pure_iter).unwrap(),
                                        key,
                                        local_seqno,
                                    );
                                    match res {
                                        Ok(None) => {
                                            op_queues[key_idx].pop_front();
                                            break_completion_loop = true;
                                        }
                                        Ok(Some(PointReadPureOutput::Value(value))) => {
                                            slot.variant = PendingOpVariant::ReadyValue { value };
                                            break_completion_loop = true;
                                        }
                                        Ok(Some(PointReadPureOutput::Io(io))) => {
                                            slot.variant = match io {
                                                PointReadIo::ExpectIndexFileOpen { pure_iter } => {
                                                    PendingOpVariant::PointIndexOpen {
                                                        pure_iter: Some(Box::new(pure_iter)),
                                                    }
                                                }
                                                PointReadIo::ExpectIndexBlockRead {
                                                    pure_iter,
                                                    block_handle,
                                                    file,
                                                } => {
                                                    let buf = unsafe {
                                                        Slice::builder_unzeroed(
                                                            block_handle.size() as usize
                                                        )
                                                    };
                                                    PendingOpVariant::PointIndexRead {
                                                        pure_iter: Some(Box::new(pure_iter)),
                                                        block_handle,
                                                        file,
                                                        buf,
                                                        read: 0,
                                                    }
                                                }
                                                PointReadIo::ExpectDataFileOpen {
                                                    pure_iter,
                                                    block_handle,
                                                } => PendingOpVariant::PointDataOpen {
                                                    pure_iter: Some(Box::new(pure_iter)),
                                                    block_handle,
                                                },
                                                PointReadIo::ExpectDataBlockRead {
                                                    pure_iter,
                                                    block_handle,
                                                    file,
                                                } => {
                                                    let buf = unsafe {
                                                        Slice::builder_unzeroed(
                                                            block_handle.size() as usize
                                                        )
                                                    };
                                                    PendingOpVariant::PointDataRead {
                                                        pure_iter: Some(Box::new(pure_iter)),
                                                        block_handle,
                                                        file,
                                                        buf,
                                                        read: 0,
                                                    }
                                                }
                                            };
                                            slot.submitted = false;
                                            break_completion_loop = true;
                                        }
                                        Err(error) => {
                                            key_states[key_idx] = KeyState::Error(error);
                                            batch_error = Some(BatchError::Completion);
                                        }
                                    }
                                }
                                PendingOpVariant::PointDataOpen {
                                    pure_iter,
                                    block_handle,
                                } => {
                                    pure_iter.as_mut().unwrap().supply_file(file.clone());
                                    let res = table
                                        .load_block_pure(block_handle.as_ref(), BlockType::Data);
                                    match res {
                                        BlockOutput::Block(block) => {
                                            let item_opt =
                                                DataBlock::new(block).point_read(key, local_seqno);
                                            if let Some(value) = item_opt {
                                                slot.variant =
                                                    PendingOpVariant::ReadyValue { value };
                                                break_completion_loop = true;
                                                return;
                                            }
                                            if *block_handle.end_key() > key {
                                                op_queues[key_idx].pop_front();
                                                break_completion_loop = true;
                                                return;
                                            }
                                            let res = table.resume_point_read_pure(
                                                *std::mem::take(pure_iter).unwrap(),
                                                key,
                                                local_seqno,
                                            );
                                            match res {
                                                Ok(None) => {
                                                    op_queues[key_idx].pop_front();
                                                }
                                                Ok(Some(PointReadPureOutput::Value(value))) => {
                                                    slot.variant =
                                                        PendingOpVariant::ReadyValue { value };
                                                }
                                                Ok(Some(PointReadPureOutput::Io(io))) => {
                                                    slot.variant = match io {
                                                        // unlikely, but
                                                        PointReadIo::ExpectIndexFileOpen {
                                                            pure_iter,
                                                        } => PendingOpVariant::PointIndexOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                        },
                                                        PointReadIo::ExpectIndexBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointIndexRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                        PointReadIo::ExpectDataFileOpen {
                                                            pure_iter,
                                                            block_handle,
                                                        } => PendingOpVariant::PointDataOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                            block_handle,
                                                        },
                                                        PointReadIo::ExpectDataBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointDataRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                    };
                                                    slot.submitted = false;
                                                }
                                                Err(error) => {
                                                    key_states[key_idx] = KeyState::Error(error);
                                                    batch_error = Some(BatchError::Completion);
                                                }
                                            }
                                            break_completion_loop = true;
                                        }
                                        BlockOutput::ReadBlock(file) => {
                                            let buf = unsafe {
                                                Slice::builder_unzeroed(block_handle.size() as usize)
                                            };
                                            slot.variant = PendingOpVariant::PointDataRead {
                                                pure_iter: std::mem::take(pure_iter),
                                                block_handle: block_handle.clone(),
                                                file,
                                                buf,
                                                read: 0,
                                            };
                                            slot.submitted = false;
                                            break_completion_loop = true;
                                        }
                                        BlockOutput::OpenFd => {
                                            key_states[key_idx] =
                                                KeyState::Error(crate::Error::Unrecoverable);
                                            batch_error = Some(BatchError::Completion);
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                CompletionOutput::MultiGetReadBlock { key_idx, read } => {
                    *ops_in_flight -= 1;
                    let key_idx = key_idx as usize;
                    let slot = op_queues[key_idx].front_mut().unwrap();
                    let key = keys_and_indices[key_idx].1;
                    let table = slot.table;
                    let local_seqno = seqno.saturating_sub(table.global_seqno());
                    match read {
                        Err(error) => {
                            key_states[key_idx] = KeyState::Error(error.into());
                            batch_error = Some(BatchError::Completion)
                        }
                        Ok(comp_read) => {
                            match &mut slot.variant {
                                PendingOpVariant::FilterBlockRead {
                                    block_handle,
                                    buf,
                                    read: cur_read,
                                    ..
                                } => {
                                    *cur_read += comp_read;
                                    if block_handle.size() != *cur_read {
                                        slot.submitted = false;
                                        break_completion_loop = true;
                                        return;
                                    }
                                    let builder =
                                        std::mem::replace(buf, Builder::new(ByteView::new(&[])));
                                    let slice = builder.freeze().into();
                                    match Block::from_slice(
                                        slice,
                                        *block_handle,
                                        CompressionType::None,
                                    ) {
                                        Err(error) => {
                                            key_states[key_idx] = KeyState::Error(error.into());
                                            batch_error = Some(BatchError::Completion)
                                        }
                                        Ok(block) => {
                                            slot.table.cache.insert_block(
                                                slot.table.global_id(),
                                                block_handle.offset(),
                                                block.clone(),
                                            );
                                            let block = FilterBlock::new(block);
                                            let key_hash = crate::table::filter::standard_bloom::Builder::get_hash(key);
                                            match block.maybe_contains_hash(key_hash) {
                                                Err(error) => {
                                                    key_states[key_idx] =
                                                        KeyState::Error(error.into());
                                                    batch_error = Some(BatchError::Completion)
                                                }
                                                Ok(false) => {
                                                    #[cfg(feature = "metrics")]
                                                    slot.table
                                                        .metrics
                                                        .io_skipped_by_filter
                                                        .fetch_add(1, Relaxed);
                                                    op_queues[key_idx].pop_front();
                                                    break_completion_loop = true;
                                                }
                                                Ok(true) => {
                                                    let res =
                                                        table.point_read_pure(key, local_seqno);
                                                    match res {
                                                        Ok(None) => {
                                                            op_queues[key_idx].pop_front();
                                                            break_completion_loop = true;
                                                        }
                                                        Ok(Some(PointReadPureOutput::Value(
                                                            value,
                                                        ))) => {
                                                            slot.variant =
                                                                PendingOpVariant::ReadyValue {
                                                                    value,
                                                                };
                                                            break_completion_loop = true;
                                                        }
                                                        Ok(Some(PointReadPureOutput::Io(io))) => {
                                                            slot.variant = match io {
                                                                PointReadIo::ExpectIndexFileOpen { pure_iter } => PendingOpVariant::PointIndexOpen { pure_iter: Some(Box::new(pure_iter)) },
                                                                PointReadIo::ExpectIndexBlockRead { pure_iter, block_handle, file } => {
                                                                    let buf = unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                                                                    PendingOpVariant::PointIndexRead { pure_iter: Some(Box::new(pure_iter)), block_handle, file, buf, read: 0 }
                                                                }
                                                                PointReadIo::ExpectDataFileOpen { pure_iter, block_handle } => PendingOpVariant::PointDataOpen { pure_iter: Some(Box::new(pure_iter)), block_handle },
                                                                PointReadIo::ExpectDataBlockRead { pure_iter, block_handle, file } => {
                                                                    let buf = unsafe { Slice::builder_unzeroed(block_handle.size() as usize) };
                                                                    PendingOpVariant::PointDataRead { pure_iter: Some(Box::new(pure_iter)), block_handle, file, buf, read: 0 }
                                                                }
                                                            };
                                                            slot.submitted = false;
                                                            break_completion_loop = true;
                                                        }
                                                        Err(error) => {
                                                            key_states[key_idx] =
                                                                KeyState::Error(error);
                                                            batch_error =
                                                                Some(BatchError::Completion);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    };
                                }
                                PendingOpVariant::PointIndexRead {
                                    pure_iter,
                                    block_handle,
                                    buf,
                                    read: cur_read,
                                    file: _,
                                } => {
                                    *cur_read += comp_read;
                                    if block_handle.size() != *cur_read {
                                        slot.submitted = false;
                                        break_completion_loop = true;
                                        return;
                                    }
                                    let builder =
                                        std::mem::replace(buf, Builder::new(ByteView::new(&[])));
                                    let slice = builder.freeze().into();
                                    match Block::from_slice(
                                        slice,
                                        *block_handle,
                                        table.metadata.index_block_compression,
                                    ) {
                                        Err(error) => {
                                            key_states[key_idx] = KeyState::Error(error.into());
                                            batch_error = Some(BatchError::Completion)
                                        }
                                        Ok(block) => {
                                            table.cache.insert_block(
                                                table.global_id(),
                                                block_handle.offset(),
                                                block.clone(),
                                            );
                                            pure_iter
                                                .as_mut()
                                                .unwrap()
                                                .supply_block(*block_handle, block);
                                            let res = table.resume_point_read_pure(
                                                *std::mem::take(pure_iter).unwrap(),
                                                key,
                                                local_seqno,
                                            );
                                            match res {
                                                Ok(None) => {
                                                    op_queues[key_idx].pop_front();
                                                    break_completion_loop = true;
                                                }
                                                Ok(Some(PointReadPureOutput::Value(value))) => {
                                                    slot.variant =
                                                        PendingOpVariant::ReadyValue { value };
                                                    break_completion_loop = true;
                                                }
                                                Ok(Some(PointReadPureOutput::Io(io))) => {
                                                    slot.variant = match io {
                                                        PointReadIo::ExpectIndexFileOpen {
                                                            pure_iter,
                                                        } => PendingOpVariant::PointIndexOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                        },
                                                        PointReadIo::ExpectIndexBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointIndexRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                        PointReadIo::ExpectDataFileOpen {
                                                            pure_iter,
                                                            block_handle,
                                                        } => PendingOpVariant::PointDataOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                            block_handle,
                                                        },
                                                        PointReadIo::ExpectDataBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointDataRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                    };
                                                    slot.submitted = false;
                                                    break_completion_loop = true;
                                                }
                                                Err(error) => {
                                                    key_states[key_idx] = KeyState::Error(error);
                                                    batch_error = Some(BatchError::Completion);
                                                }
                                            }
                                        }
                                    };
                                }
                                PendingOpVariant::PointDataRead {
                                    pure_iter,
                                    block_handle,
                                    buf,
                                    read: cur_read,
                                    ..
                                } => {
                                    *cur_read += comp_read;
                                    if block_handle.size() != *cur_read {
                                        slot.submitted = false;
                                        break_completion_loop = true;
                                        return;
                                    }
                                    let builder =
                                        std::mem::replace(buf, Builder::new(ByteView::new(&[])));
                                    let slice = builder.freeze().into();
                                    match Block::from_slice(
                                        slice,
                                        block_handle.clone().into_inner(),
                                        table.metadata.data_block_compression,
                                    ) {
                                        Err(error) => {
                                            key_states[key_idx] = KeyState::Error(error.into());
                                            batch_error = Some(BatchError::Completion)
                                        }
                                        Ok(block) => {
                                            table.cache.insert_block(
                                                table.global_id(),
                                                block_handle.offset(),
                                                block.clone(),
                                            );
                                            let db = DataBlock::new(block);
                                            if let Some(value) = db.point_read(key, local_seqno) {
                                                slot.variant =
                                                    PendingOpVariant::ReadyValue { value };
                                                break_completion_loop = true;
                                                return;
                                            }
                                            if *block_handle.end_key() > key {
                                                op_queues[key_idx].pop_front();
                                                break_completion_loop = true;
                                                return;
                                            }
                                            let res = table.resume_point_read_pure(
                                                *std::mem::take(pure_iter).unwrap(),
                                                key,
                                                local_seqno,
                                            );
                                            match res {
                                                Ok(None) => {
                                                    op_queues[key_idx].pop_front();
                                                    break_completion_loop = true;
                                                }
                                                Ok(Some(PointReadPureOutput::Value(value))) => {
                                                    slot.variant =
                                                        PendingOpVariant::ReadyValue { value };
                                                    break_completion_loop = true;
                                                }
                                                Ok(Some(PointReadPureOutput::Io(io))) => {
                                                    slot.variant = match io {
                                                        PointReadIo::ExpectIndexFileOpen {
                                                            pure_iter,
                                                        } => PendingOpVariant::PointIndexOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                        },
                                                        PointReadIo::ExpectIndexBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointIndexRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                        PointReadIo::ExpectDataFileOpen {
                                                            pure_iter,
                                                            block_handle,
                                                        } => PendingOpVariant::PointDataOpen {
                                                            pure_iter: Some(Box::new(pure_iter)),
                                                            block_handle,
                                                        },
                                                        PointReadIo::ExpectDataBlockRead {
                                                            pure_iter,
                                                            block_handle,
                                                            file,
                                                        } => {
                                                            let buf = unsafe {
                                                                Slice::builder_unzeroed(
                                                                    block_handle.size() as usize,
                                                                )
                                                            };
                                                            PendingOpVariant::PointDataRead {
                                                                pure_iter: Some(Box::new(
                                                                    pure_iter,
                                                                )),
                                                                block_handle,
                                                                file,
                                                                buf,
                                                                read: 0,
                                                            }
                                                        }
                                                    };
                                                    slot.submitted = false;
                                                    break_completion_loop = true;
                                                }
                                                Err(error) => {
                                                    key_states[key_idx] = KeyState::Error(error);
                                                    batch_error = Some(BatchError::Completion);
                                                }
                                            }
                                        }
                                    };
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                _ =>            panic!("Unexpected completion output, other domain completions should not occur during multi-get processing"), // todo we may process blob reads right after key resolution
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
