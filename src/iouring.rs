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
    MultiBlob = 1,
}

impl Domain {
    const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Domain::MultiGet),
            1 => Some(Domain::MultiBlob),
            _ => None,
        }
    }
}

pub trait Op: Into<u64> + From<u8> {}

#[repr(u8)]
pub enum MultiGetOp {
    OpenFd = 0,
    ReadBlock = 1,
}

impl From<u8> for MultiGetOp {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::OpenFd,
            1 => Self::ReadBlock,
            _ => panic!("invalid op"),
        }
    }
}

impl From<MultiGetOp> for u64 {
    fn from(val: MultiGetOp) -> Self {
        val as u64
    }
}

impl Op for MultiGetOp {}

#[repr(u8)]
pub enum MultiBlobOp {
    OpenFd = 0,
    Read = 1,
}

impl From<u8> for MultiBlobOp {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::OpenFd,
            1 => Self::Read,
            _ => panic!("invalid op"),
        }
    }
}

impl From<MultiBlobOp> for u64 {
    fn from(val: MultiBlobOp) -> Self {
        val as u64
    }
}

impl Op for MultiBlobOp {}

pub enum CompletionOutput {
    MultiGetOpenFd {
        key_idx: u32,
        fd: Result<std::fs::File, std::io::Error>,
    },
    MultiGetReadBlock {
        key_idx: u32,
        read: Result<u32, std::io::Error>,
    },
    #[allow(unused)]
    MultiGetBlobOpenFd {
        key_idx: u32,
        fd: Result<std::fs::File, std::io::Error>,
    },
    #[allow(unused)]
    MultiGetBlobRead {
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

fn pack_user_data<O: Op>(domain: Domain, op: O, key_idx: u32) -> u64 {
    let domain = domain as u64;
    let op = op.into();
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

pub fn push_multi_get_open_fd(key_idx: u32, path: &PathBuf) -> Result<(), PushError> {
    IO_URING.with(|io_uring| {
        let user_data = pack_user_data(Domain::MultiGet, MultiGetOp::OpenFd, key_idx);

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

pub fn push_multi_blob_open_fd(key_idx: u32, path: &PathBuf) -> Result<(), PushError> {
    IO_URING.with(|io_uring| {
        let user_data = pack_user_data(Domain::MultiBlob, MultiBlobOp::OpenFd, key_idx);

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

pub fn push_multi_get_read_block(
    key_idx: u32,
    file: &File,
    offset: u64,
    buf: &mut [u8],
) -> Result<(), PushError> {
    IO_URING.with(|io_uring| {
        let user_data = pack_user_data(Domain::MultiGet, MultiGetOp::ReadBlock, key_idx);

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

pub fn push_multi_blob_read(
    key_idx: u32,
    file: &File,
    offset: u64,
    buf: &mut [u8],
) -> Result<(), PushError> {
    IO_URING.with(|io_uring| {
        let user_data = pack_user_data(Domain::MultiBlob, MultiBlobOp::Read, key_idx);

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

            parse_user_data(user_data, |domain, user_data| {
                let op = ((user_data >> 8) & 0xFF) as u8;
                let key_idx = (user_data >> 32) as u32;

                match domain {
                    Domain::MultiGet => {
                        let op = MultiGetOp::from(op);
                        match op {
                            MultiGetOp::OpenFd => {
                                let res = cqe.raw_result();
                                let fd = if res >= 0 {
                                    Ok(unsafe { std::fs::File::from_raw_fd(res) })
                                } else {
                                    Err(std::io::Error::from_raw_os_error(-res))
                                };
                                cb(CompletionOutput::MultiGetOpenFd { key_idx, fd })
                            }
                            MultiGetOp::ReadBlock => {
                                let res = cqe.raw_result();

                                if res >= 0 {
                                    cb(CompletionOutput::MultiGetReadBlock {
                                        key_idx,
                                        read: Ok(res as u32),
                                    })
                                } else {
                                    cb(CompletionOutput::MultiGetReadBlock {
                                        key_idx,
                                        read: Err(std::io::Error::from_raw_os_error(-res)),
                                    });
                                }
                            }
                        }
                    }
                    Domain::MultiBlob => {
                        let op = MultiBlobOp::from(op);
                        match op {
                            MultiBlobOp::OpenFd => {
                                let res = cqe.raw_result();
                                let fd = if res >= 0 {
                                    Ok(unsafe { std::fs::File::from_raw_fd(res) })
                                } else {
                                    Err(std::io::Error::from_raw_os_error(-res))
                                };
                                cb(CompletionOutput::MultiGetBlobOpenFd { key_idx, fd })
                            }
                            MultiBlobOp::Read => {
                                let res = cqe.raw_result();

                                if res >= 0 {
                                    cb(CompletionOutput::MultiGetBlobRead {
                                        key_idx,
                                        read: Ok(res as u32),
                                    })
                                } else {
                                    cb(CompletionOutput::MultiGetBlobRead {
                                        key_idx,
                                        read: Err(std::io::Error::from_raw_os_error(-res)),
                                    });
                                }
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
