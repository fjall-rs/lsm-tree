// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TableType {
    Block,
}

impl From<TableType> for u8 {
    fn from(val: TableType) -> Self {
        match val {
            TableType::Block => 0,
        }
    }
}

impl TryFrom<u8> for TableType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Block),
            _ => Err(()),
        }
    }
}
