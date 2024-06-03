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
