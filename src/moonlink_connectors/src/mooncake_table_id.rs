use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MooncakeTableId {
    pub database_id: u32,
    pub table_id: u32,
}

impl Display for MooncakeTableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}", self.database_id, self.table_id)
    }
}
