use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnstoreTableId<D, T> {
    pub database_id: D,
    pub table_id: T,
}

impl<D, T> Display for ColumnstoreTableId<D, T>
where
    D: Display,
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}", self.database_id, self.table_id)
    }
}
