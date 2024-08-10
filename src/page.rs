use crate::cell::{InteriorIndexCell, InteriorTableCell, LeafIndexCell, LeafTableCell};
use std::fmt::Display;

#[derive(Debug, Clone, Copy)]
pub enum Kind {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

pub enum Page {
    InteriorIndex {
        rmptr: u32,
        cells: Vec<InteriorIndexCell>,
    },
    InteriorTable {
        rmptr: u32,
        cells: Vec<InteriorTableCell>,
    },
    LeafIndex {
        cells: Vec<LeafIndexCell>,
    },
    LeafTable {
        cells: Vec<LeafTableCell>,
    },
}

impl Display for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Page::InteriorIndex { rmptr, cells } => {
                writeln!(f, "Interior Index Page")?;
                write!(f, "rightmost pointer: {}\n", rmptr)?;
                for cell in cells {
                    write!(f, "{}\n", cell)?;
                }
                Ok(())
            }
            Page::InteriorTable { rmptr, cells } => {
                writeln!(f, "Interior Table Page")?;
                write!(f, "rightmost pointer: {}\n", rmptr)?;
                for cell in cells {
                    write!(f, "{}\n", cell)?;
                }
                Ok(())
            }
            Page::LeafIndex { cells } => {
                writeln!(f, "Leaf Index Page")?;
                for cell in cells {
                    write!(f, "{}\n", cell)?;
                }
                Ok(())
            }
            Page::LeafTable { cells } => {
                writeln!(f, "Leaf Table Page")?;
                for cell in cells {
                    write!(f, "{}\n", cell)?;
                }
                Ok(())
            }
        }
    }
}

pub mod schema {
    #[derive(Debug, PartialEq, Eq)]
    pub enum Kind {
        Table,
        Index,
        View,
        Trigger,
    }

    #[allow(dead_code)]
    pub struct Schema {
        pub kind: Kind,
        pub name: String,
        pub tbl_name: String,
        pub rootpage: usize,
        pub sql: String,
    }
}
