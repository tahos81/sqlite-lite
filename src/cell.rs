use super::record::Record;
use std::fmt::Display;

#[derive(Debug)]
pub struct InteriorIndexCell {
    pub left_child: u32,
    pub keys: Vec<Record>,
}

#[derive(Debug)]
pub struct InteriorTableCell {
    pub left_child: u32,
    pub row_id: u64,
}

#[derive(Debug)]
pub struct LeafIndexCell {
    pub keys: Vec<Record>,
}

#[derive(Debug)]
pub struct LeafTableCell {
    pub row_id: u64,
    pub values: Vec<Record>,
}

impl Display for InteriorIndexCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "left_child: {}, keys: [", self.left_child)?;
        for (i, key) in self.keys.iter().enumerate() {
            if i == self.keys.len() - 1 {
                write!(f, "{}", key)?;
            } else {
                write!(f, "{}, ", key)?;
            }
        }
        write!(f, "]")
    }
}

impl Display for InteriorTableCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "left_child: {}, row_id: {}",
            self.left_child, self.row_id
        )
    }
}

impl Display for LeafIndexCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "keys: [")?;
        for (i, key) in self.keys.iter().enumerate() {
            if i == self.keys.len() - 1 {
                write!(f, "{}", key)?;
            } else {
                write!(f, "{}, ", key)?;
            }
        }
        write!(f, "]")
    }
}

impl Display for LeafTableCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row_id: {}, values: [", self.row_id)?;
        for (i, value) in self.values.iter().enumerate() {
            if i == self.values.len() - 1 {
                write!(f, "{}", value)?;
            } else {
                write!(f, "{}, ", value)?;
            }
        }
        write!(f, "]")
    }
}
