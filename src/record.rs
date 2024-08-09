use std::fmt::Display;

#[derive(Debug)]
pub enum ColumnType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float,
    Zero,
    One,
    Reserved1,
    Reserved2,
    Blob(usize),
    Text(usize),
}

#[derive(Debug)]
pub enum Record {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float(f64),
    Zero,
    One,
    Reserved1,
    Reserved2,
    Blob(Vec<u8>),
    Text(String),
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Record::Null => write!(f, "NULL"),
            Record::Int8(v) => write!(f, "{}", v),
            Record::Int16(v) => write!(f, "{}", v),
            Record::Int24(v) => write!(f, "{}", v),
            Record::Int32(v) => write!(f, "{}", v),
            Record::Int48(v) => write!(f, "{}", v),
            Record::Int64(v) => write!(f, "{}", v),
            Record::Float(v) => write!(f, "{}", v),
            Record::Zero => write!(f, "0"),
            Record::One => write!(f, "1"),
            Record::Reserved1 => write!(f, "Reserved1"),
            Record::Reserved2 => write!(f, "Reserved2"),
            Record::Blob(v) => write!(f, "{:?}", v),
            Record::Text(v) => write!(f, "{}", v),
        }
    }
}
