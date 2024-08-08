use std::{fmt::Display, fs::File, os::unix::fs::FileExt};

use anyhow::{anyhow, Result};
use nom::number::complete::{be_f64, be_i16, be_i24, be_i32, be_i64, be_i8, be_u32};

use crate::DB_HEADER_SIZE;

#[derive(Debug, Clone, Copy)]
enum Kind {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

#[derive(Debug)]
enum Record {
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

#[derive(Debug)]
pub struct InteriorIndexCell {
    left_child: u32,
    keys: Vec<Record>,
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

#[derive(Debug)]
pub struct InteriorTableCell {
    left_child: u32,
    row_id: u64,
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

#[derive(Debug)]
pub struct LeafIndexCell {
    keys: Vec<Record>,
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

#[derive(Debug)]
pub struct LeafTableCell {
    row_id: u64,
    values: Vec<Record>,
}

impl LeafTableCell {
    pub fn is_table(&self) -> bool {
        match &self.values[0] {
            Record::Text(text) => {
                if text == "table" {
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
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

#[derive(Debug)]
enum ColumnType {
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

pub fn parse_page(db: &File, page_idx: usize, page_size: u16) -> Result<Page> {
    let mut page = vec![0; page_size as usize];
    db.read_exact_at(&mut page, (page_idx * page_size as usize) as u64)?;
    let offset = match page_idx {
        0 => DB_HEADER_SIZE,
        _ => 0,
    };
    let kind = match page[0 + offset] {
        2 => Kind::InteriorIndex,
        5 => Kind::InteriorTable,
        10 => Kind::LeafIndex,
        13 => Kind::LeafTable,
        _ => Err(anyhow!("Invalid page kind"))?,
    };

    let num_of_cells = u16::from_be_bytes([page[3 + offset], page[4 + offset]]);
    let _start_idx = u16::from_be_bytes([page[5 + offset], page[6 + offset]]);
    let mut right_most = 0;
    if let Kind::InteriorTable | Kind::InteriorIndex = kind {
        right_most = u32::from_be_bytes([
            page[8 + offset],
            page[9 + offset],
            page[10 + offset],
            page[11 + offset],
        ]);
    }

    let mut cell_pointers = Vec::with_capacity(num_of_cells as usize);
    let header_end = match kind {
        Kind::InteriorTable | Kind::InteriorIndex => 12 + offset as u16,
        _ => 8 + offset as u16,
    };

    cell_pointers.extend((0..num_of_cells).map(|i| {
        let offset = (header_end + i * 2) as usize;
        u16::from_be_bytes([page[offset], page[offset + 1]])
    }));

    match kind {
        Kind::LeafTable => {
            let mut cells = Vec::new();
            for ptr in cell_pointers {
                let mut values = Vec::new();
                let cell = &page[ptr as usize..];
                let (_length, cell, _) = parse_varint(cell)?;
                let (id, cell, _) = parse_varint(cell)?;
                let (rec_header_size, mut cell, varint_size) = parse_varint(cell)?;
                let mut col_types = Vec::new();
                let mut cur_header_size = varint_size;
                while cur_header_size < rec_header_size as usize {
                    let (column_type, remaining_cell, varint_size) = parse_varint(cell)?;
                    let col_type = match column_type {
                        0 => ColumnType::Null,
                        1 => ColumnType::Int8,
                        2 => ColumnType::Int16,
                        3 => ColumnType::Int24,
                        4 => ColumnType::Int32,
                        5 => ColumnType::Int48,
                        6 => ColumnType::Int64,
                        7 => ColumnType::Float,
                        8 => ColumnType::Zero,
                        9 => ColumnType::One,
                        10 => ColumnType::Reserved1,
                        11 => ColumnType::Reserved2,
                        n if n % 2 == 0 => ColumnType::Blob((n - 12) as usize / 2),
                        n => ColumnType::Text((n - 13) as usize / 2),
                    };
                    col_types.push(col_type);
                    cur_header_size += varint_size;
                    cell = remaining_cell;
                }

                for col in col_types {
                    match col {
                        ColumnType::Null => {
                            values.push(Record::Null);
                        }
                        ColumnType::Int8 => {
                            let (rem, value) = be_i8::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Int8(value));
                        }
                        ColumnType::Int16 => {
                            let (rem, value) = be_i16::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Int16(value));
                        }
                        ColumnType::Int24 => {
                            let (rem, value) = be_i24::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Int24(value));
                        }
                        ColumnType::Int32 => {
                            let (rem, value) = be_i32::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Int32(value));
                        }
                        ColumnType::Int48 => {
                            let value = i64::from_be_bytes([
                                0, 0, cell[0], cell[1], cell[2], cell[3], cell[4], cell[5],
                            ]);
                            cell = &cell[6..];
                            values.push(Record::Int48(value));
                        }
                        ColumnType::Int64 => {
                            let (rem, value) = be_i64::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Int64(value));
                        }
                        ColumnType::Float => {
                            let (rem, value) = be_f64::<_, ()>(cell)?;
                            cell = rem;
                            values.push(Record::Float(value));
                        }
                        ColumnType::Zero => {
                            values.push(Record::Zero);
                        }
                        ColumnType::One => {
                            values.push(Record::One);
                        }
                        ColumnType::Reserved1 => values.push(Record::Reserved1),
                        ColumnType::Reserved2 => values.push(Record::Reserved2),
                        ColumnType::Blob(len) => {
                            let (blob, remaining) = cell.split_at(len);
                            cell = remaining;
                            values.push(Record::Blob(blob.to_vec()));
                        }
                        ColumnType::Text(len) => {
                            let (text, remaining) = cell.split_at(len);
                            let text = std::str::from_utf8(text)?;
                            cell = remaining;
                            values.push(Record::Text(text.to_string()));
                        }
                    }
                }
                cells.push(LeafTableCell { row_id: id, values });
            }

            Ok(Page::LeafTable { cells })
        }
        Kind::InteriorTable => {
            let mut cells = Vec::new();
            for ptr in cell_pointers {
                let cell = &page[ptr as usize..];
                let (cell, left_child_pointer) = be_u32::<_, ()>(cell)?;
                let (id, _, _) = parse_varint(cell)?;
                cells.push(InteriorTableCell {
                    left_child: left_child_pointer,
                    row_id: id,
                });
            }

            Ok(Page::InteriorTable {
                rmptr: right_most,
                cells,
            })
        }
        Kind::LeafIndex => {
            let mut cells = Vec::new();
            for ptr in cell_pointers {
                let mut keys = Vec::new();
                let cell = &page[ptr as usize..];
                let (_len, cell, _) = parse_varint(cell)?;
                let (rec_header_size, mut cell, varint_size) = parse_varint(cell)?;
                let mut col_types = Vec::new();
                let mut cur_header_size = varint_size;
                while cur_header_size < rec_header_size as usize {
                    let (column_type, remaining_cell, varint_size) = parse_varint(cell)?;
                    let col_type = match column_type {
                        0 => ColumnType::Null,
                        1 => ColumnType::Int8,
                        2 => ColumnType::Int16,
                        3 => ColumnType::Int24,
                        4 => ColumnType::Int32,
                        5 => ColumnType::Int48,
                        6 => ColumnType::Int64,
                        7 => ColumnType::Float,
                        8 => ColumnType::Zero,
                        9 => ColumnType::One,
                        10 => ColumnType::Reserved1,
                        11 => ColumnType::Reserved2,
                        n if n % 2 == 0 => ColumnType::Blob((n - 12) as usize / 2),
                        n => ColumnType::Text((n - 13) as usize / 2),
                    };
                    col_types.push(col_type);
                    cur_header_size += varint_size;
                    cell = remaining_cell;
                }

                for col in col_types {
                    match col {
                        ColumnType::Null => {
                            keys.push(Record::Null);
                        }
                        ColumnType::Int8 => {
                            let (rem, value) = be_i8::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int8(value));
                        }
                        ColumnType::Int16 => {
                            let (rem, value) = be_i16::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int16(value));
                        }
                        ColumnType::Int24 => {
                            let (rem, value) = be_i24::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int24(value));
                        }
                        ColumnType::Int32 => {
                            let (rem, value) = be_i32::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int32(value));
                        }
                        ColumnType::Int48 => {
                            let value = i64::from_be_bytes([
                                0, 0, cell[0], cell[1], cell[2], cell[3], cell[4], cell[5],
                            ]);
                            cell = &cell[6..];
                            keys.push(Record::Int48(value));
                        }
                        ColumnType::Int64 => {
                            let (rem, value) = be_i64::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int64(value));
                        }
                        ColumnType::Float => {
                            let (rem, value) = be_f64::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Float(value));
                        }
                        ColumnType::Zero => {
                            keys.push(Record::Zero);
                        }
                        ColumnType::One => {
                            keys.push(Record::One);
                        }
                        ColumnType::Reserved1 => keys.push(Record::Reserved1),
                        ColumnType::Reserved2 => keys.push(Record::Reserved2),
                        ColumnType::Blob(len) => {
                            let (blob, remaining) = cell.split_at(len);
                            cell = remaining;
                            keys.push(Record::Blob(blob.to_vec()));
                        }
                        ColumnType::Text(len) => {
                            let (text, remaining) = cell.split_at(len);
                            let text = std::str::from_utf8(text)?;
                            cell = remaining;
                            keys.push(Record::Text(text.to_string()));
                        }
                    }
                }

                cells.push(LeafIndexCell { keys });
            }

            Ok(Page::LeafIndex { cells })
        }
        Kind::InteriorIndex => {
            let mut cells = Vec::new();
            for ptr in cell_pointers {
                let mut keys = Vec::new();
                let cell = &page[ptr as usize..];
                let (cell, left_child_pointer) = be_u32::<_, ()>(cell)?;
                let (_len, cell, _) = parse_varint(cell)?;
                let (rec_header_size, mut cell, varint_size) = parse_varint(cell)?;
                let mut col_types = Vec::new();
                let mut cur_header_size = varint_size;
                while cur_header_size < rec_header_size as usize {
                    let (column_type, remaining_cell, varint_size) = parse_varint(cell)?;
                    let col_type = match column_type {
                        0 => ColumnType::Null,
                        1 => ColumnType::Int8,
                        2 => ColumnType::Int16,
                        3 => ColumnType::Int24,
                        4 => ColumnType::Int32,
                        5 => ColumnType::Int48,
                        6 => ColumnType::Int64,
                        7 => ColumnType::Float,
                        8 => ColumnType::Zero,
                        9 => ColumnType::One,
                        10 => ColumnType::Reserved1,
                        11 => ColumnType::Reserved2,
                        n if n % 2 == 0 => ColumnType::Blob((n - 12) as usize / 2),
                        n => ColumnType::Text((n - 13) as usize / 2),
                    };
                    col_types.push(col_type);
                    cur_header_size += varint_size;
                    cell = remaining_cell;
                }

                for col in col_types {
                    match col {
                        ColumnType::Null => {
                            keys.push(Record::Null);
                        }
                        ColumnType::Int8 => {
                            let (rem, value) = be_i8::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int8(value));
                        }
                        ColumnType::Int16 => {
                            let (rem, value) = be_i16::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int16(value));
                        }
                        ColumnType::Int24 => {
                            let (rem, value) = be_i24::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int24(value));
                        }
                        ColumnType::Int32 => {
                            let (rem, value) = be_i32::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int32(value));
                        }
                        ColumnType::Int48 => {
                            let value = i64::from_be_bytes([
                                0, 0, cell[0], cell[1], cell[2], cell[3], cell[4], cell[5],
                            ]);
                            cell = &cell[6..];
                            keys.push(Record::Int48(value));
                        }
                        ColumnType::Int64 => {
                            let (rem, value) = be_i64::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Int64(value));
                        }
                        ColumnType::Float => {
                            let (rem, value) = be_f64::<_, ()>(cell)?;
                            cell = rem;
                            keys.push(Record::Float(value));
                        }
                        ColumnType::Zero => {
                            keys.push(Record::Zero);
                        }
                        ColumnType::One => {
                            keys.push(Record::One);
                        }
                        ColumnType::Reserved1 => keys.push(Record::Reserved1),
                        ColumnType::Reserved2 => keys.push(Record::Reserved2),
                        ColumnType::Blob(len) => {
                            let (blob, remaining) = cell.split_at(len);
                            cell = remaining;
                            keys.push(Record::Blob(blob.to_vec()));
                        }
                        ColumnType::Text(len) => {
                            let (text, remaining) = cell.split_at(len);
                            let text = std::str::from_utf8(text)?;
                            cell = remaining;
                            keys.push(Record::Text(text.to_string()));
                        }
                    }
                }

                cells.push(InteriorIndexCell {
                    left_child: left_child_pointer,
                    keys,
                });
            }

            Ok(Page::InteriorIndex {
                rmptr: right_most,
                cells,
            })
        }
    }
}

fn parse_varint(data: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result: u64 = 0;

    for (idx, &byte) in data.iter().enumerate() {
        if idx >= 10 {
            return Err(anyhow!("Varint is too long"));
        }

        result = (result << 7) | (byte & 0x7F) as u64;

        if byte & 0x80 == 0 {
            return Ok((result, &data[idx + 1..], idx + 1));
        }
    }

    Err(anyhow!("Varint is incomplete"))
}
