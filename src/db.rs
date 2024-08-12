use crate::{
    cell::{InteriorIndexCell, InteriorTableCell, LeafIndexCell, LeafTableCell},
    page::{
        schema::{self, Schema},
        Kind,
    },
    record::{ColumnType, Record},
    sql::{parse_sql, Condition, Statement},
    Page, DB_HEADER_SIZE,
};
use anyhow::{anyhow, Result};
use nom::number::complete::{be_f64, be_i16, be_i24, be_i32, be_i64, be_i8, be_u32};
use std::{fs::File, os::unix::fs::FileExt};

pub struct Database {
    db: File,
    page_size: usize,
    schema: Vec<Schema>,
}

impl Database {
    pub fn load_db(path: String) -> Result<Database> {
        let file = File::open(&path)?;

        let mut db_header = [0; DB_HEADER_SIZE];
        file.read_at(&mut db_header, 0)?;
        let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

        let loader = DbLoader::new(file, page_size);
        let schema = loader.read_schema()?;

        Ok(Database {
            db: loader.db,
            page_size: loader.page_size,
            schema,
        })
    }

    pub fn info(&self) -> Result<()> {
        println!("database page size: {}", self.page_size);
        let table_count = self.table_count()?;
        println!("number of tables: {}", table_count);
        Ok(())
    }

    pub fn tables(&self) -> Result<()> {
        for schema in &self.schema {
            if schema.kind == schema::Kind::Table {
                println!("{}", schema.name);
            }
        }
        Ok(())
    }

    pub fn execute_statement(&self, statement: Statement) -> Result<()> {
        match statement {
            Statement::Select {
                table,
                columns: selected_cols,
                condition,
            } => {
                if selected_cols.len() == 1 && selected_cols[0] == String::from("COUNT(*)") {
                    let count = self.table_row_count(&table, condition)?;
                    println!("{}", count);
                } else {
                    let schema = self.get_schema(table.as_str())?;
                    let statement = parse_sql(schema.sql.as_str())?;
                    if let Statement::CreateTable { table, columns } = statement {
                        let col_idxs: Vec<_> = selected_cols
                            .iter()
                            .filter_map(|c| columns.iter().position(|col| col.name == *c))
                            .collect();

                        let rootpage = self.get_rootpage(table.as_str())?;
                        let page = self.read_page(rootpage - 1)?;
                        match page {
                            Page::LeafTable { cells } => {
                                cells
                                    .iter()
                                    .filter(|cell| {
                                        if let Some(condition) = &condition {
                                            match condition {
                                                Condition::Equals { column, value } => {
                                                    let statement =
                                                        parse_sql(schema.sql.as_str()).unwrap();
                                                    let columns = match statement {
                                                        Statement::CreateTable {
                                                            columns, ..
                                                        } => columns,
                                                        _ => unreachable!(),
                                                    };
                                                    let col_idx = columns
                                                        .iter()
                                                        .position(|c| c.name == *column)
                                                        .unwrap();
                                                    match &cell.values[col_idx] {
                                                        Record::Text(s) => *s == *value,
                                                        _ => unimplemented!(),
                                                    }
                                                }
                                            }
                                        } else {
                                            true
                                        }
                                    })
                                    .for_each(|cell| {
                                        let values = &cell.values;
                                        let last_col = col_idxs.len() - 1;
                                        for (idx, col_idx) in col_idxs.iter().enumerate() {
                                            if idx == last_col {
                                                println!("{}", values[*col_idx]);
                                            } else {
                                                print!("{}|", values[*col_idx]);
                                            }
                                        }
                                    });
                                // for cell in cells {
                                //     let values = cell.values;
                                //     let last_col = col_idxs.len() - 1;
                                //     for (idx, col_idx) in col_idxs.iter().enumerate() {
                                //         if idx == last_col {
                                //             println!("{}", values[*col_idx]);
                                //         } else {
                                //             print!("{}|", values[*col_idx]);
                                //         }
                                //     }
                                // }
                            }
                            Page::InteriorTable { .. } => unimplemented!(),
                            _ => Err(anyhow!("Invalid page type"))?,
                        }
                    }
                }
            }
            _ => unimplemented!(),
        }

        Ok(())
    }

    fn table_row_count(&self, table_name: &str, condition: Option<Condition>) -> Result<usize> {
        let rootpage = self.get_rootpage(table_name)?;
        let page = self.read_page(rootpage - 1)?;
        let schema = self.get_schema(table_name)?;

        match page {
            Page::LeafTable { cells } => {
                let count = cells
                    .iter()
                    .filter(|cell| {
                        if let Some(condition) = &condition {
                            match condition {
                                Condition::Equals { column, value } => {
                                    let statement = parse_sql(schema.sql.as_str()).unwrap();
                                    let columns = match statement {
                                        Statement::CreateTable { columns, .. } => columns,
                                        _ => unreachable!(),
                                    };
                                    let col_idx =
                                        columns.iter().position(|c| c.name == *column).unwrap();
                                    match &cell.values[col_idx] {
                                        Record::Text(s) => *s == *value,
                                        _ => unimplemented!(),
                                    }
                                }
                            }
                        } else {
                            true
                        }
                    })
                    .count();
                Ok(count)
            }
            Page::InteriorTable { .. } => unimplemented!(),
            _ => Err(anyhow!("Invalid page type"))?,
        }
    }

    fn get_schema(&self, table_name: &str) -> Result<&Schema> {
        self.schema
            .iter()
            .find(|s| s.name == table_name && s.kind == schema::Kind::Table)
            .ok_or(anyhow!("Table not found"))
    }

    fn get_rootpage(&self, table_name: &str) -> Result<usize> {
        let schema = self
            .schema
            .iter()
            .find(|s| s.name == table_name && s.kind == schema::Kind::Table)
            .ok_or(anyhow!("Table not found"))?;
        Ok(schema.rootpage)
    }

    fn read_page(&self, page_num: usize) -> Result<Page> {
        let mut page = vec![0; self.page_size];
        self.db
            .read_exact_at(&mut page, (page_num * self.page_size) as u64)?;
        let offset = match page_num {
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

    fn table_count(&self) -> Result<usize> {
        let mut count = 0;
        for schema in &self.schema {
            if schema.kind == schema::Kind::Table {
                count += 1;
            }
        }
        Ok(count)
    }
}

struct DbLoader {
    db: File,
    page_size: usize,
}

impl DbLoader {
    fn new(db: File, page_size: u16) -> Self {
        Self {
            db,
            page_size: page_size as usize,
        }
    }

    fn read_schema(&self) -> Result<Vec<Schema>> {
        let mut page = vec![0; self.page_size];
        self.db.read_exact_at(&mut page, 0)?;
        let kind = match page[0 + DB_HEADER_SIZE] {
            5 => unimplemented!(),
            13 => Kind::LeafTable,
            _ => Err(anyhow!("Invalid schema page kind"))?,
        };

        let num_of_cells = u16::from_be_bytes([page[3 + DB_HEADER_SIZE], page[4 + DB_HEADER_SIZE]]);
        let _start_idx = u16::from_be_bytes([page[5 + DB_HEADER_SIZE], page[6 + DB_HEADER_SIZE]]);
        let mut _right_most = 0;
        if let Kind::InteriorTable = kind {
            _right_most = u32::from_be_bytes([
                page[8 + DB_HEADER_SIZE],
                page[9 + DB_HEADER_SIZE],
                page[10 + DB_HEADER_SIZE],
                page[11 + DB_HEADER_SIZE],
            ]);
        }

        let mut cell_pointers = Vec::with_capacity(num_of_cells as usize);
        let header_end = match kind {
            Kind::InteriorTable => 12 + DB_HEADER_SIZE as u16,
            Kind::LeafTable => 8 + DB_HEADER_SIZE as u16,
            _ => unreachable!(),
        };

        cell_pointers.extend((0..num_of_cells).map(|i| {
            let offset = (header_end + i * 2) as usize;
            u16::from_be_bytes([page[offset], page[offset + 1]])
        }));

        match kind {
            Kind::LeafTable => {
                let mut schema = Vec::new();
                for ptr in cell_pointers {
                    let cell = &page[ptr as usize..];
                    let (_length, cell, _) = parse_varint(cell)?;
                    let (_id, cell, _) = parse_varint(cell)?;
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

                    match col_types[..] {
                        [ColumnType::Text(type_len), ColumnType::Text(name_len), ColumnType::Text(tbl_name_len), ColumnType::Int8, ColumnType::Text(sql_len)] =>
                        {
                            let (text, cell) = cell.split_at(type_len);
                            let kind = std::str::from_utf8(text)?;

                            let kind = match kind {
                                "table" => schema::Kind::Table,
                                "index" => schema::Kind::Index,
                                "view" => schema::Kind::View,
                                "trigger" => schema::Kind::Trigger,
                                _ => Err(anyhow!("Invalid kind"))?,
                            };

                            let (text, cell) = cell.split_at(name_len);
                            let name = std::str::from_utf8(text)?;

                            let (text, cell) = cell.split_at(tbl_name_len);
                            let tbl_name = std::str::from_utf8(text)?;

                            let (cell, rootpage) = be_i8::<_, ()>(cell)?;

                            let (text, _) = cell.split_at(sql_len);
                            let sql = std::str::from_utf8(text)?;

                            schema.push(Schema {
                                kind,
                                name: name.to_owned(),
                                tbl_name: tbl_name.to_owned(),
                                rootpage: rootpage as usize,
                                sql: sql.to_owned(),
                            });
                        }
                        _ => Err(anyhow!("Invalid schema"))?,
                    }
                }

                Ok(schema)
            }
            Kind::InteriorTable => unimplemented!(),
            _ => unreachable!(),
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
