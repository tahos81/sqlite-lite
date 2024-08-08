use anyhow::{bail, Result};
use page::Page;
use std::fs::File;
use std::os::unix::fs::FileExt;

mod page;

pub const DB_HEADER_SIZE: usize = 100;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let file = File::open(&args[1])?;
            let mut db_header = [0; DB_HEADER_SIZE];
            file.read_at(&mut db_header, 0)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

            println!("database page size: {}", page_size);

            let mut table_count = 0;
            let schema_page = page::parse_page(&file, 0, page_size)?;
            match schema_page {
                Page::LeafTable { cells } => {
                    for cell in cells {
                        if cell.is_table() {
                            table_count += 1;
                        }
                    }
                }
                Page::InteriorTable { .. } => {
                    unimplemented!()
                }
                _ => bail!("Invalid schema page type"),
            }
            println!("number of tables: {table_count}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
