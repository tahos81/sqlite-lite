use anyhow::{anyhow, Result};
use db::Database;
use page::Page;
use sql::parse_select;

mod cell;
mod db;
mod page;
mod record;
mod sql;

pub const DB_HEADER_SIZE: usize = 100;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let db_path = args
        .next()
        .ok_or(anyhow!("Missing <database path> and <command>"))?;
    let command = args.next().ok_or(anyhow!("Missing <command>"))?;

    let db = Database::load_db(db_path)?;

    match command.as_str() {
        ".dbinfo" => {
            db.info()?;
        }
        ".tables" => {
            db.tables()?;
        }
        _ => {
            let statement = parse_select(command.as_str())?;
            db.execute_statement(statement)?;
        }
    }

    Ok(())
}
