use anyhow::{anyhow, bail, Result};
use db::Database;
use page::Page;

mod cell;
mod db;
mod page;
mod record;

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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
