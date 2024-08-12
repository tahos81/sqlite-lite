#![allow(unused)]
use anyhow::Result;

peg::parser! {
    grammar sql_parser() for str {
        rule _() = quiet!{[' ' | '\t' | '\r' | '\n']*}

        rule identifier() -> &'input str
            = quiet!{ident:$(['a'..='z' | 'A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { ident }}
            / expected!("identifier")

        rule table_name() -> &'input str = identifier()

        rule column_name() -> &'input str = identifier()

        rule data_type() -> &'input str
            = quiet!{
                t:$("integer" / "text" / "INT" / "TEXT" / "REAL" / "BLOB") { t }
            } / expected!("data type")

        rule value() -> &'input str
            = quiet!{val:$(['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+) { val }}

        rule condition() -> Condition
        = col:column_name() _ "=" _ "'"? val:value() "'"? {
                Condition::Equals {
                    column: col.to_string(),
                    value: val.to_string(),
                }
            }

        rule select_statement() -> Statement
            = i("SELECT") _ cols:(("COUNT(*)" / "count(*)") {vec!["COUNT(*)"]}  / column_name() ** (_ "," _)) _ i("FROM") _ table:table_name() _ cond:(i("WHERE") _ c:condition() { c })? {
                Statement::Select {
                    table: table.to_string(),
                    columns: cols.into_iter().map(|s| s.to_string()).collect(),
                    condition: cond,
                }
            }

        rule column_def() -> ColumnDef
            = name:column_name() _ data_type:data_type() {
                ColumnDef {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                }
            }

        rule create_statement() -> Statement
            = i("CREATE") _ i("TABLE") _ table:table_name() _ "(" _ cols:column_def() ** ([^ ',']* "," _) _ ")" {
                Statement::CreateTable {
                    table: table.to_string(),
                    columns: cols,
                }
            }

        rule i(expected: &'static str) -> &'static str
            = input:$(quiet!{['a'..='z' | 'A'..='Z']*}) {?
                if input.eq_ignore_ascii_case(expected) && input.len() == expected.len() {
                    Ok(expected)
                } else {
                    Err("case-insensitive match failed")
                }
            }

        pub rule sql() -> Statement
            = stmt:(select_statement() / create_statement()) {
                stmt
            }
    }
}

#[derive(Debug)]
pub enum Statement {
    Select {
        table: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnDef>,
    },
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    data_type: String,
}

#[derive(Debug)]
pub enum Condition {
    Equals { column: String, value: String },
}

pub fn parse_sql(input: &str) -> Result<Statement> {
    sql_parser::sql(input).map_err(|e| anyhow::anyhow!("{}", e))
}
