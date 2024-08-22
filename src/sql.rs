#![allow(unused)]
use anyhow::Result;

peg::parser! {
    grammar sql_parser() for str {
        rule _() = quiet!{[' ' | '\t' | '\r' | '\n']*}

        rule identifier() -> &'input str
            = quiet!{ident:$(['a'..='z' | 'A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '*']*) { ident }}
            / expected!("identifier")

        rule table_name() -> &'input str = identifier()

        rule column_name() -> &'input str = i("count(*)") / identifier()

        rule data_type() -> &'input str
            = quiet!{
                t:$("integer" / "text" / "INT" / "TEXT" / "REAL" / "BLOB") { t }
            } / expected!("data type")

        rule value() -> &'input str
            = quiet!{val:$(['a'..='z' | 'A'..='Z' | '0'..='9' | '_']+) { val }}

        rule string_literal() -> &'input str
            = "\"" val:$((!"\"" [_])*) "\"" { val }
            / "'" val:$((!"'" [_])*) "'" { val }

        rule condition() -> Condition
        = col:column_name() _ "=" _ val:string_literal() {
                Condition::Equals {
                    column: col.to_string(),
                    value: val.to_string(),
                }
            }

        rule column_def() -> ColumnDef
            = name:(column_name() / string_literal()) _ data_type:data_type() {
                ColumnDef {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                }
            }

        rule select_statement() -> Statement
            = i("SELECT") _ cols:(column_name() ** (_ "," _)) _ i("FROM") _ table:table_name() _ cond:(i("WHERE") _ c:condition() { c })? {
                Statement::Select {
                    table: table.to_string(),
                    columns: cols.into_iter().map(|s| s.to_string()).collect(),
                    condition: cond,
                }
            }

        rule create_table_statement() -> Statement
        = i("CREATE") _ i("TABLE") _ "\""? table:table_name() "\""? _ "(" _ cols:column_def() ** ([^ ',']* "," _) _ ")" {
                Statement::CreateTable {
                    table: table.to_string(),
                    columns: cols,
                }
            }

        rule create_index_statement() -> Statement
            = i("CREATE") _ i("INDEX") _ if_not_exists:("IF NOT EXISTS" _ { true })? _ index:identifier() _ i("ON") _ table:table_name() _ "(" _ columns:(column_name() ** (_ "," _)) _ ")" {
                Statement::CreateIndex {
                    index_name: index.to_string(),
                    table: table.to_string(),
                    columns: columns.into_iter().map(|s| s.to_string()).collect(),
                    if_not_exists: if_not_exists.unwrap_or(false),
                }
            }

        rule i(expected: &'static str) -> &'static str
            = input:$(quiet!{['a'..='z' | 'A'..='Z' | '*' | '(' | ')']*}) {?
                if input.eq_ignore_ascii_case(expected) && input.len() == expected.len() {
                    Ok(expected)
                } else {
                    Err("case-insensitive match failed")
                }
            }

        pub rule sql() -> Statement
            = stmt:(select_statement() / create_table_statement() / create_index_statement()) {
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
    CreateIndex {
        index_name: String,
        table: String,
        columns: Vec<String>,
        if_not_exists: bool,
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
