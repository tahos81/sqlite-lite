use peg::parser;

parser! {
    grammar sql_parser() for str {
        pub rule select() -> Statement
        = ("SELECT" / "select") whitespace() columns:column_list()  whitespace()
              "FROM" whitespace() table:identifier()
              //where:(whitespace() "WHERE" whitespace() e:expression() { e })?
              { Statement::Select { columns, table } }

        rule column_list() -> Vec<String>
              = "COUNT(*)" {["COUNT(*)".to_string()].to_vec()} / c:identifier() ** "," { c }

        rule identifier() -> String
            = quiet!{n:$(['a'..='z' | 'A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { n.to_string() }}

        rule whitespace()
            = quiet!{[' ' | '\t' | '\n' | '\r']+}
    }
}

#[derive(Debug)]
pub enum Statement {
    Select {
        columns: Vec<String>,
        table: String,
        //where_clause: Option<Expression>,
    },
}

// #[derive(Debug)]
// pub enum Value {
//     String(String),
//     Number(f64),
//     Null,
// }

// #[derive(Debug)]
// pub enum Expression {
//     Comparison(String, Operator, Value),
// }

// #[derive(Debug)]
// pub enum Operator {
//     Eq,
// }

pub fn parse_select(sql: &str) -> Result<Statement, peg::error::ParseError<peg::str::LineCol>> {
    sql_parser::select(sql)
}
