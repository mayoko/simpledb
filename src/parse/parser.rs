use crate::query::{
    constant::Constant,
    expression::Expression,
    predicate::ProductPredicate,
    term::{EqualTerm, Term},
};

use super::{
    constant::KEYWORDS,
    content::{delete_data::DeleteData, insert_data::InsertData, query_data::QueryData},
    lexer::{Lexer, Token},
};
use anyhow::{anyhow, Result as AnyhowResult};

use thiserror::Error;

/**
 * SQL 文を受け取って、それを解析するためのトレイト
 * 内部的に cursor を持ち、このクラスのユーザーは次にどのような syntactic category が来るかをメソッドで指定する。
 * 現在 cursor が指している箇所がその category と一致していれば、その content を返しつつ、cursor を進める。
 * 一致していなければエラーを返す。
 */
pub trait Parser {
    /// constant value の取得
    fn parse_constant(&mut self) -> AnyhowResult<Constant>;
    /// expression の取得
    fn parse_expression(&mut self) -> AnyhowResult<Expression>;
    /// = で結ばれた term の取得
    fn parse_equal_term(&mut self) -> AnyhowResult<EqualTerm>;
    /// and で結ばれた predicate の取得
    fn parse_predicate(&mut self) -> AnyhowResult<ProductPredicate>;
    /// select 文の取得
    fn parse_query(&mut self) -> AnyhowResult<QueryData>;
    /// insert 文の取得
    fn parse_insert(&mut self) -> AnyhowResult<InsertData>;
    /// delete 文の取得
    fn parse_delete(&mut self) -> AnyhowResult<DeleteData>;
}

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token")]
    UnexpectedToken(String),
    #[error("internal error")]
    Internal(String),
}

pub struct ParserImpl {
    lexer: Lexer,
}

impl Parser for ParserImpl {
    fn parse_constant(&mut self) -> AnyhowResult<Constant> {
        match &self.lexer.get_token() {
            Token::IntConstant(_) => {
                let value = self.lexer.eat_int_constant()?;
                Ok(Constant::Int(value))
            }
            Token::StringConstant(_) => {
                let value = self.lexer.eat_string_constant()?;
                Ok(Constant::String(value))
            }
            _ => Err(anyhow!(ParserError::UnexpectedToken(
                "expected constant".to_string()
            ))),
        }
    }
    fn parse_expression(&mut self) -> AnyhowResult<Expression> {
        match &self.lexer.get_token() {
            Token::IntConstant(_) | Token::StringConstant(_) => {
                let constant = self.parse_constant()?;
                Ok(Expression::Constant(constant))
            }
            Token::Id(_) => {
                let field_name = self.lexer.eat_id()?;
                Ok(Expression::Field(field_name))
            }
            _ => Err(anyhow!(ParserError::UnexpectedToken(
                "expected expression".to_string()
            ))),
        }
    }
    fn parse_equal_term(&mut self) -> AnyhowResult<EqualTerm> {
        let lhs = self.parse_expression()?;
        self.lexer.eat_exact(Token::Delimiter('='))?;
        let rhs = self.parse_expression()?;
        Ok(EqualTerm::new(lhs, rhs))
    }
    fn parse_predicate(&mut self) -> AnyhowResult<ProductPredicate> {
        let mut terms: Vec<Box<dyn Term>> = vec![Box::new(self.parse_equal_term()?)];
        while self.lexer.is_matched(Token::Keyword("and".to_string())) {
            self.lexer.eat_exact(Token::Keyword("and".to_string()))?;
            terms.push(Box::new(self.parse_equal_term()?));
        }
        Ok(ProductPredicate::new(terms))
    }
    fn parse_query(&mut self) -> AnyhowResult<QueryData> {
        self.lexer.eat_exact(Token::Keyword("select".to_string()))?;
        let fields = self.parse_id_list()?;
        self.lexer.eat_exact(Token::Keyword("from".to_string()))?;
        let tables = self.parse_id_list()?;
        if self.lexer.is_matched(Token::Keyword("where".to_string())) {
            self.lexer.eat_exact(Token::Keyword("where".to_string()))?;
            let predicate = self.parse_predicate()?;
            Ok(QueryData::new(fields, tables, predicate))
        } else {
            Ok(QueryData::new(
                fields,
                tables,
                ProductPredicate::new(vec![]),
            ))
        }
    }
    fn parse_insert(&mut self) -> AnyhowResult<InsertData> {
        self.lexer.eat_exact(Token::Keyword("insert".to_string()))?;
        self.lexer.eat_exact(Token::Keyword("into".to_string()))?;
        let table_name = self.lexer.eat_id()?;
        self.lexer.eat_exact(Token::Delimiter('('))?;
        let fields = self.parse_id_list()?;
        self.lexer.eat_exact(Token::Delimiter(')'))?;
        self.lexer.eat_exact(Token::Keyword("values".to_string()))?;
        self.lexer.eat_exact(Token::Delimiter('('))?;
        let values = self.parse_constant_list()?;
        self.lexer.eat_exact(Token::Delimiter(')'))?;
        Ok(InsertData::new(table_name, fields, values))
    }
    fn parse_delete(&mut self) -> AnyhowResult<DeleteData> {
        self.lexer.eat_exact(Token::Keyword("delete".to_string()))?;
        self.lexer.eat_exact(Token::Keyword("from".to_string()))?;
        let table_name = self.lexer.eat_id()?;
        if self.lexer.is_matched(Token::Keyword("where".to_string())) {
            self.lexer.eat_exact(Token::Keyword("where".to_string()))?;
            let predicate = self.parse_predicate()?;
            Ok(DeleteData::new(table_name, predicate))
        } else {
            Ok(DeleteData::new(table_name, ProductPredicate::new(vec![])))
        }
    }
}

impl ParserImpl {
    pub fn new(input: String) -> AnyhowResult<ParserImpl> {
        let lexer = Lexer::new(input, KEYWORDS.iter().map(|s| s.to_string()).collect())?;
        Ok(ParserImpl { lexer })
    }
    fn parse_id_list(&mut self) -> AnyhowResult<Vec<String>> {
        let mut fields = vec![self.lexer.eat_id()?];
        while self.lexer.is_matched(Token::Delimiter(',')) {
            self.lexer.eat_exact(Token::Delimiter(','))?;
            fields.push(self.lexer.eat_id()?);
        }
        Ok(fields)
    }
    fn parse_constant_list(&mut self) -> AnyhowResult<Vec<Constant>> {
        let mut values = vec![self.parse_constant()?];
        while self.lexer.is_matched(Token::Delimiter(',')) {
            self.lexer.eat_exact(Token::Delimiter(','))?;
            values.push(self.parse_constant()?);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod parser_test {
    use super::*;
    #[test]
    fn test_select_sentence() {
        let query = "select a from x, z where b = 3 and c = 'string'";
        let mut parser = ParserImpl::new(query.to_string()).unwrap();
        let query_data = parser.parse_query().unwrap();
        assert_eq!(query_data.get_fields(), &vec!["a".to_string()]);
        assert_eq!(
            query_data.get_tables(),
            &vec!["x".to_string(), "z".to_string()]
        );
        let predicate = query_data.get_predicate();
        assert_eq!(predicate.to_string(), "b = 3 and c = 'string'");
    }
    #[test]
    fn test_insert_sentence() {
        let query = "insert into x (a, b) values (3, 'string')";
        let mut parser = ParserImpl::new(query.to_string()).unwrap();
        let insert_data = parser.parse_insert().unwrap();
        assert_eq!(insert_data.get_table(), "x");
        assert_eq!(
            insert_data.get_fields(),
            &vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            insert_data.get_values(),
            &vec![Constant::Int(3), Constant::String("string".to_string())]
        );
    }
    #[test]
    fn test_delete_sentence_with_where_phrase() {
        let query = "delete from x where a = 3 and b = 'string'";
        let mut parser = ParserImpl::new(query.to_string()).unwrap();
        let delete_data = parser.parse_delete().unwrap();
        assert_eq!(delete_data.get_table(), "x");
        let predicate = delete_data.get_predicate();
        assert_eq!(predicate.to_string(), "a = 3 and b = 'string'");
    }
    #[test]
    fn test_delete_sentence_without_where_phrase() {
        let query = "delete from x";
        let mut parser = ParserImpl::new(query.to_string()).unwrap();
        let delete_data = parser.parse_delete().unwrap();
        assert_eq!(delete_data.get_table(), "x");
        let predicate = delete_data.get_predicate();
        assert_eq!(predicate.to_string(), "");
    }
}
