use crate::query::{
    constant::Constant,
    expression::Expression,
    predicate::ProductPredicate,
    term::{EqualTerm, Term},
};

use super::{
    constant::KEYWORDS,
    content::query_data::QueryData,
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
}
