use anyhow::{anyhow, Result as AnyhowResult};
use std::collections::HashSet;
use thiserror::Error;

/**
 * Parser で扱う token の種類
 */
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum Token {
    // 予約語
    Keyword(String),
    // 識別子
    Id(String),
    // 区切り文字
    Delimiter(char),
    // 文字列リテラル
    StringConstant(String),
    // 数値リテラル
    IntConstant(i32),
    #[default]
    None,
}

/**
 * 入力した文字列を token に分割しながら読んでいく class
 */
pub struct Lexer {
    input: String,
    position: usize, // byte 単位での位置 (utf-8 なので、文字単位の位置とは必ずしも一致しない)
    token: Token,
    keywords: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("Unexpected token")]
    UnexpectedToken(String),
    #[error("internal error")]
    Internal(String),
}

impl Lexer {
    pub fn new(input: String, keywords: HashSet<String>) -> AnyhowResult<Lexer> {
        let mut lexer = Lexer {
            input,
            position: 0,
            token: Token::None,
            keywords,
        };
        lexer.token = lexer.read_token()?;
        Ok(lexer)
    }
    /**
     * token に match したら true を返す
     */
    pub fn is_matched(&self, token: Token) -> bool {
        self.token == token
    }
    /**
     * token に match したら、match した分だけ読み進める
     * そうでないばあいは error を返す
     */
    pub fn eat_exact(&mut self, token: Token) -> AnyhowResult<()> {
        if self.token == token {
            self.token = self.read_token()?;
            Ok(())
        } else {
            Err(anyhow!(LexerError::UnexpectedToken(format!(
                "expected {:?}, but got {:?}",
                token, self.token
            ))))
        }
    }

    /// int constant を読み進める
    pub fn eat_int_constant(&mut self) -> AnyhowResult<i32> {
        match self.token {
            Token::IntConstant(val) => {
                self.token = self.read_token()?;
                Ok(val)
            }
            _ => Err(anyhow!(LexerError::UnexpectedToken(
                "expected integer constant".to_string()
            ))),
        }
    }

    /// string constant を読み進める
    pub fn eat_string_constant(&mut self) -> AnyhowResult<String> {
        match std::mem::take(&mut self.token) {
            Token::StringConstant(val) => {
                self.token = self.read_token()?;
                Ok(val)
            }
            _ => Err(anyhow!(LexerError::UnexpectedToken(
                "expected string constant".to_string()
            ))),
        }
    }

    pub fn eat_id(&mut self) -> AnyhowResult<String> {
        match std::mem::take(&mut self.token) {
            Token::Id(val) => {
                self.token = self.read_token()?;
                Ok(val)
            }
            _ => Err(anyhow!(LexerError::UnexpectedToken(
                "expected identifier".to_string()
            ))),
        }
    }

    /// トークンを読み進める
    fn read_token(&mut self) -> AnyhowResult<Token> {
        let mut chars = self.input[self.position..].chars();
        while let Some(c) = chars.next() {
            if c.is_whitespace() {
                self.position += c.len_utf8();
                continue;
            }
            if c == '\'' {
                // 文字列リテラル
                let mut str = String::new();
                for c in chars.by_ref() {
                    if c == '\'' {
                        break;
                    }
                    str.push(c);
                }
                self.position += str.len() + (2 * '\''.len_utf8());
                return Ok(Token::StringConstant(str));
            }
            // 数値リテラル
            let is_negative = if c == '-' {
                self.position += c.len_utf8();
                true
            } else {
                false
            };
            if c.is_numeric() {
                let mut num = String::new();
                if is_negative {
                    num.push('-');
                }
                num.push(c);
                for c in chars.by_ref() {
                    if c.is_numeric() {
                        num.push(c);
                    } else {
                        break;
                    }
                }
                self.position += num.len();
                return Ok(Token::IntConstant(num.parse().map_err(|_| {
                    anyhow!(LexerError::Internal(format!(
                        "failed to parse string into integer: {}",
                        num
                    )))
                })?));
            }

            if c.is_alphabetic() || c == '_' {
                let mut sval = String::new();
                sval.push(c);
                for c in chars.by_ref() {
                    if c.is_alphanumeric() {
                        sval.push(c);
                    } else {
                        break;
                    }
                }
                self.position += sval.len();
                return if self.keywords.contains(&sval) {
                    Ok(Token::Keyword(sval))
                } else {
                    Ok(Token::Id(sval))
                };
            }
            self.position += c.len_utf8();
            return Ok(Token::Delimiter(c));
        }
        Ok(Token::None)
    }
}

#[cfg(test)]
mod lexer_test {
    use crate::parse::constant::KEYWORDS;

    use super::*;
    #[test]
    fn test_legal_input() {
        let mut lexer = Lexer::new(
            "select a from x, z where b = 3 and c = 'string'".to_string(),
            KEYWORDS.iter().map(|&s| s.to_string()).collect(),
        )
        .unwrap();
        assert!(lexer.is_matched(Token::Keyword("select".to_string())));
        lexer
            .eat_exact(Token::Keyword("select".to_string()))
            .unwrap();

        assert!(lexer.is_matched(Token::Id("a".to_string())));
        assert_eq!(lexer.eat_id().unwrap(), "a");

        assert!(lexer.is_matched(Token::Keyword("from".to_string())));
        lexer.eat_exact(Token::Keyword("from".to_string())).unwrap();

        assert!(lexer.is_matched(Token::Id("x".to_string())));
        assert_eq!(lexer.eat_id().unwrap(), "x");

        assert!(lexer.is_matched(Token::Delimiter(',')));
        lexer.eat_exact(Token::Delimiter(',')).unwrap();

        assert!(lexer.is_matched(Token::Id("z".to_string())));
        assert_eq!(lexer.eat_id().unwrap(), "z");

        assert!(lexer.is_matched(Token::Keyword("where".to_string())));
        lexer
            .eat_exact(Token::Keyword("where".to_string()))
            .unwrap();

        assert!(lexer.is_matched(Token::Id("b".to_string())));
        assert_eq!(lexer.eat_id().unwrap(), "b");

        assert!(lexer.is_matched(Token::Delimiter('=')));
        lexer.eat_exact(Token::Delimiter('=')).unwrap();

        assert!(lexer.is_matched(Token::IntConstant(3)));
        assert_eq!(lexer.eat_int_constant().unwrap(), 3);

        assert!(lexer.is_matched(Token::Keyword("and".to_string())));
        lexer.eat_exact(Token::Keyword("and".to_string())).unwrap();

        assert!(lexer.is_matched(Token::Id("c".to_string())));
        assert_eq!(lexer.eat_id().unwrap(), "c");

        assert!(lexer.is_matched(Token::Delimiter('=')));
        lexer.eat_exact(Token::Delimiter('=')).unwrap();

        assert!(lexer.is_matched(Token::StringConstant("string".to_string())));
        assert_eq!(lexer.eat_string_constant().unwrap(), "string");

        assert!(lexer.is_matched(Token::None));
    }

    #[test]
    fn test_it_returns_error_if_unmatching_token() {
        let mut lexer = Lexer::new(
            "select a from x, z where b = 3".to_string(),
            KEYWORDS.iter().map(|&s| s.to_string()).collect(),
        )
        .unwrap();

        // select なのにあえて from を読む
        assert!(lexer.is_matched(Token::Keyword("select".to_string())));
        assert!(lexer.eat_exact(Token::Keyword("from".to_string())).is_err());

        // 次は select を読んでみると、ちゃんと読める
        lexer
            .eat_exact(Token::Keyword("select".to_string()))
            .unwrap();
    }
}
