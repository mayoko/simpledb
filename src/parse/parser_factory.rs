use super::parser::{Parser, ParserImpl};

use anyhow::Result as AnyhowResult;

pub struct ParserFactory {}

impl ParserFactory {
    pub fn new() -> Self {
        Self {}
    }
    pub fn create(&self, query: String) -> AnyhowResult<Box<dyn Parser>> {
        Ok(Box::new(ParserImpl::new(query)?))
    }
}
