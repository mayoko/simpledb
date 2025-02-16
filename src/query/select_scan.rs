use anyhow::{anyhow, Result as AnyhowResult};
use thiserror::Error;

use crate::record::rid::Rid;

use super::{
    constant::Constant,
    predicate::Predicate,
    scan::{ReadScan, Scan, UpdateScan},
};

pub struct SelectScan {
    scan: Scan,
    pred: Box<dyn Predicate>,
}

#[derive(Error, Debug)]
pub enum SelectScanError {
    #[error("[select scan] invalid call : {0}")]
    InvalidCall(String),
}

impl ReadScan for SelectScan {
    fn before_first(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(ref mut scan) => scan.before_first(),
            Scan::Updatable(ref mut scan) => scan.before_first(),
        }
    }

    fn move_next(&mut self) -> AnyhowResult<bool> {
        loop {
            let has_next = match self.scan {
                Scan::ReadOnly(ref mut scan) => scan.move_next()?,
                Scan::Updatable(ref mut scan) => scan.move_next()?,
            };
            if !has_next {
                return Ok(false);
            }
            if self.pred.is_satisfied(&self.scan)? {
                return Ok(true);
            }
        }
    }

    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant> {
        match self.scan {
            Scan::ReadOnly(ref scan) => scan.get_val(field_name),
            Scan::Updatable(ref scan) => scan.get_val(field_name),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        match self.scan {
            Scan::ReadOnly(ref scan) => scan.has_field(field_name),
            Scan::Updatable(ref scan) => scan.has_field(field_name),
        }
    }
}

impl UpdateScan for SelectScan {
    fn insert(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(SelectScanError::InvalidCall(
                "insert called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.insert(),
        }
    }

    fn delete(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(SelectScanError::InvalidCall(
                "delete called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.delete(),
        }
    }

    fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(SelectScanError::InvalidCall(
                "set_val called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref scan) => scan.set_val(field_name, val),
        }
    }

    fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(SelectScanError::InvalidCall(
                "move_to_rid called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.move_to_rid(rid),
        }
    }

    fn get_rid(&self) -> AnyhowResult<Rid> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(SelectScanError::InvalidCall(
                "get_rid called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref scan) => scan.get_rid(),
        }
    }
}

impl SelectScan {
    pub fn new(scan: Scan, pred: Box<dyn Predicate>) -> Self {
        Self { scan, pred }
    }
}

#[cfg(test)]
mod select_scan_test {
    use crate::query::{predicate::MockPredicate, scan::MockReadScan};

    use super::*;

    #[test]
    fn move_next_test() {
        // 3 つの record を持つ scan を用意
        let scan = {
            let mut scan = MockReadScan::new();
            scan.expect_before_first().times(1).returning(|| Ok(()));

            scan.expect_move_next().times(3).returning(|| Ok(true));
            scan.expect_move_next().times(1).returning(|| Ok(false));

            {
                let mut count = 0;
                // record として 1, 2 を返す (record 1 => 1, record 2 => pred で skip, record 3 => 2)
                scan.expect_get_val().times(2).returning(move |_| {
                    count += 1;
                    Ok(Constant::Int(count))
                });
            }
            Scan::ReadOnly(Box::new(scan))
        };
        let pred = {
            let mut pred = MockPredicate::new();
            let mut count = 0;
            // 1, 2, 3 のうち奇数のみを返す
            pred.expect_is_satisfied().times(3).returning(move |_| {
                count += 1;
                Ok(count % 2 == 1)
            });
            Box::new(pred)
        };
        let mut select_scan = SelectScan::new(scan, pred);

        select_scan.before_first().unwrap();
        select_scan.move_next().unwrap();
        assert_eq!(select_scan.get_val("a").unwrap(), Constant::Int(1));
        select_scan.move_next().unwrap();
        assert_eq!(select_scan.get_val("a").unwrap(), Constant::Int(2));
        // もう値がないので false が返る
        assert!(!select_scan.move_next().unwrap());
    }
}
