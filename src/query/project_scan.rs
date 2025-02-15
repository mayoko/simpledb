use std::collections::HashSet;

use anyhow::{anyhow, Result as AnyhowResult};
use thiserror::Error;

use crate::record::rid::Rid;

use super::{
    constant::Constant,
    scan::{ReadScan, ReadScanError, Scan, UpdateScan},
};

pub struct ProjectScan {
    scan: Scan,
    field_list: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum ProjectScanError {
    #[error("[project scan] invalid call : {0}")]
    InvalidCall(String),
}

impl ReadScan for ProjectScan {
    fn before_first(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(ref mut scan) => scan.before_first(),
            Scan::Updatable(ref mut scan) => scan.before_first(),
        }
    }

    fn move_next(&mut self) -> AnyhowResult<bool> {
        match self.scan {
            Scan::ReadOnly(ref mut scan) => scan.move_next(),
            Scan::Updatable(ref mut scan) => scan.move_next(),
        }
    }

    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant> {
        // new する段階で field_list に含まれている field のみを scan に渡すので、
        // ここでは field_list に含まれているかどうかを返すだけで良い
        if self.field_list.contains(field_name) {
            Ok(match self.scan {
                Scan::ReadOnly(ref scan) => scan.get_val(field_name),
                Scan::Updatable(ref scan) => scan.get_val(field_name),
            }?)
        } else {
            Err(anyhow!(ReadScanError::InvalidCall(format!(
                "field {} not found for the project scan. It expects one of {:?}",
                field_name, self.field_list
            ))))
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        // new する段階で field_list に含まれている field のみを scan に渡すので、
        // ここでは field_list に含まれているかどうかを返すだけで良い
        self.field_list.contains(field_name)
    }
}

impl UpdateScan for ProjectScan {
    fn insert(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(ProjectScanError::InvalidCall(
                "insert called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.insert(),
        }
    }

    fn delete(&mut self) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(ProjectScanError::InvalidCall(
                "delete called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.delete(),
        }
    }

    fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()> {
        // new する段階で field_list に含まれている field のみを scan に渡すので、
        // ここでは field_list に含まれているかどうかを返すだけで良い
        if self.field_list.contains(field_name) {
            match self.scan {
                Scan::ReadOnly(_) => Err(anyhow!(ProjectScanError::InvalidCall(
                    "set_val called on read-only scan".to_string()
                ))),
                Scan::Updatable(ref scan) => scan.set_val(field_name, val),
            }
        } else {
            Err(anyhow!(ProjectScanError::InvalidCall(format!(
                "field {} not found for the project scan. It expects one of {:?}",
                field_name, self.field_list
            ))))
        }
    }

    fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(ProjectScanError::InvalidCall(
                "move_to_rid called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref mut scan) => scan.move_to_rid(rid),
        }
    }

    fn get_rid(&self) -> AnyhowResult<Rid> {
        match self.scan {
            Scan::ReadOnly(_) => Err(anyhow!(ProjectScanError::InvalidCall(
                "get_rid called on read-only scan".to_string()
            ))),
            Scan::Updatable(ref scan) => scan.get_rid(),
        }
    }
}

impl ProjectScan {
    pub fn new(scan: Scan, field_list: HashSet<String>) -> AnyhowResult<Self> {
        for field in &field_list {
            let has_field = match scan {
                Scan::ReadOnly(ref scan) => scan.has_field(field),
                Scan::Updatable(ref scan) => scan.has_field(field),
            };
            if !has_field {
                return Err(anyhow!(ProjectScanError::InvalidCall(format!(
                    "field {} not found for the scan.",
                    field,
                ))));
            }
        }
        Ok(Self { scan, field_list })
    }
}

#[cfg(test)]
mod project_scan_test {
    use mockall::predicate::eq;

    use super::*;
    use crate::query::scan::{MockReadScan, MockUpdateScan};

    #[test]
    fn test_new_fails_if_field_does_not_exist() {
        let scan = {
            let mut scan = MockUpdateScan::new();
            scan.expect_has_field().returning(|_| false);
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        assert!(ProjectScan::new(Scan::Updatable(Box::new(scan)), field_list).is_err());
    }

    #[test]
    fn test_new_succeeds_if_field_exists() {
        let scan = {
            let mut scan = MockUpdateScan::new();
            // field_list に２つの要素が含まれているので、has_field は２回呼ばれる
            scan.expect_has_field().times(2).returning(|_| true);
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string(), "b".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        assert!(ProjectScan::new(Scan::Updatable(Box::new(scan)), field_list).is_ok());
    }

    #[test]
    fn test_get_val_fails_if_field_does_not_exist() {
        let scan = {
            let mut scan = MockReadScan::new();
            scan.expect_has_field().returning(|_| true);
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        let project_scan = ProjectScan::new(Scan::ReadOnly(Box::new(scan)), field_list).unwrap();
        // "b" は field_list に含まれていないのでエラーになる
        assert!(project_scan.get_val("b").is_err());
    }

    #[test]
    fn test_get_val_returns_value_if_field_exists() {
        let scan = {
            let mut scan = MockReadScan::new();
            scan.expect_has_field().returning(|_| true);
            scan.expect_get_val().returning(|_| Ok(Constant::Int(1)));
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        let project_scan = ProjectScan::new(Scan::ReadOnly(Box::new(scan)), field_list).unwrap();

        let result = project_scan.get_val("a");

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Constant::Int(1)));
    }

    #[test]
    fn test_set_val_fails_if_field_does_not_exist() {
        let scan = {
            let mut scan = MockUpdateScan::new();
            scan.expect_has_field().returning(|_| true);
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        let project_scan = ProjectScan::new(Scan::ReadOnly(Box::new(scan)), field_list).unwrap();
        // "b" は field_list に含まれていないのでエラーになる
        assert!(project_scan.set_val("b", &Constant::Int(1)).is_err());
    }

    #[test]
    fn test_set_val_returns_value_if_field_exists() {
        let scan = {
            let mut scan = MockUpdateScan::new();
            scan.expect_has_field().returning(|_| true);
            scan.expect_set_val()
                .with(eq("a"), eq(&Constant::Int(1)))
                .returning(|_, _| Ok(()));
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        let project_scan = ProjectScan::new(Scan::Updatable(Box::new(scan)), field_list).unwrap();

        assert!(project_scan.set_val("a", &Constant::Int(1)).is_ok());
    }

    #[test]
    fn test_set_val_returns_error_if_parent_scan_is_readonly() {
        let scan = {
            let mut scan = MockUpdateScan::new();
            scan.expect_has_field().returning(|_| true);
            scan
        };
        let field_list: HashSet<String> = vec!["a".to_string()]
            .into_iter()
            .collect::<HashSet<String>>();
        // scan 自体は Updatable だが、ReadOnly として渡してしまっているのでエラーが起きる
        let project_scan = ProjectScan::new(Scan::ReadOnly(Box::new(scan)), field_list).unwrap();

        assert!(project_scan.set_val("a", &Constant::Int(1)).is_err());
    }
}
