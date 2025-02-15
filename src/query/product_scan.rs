use super::{constant::Constant, scan::ReadScan};

use anyhow::Result as AnyhowResult;

pub struct ProductScan {
    s1: Box<dyn ReadScan>,
    s2: Box<dyn ReadScan>,
}

impl ReadScan for ProductScan {
    fn before_first(&mut self) -> AnyhowResult<()> {
        self.s1.before_first()?;
        self.s1.move_next()?;
        self.s2.before_first()?;
        Ok(())
    }

    fn move_next(&mut self) -> AnyhowResult<bool> {
        if self.s2.move_next()? {
            Ok(true)
        } else {
            self.s2.before_first()?;
            Ok(self.s2.move_next()? && self.s1.move_next()?)
        }
    }

    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant> {
        if self.s1.has_field(field_name) {
            self.s1.get_val(field_name)
        } else {
            self.s2.get_val(field_name)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.s1.has_field(field_name) || self.s2.has_field(field_name)
    }
}

impl ProductScan {
    pub fn new(s1: Box<dyn ReadScan>, s2: Box<dyn ReadScan>) -> Self {
        Self { s1, s2 }
    }
}

#[cfg(test)]
mod test_product_scan {
    use mockall::predicate::eq;

    use crate::query::scan::MockReadScan;

    use super::*;

    #[test]
    fn test_product_scan() {
        // "a" という int 型, "b" という String 型の field を持つ scan
        // (1, "one)", (2, "two"), (3, "three") というレコードがある
        let s1 = {
            let mut scan = MockReadScan::new();
            scan.expect_before_first().returning(|| Ok(()));

            {
                let mut count = 0;
                scan.expect_move_next().times(4).returning(move || {
                    count += 1;
                    Ok(count <= 3)
                });
            }

            scan.expect_has_field()
                .returning(|field_name| field_name == "a" || field_name == "b");

            // s2 が 2 つのレコードを持っているので、s1 は 2 周する
            {
                let mut a_count = 0;
                scan.expect_get_val()
                    .with(eq("a"))
                    .times(6)
                    .returning(move |_| {
                        a_count += 1;
                        Ok(Constant::Int((a_count + 1) / 2))
                    });

                let mut b_count = 0;
                scan.expect_get_val()
                    .with(eq("b"))
                    .times(6)
                    .returning(move |_| {
                        b_count += 1;
                        match b_count {
                            1 | 2 => Ok(Constant::String("one".to_string())),
                            3 | 4 => Ok(Constant::String("two".to_string())),
                            5 | 6 => Ok(Constant::String("three".to_string())),
                            _ => unreachable!(),
                        }
                    });
            }

            scan
        };

        // "c" という int 型の field を持つ scan
        // 4, 5 というレコードがある
        let s2 = {
            let mut scan = MockReadScan::new();
            scan.expect_before_first().returning(|| Ok(()));

            {
                // s1 が 3 つのレコードを持っているので、s2 は 3 周する
                let mut count = 0;
                scan.expect_move_next().returning(move || {
                    count += 1;
                    if count % 3 == 0 {
                        Ok(false)
                    } else {
                        Ok(true)
                    }
                });
            }

            {
                let mut count = 0;
                scan.expect_get_val().with(eq("c")).returning(move |_| {
                    count += 1;
                    if count % 2 == 0 {
                        Ok(Constant::Int(5))
                    } else {
                        Ok(Constant::Int(4))
                    }
                });
            }

            scan
        };

        // act & assert
        let mut product_scan = ProductScan::new(Box::new(s1), Box::new(s2));
        product_scan.before_first().unwrap();

        // (1, "one", 4)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(1));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("one".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(4));

        // (1, "one", 5)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(1));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("one".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(5));

        // (2, "two", 4)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(2));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("two".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(4));

        // (2, "two", 5)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(2));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("two".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(5));

        // (3, "three", 4)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(3));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("three".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(4));

        // (3, "three", 5)
        product_scan.move_next().unwrap();
        assert_eq!(product_scan.get_val("a").unwrap(), Constant::Int(3));
        assert_eq!(
            product_scan.get_val("b").unwrap(),
            Constant::String("three".to_string())
        );
        assert_eq!(product_scan.get_val("c").unwrap(), Constant::Int(5));

        // end
        assert!(!product_scan.move_next().unwrap());
    }
}
