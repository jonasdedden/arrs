//! Parser and resolver for the `--indices` flag.
//!
//! Grammar:
//! ```text
//! indices := expr ("," expr)*
//! expr    := int | range
//! range   := int? ":" int?
//! int     := "-"? [0-9]+
//! ```
//!
//! Resolution against a dataset with `rowcount` rows:
//! - Negative `i` means `rowcount + i` (errors if still < 0).
//! - `a:b` is inclusive on both ends.
//! - `a:` means `a..=rowcount-1`.
//! - `:b` means `0..=b`.
//! - `:` means the entire dataset.
//! - Order is preserved and duplicates are not removed.

use crate::Result;
use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expr {
    Single(i64),
    Range(Option<i64>, Option<i64>),
}

fn parse_int(s: &str) -> std::result::Result<i64, String> {
    s.parse::<i64>()
        .map_err(|_| format!("invalid integer '{s}'"))
}

fn parse_expr(tok: &str) -> std::result::Result<Expr, String> {
    let tok = tok.trim();
    if tok.is_empty() {
        return Err("empty index expression".to_string());
    }
    if let Some((lhs, rhs)) = tok.split_once(':') {
        let start = if lhs.is_empty() {
            None
        } else {
            Some(parse_int(lhs)?)
        };
        let end = if rhs.is_empty() {
            None
        } else {
            Some(parse_int(rhs)?)
        };
        Ok(Expr::Range(start, end))
    } else {
        Ok(Expr::Single(parse_int(tok)?))
    }
}

fn parse(raw: &str) -> std::result::Result<Vec<Expr>, String> {
    if raw.trim().is_empty() {
        return Err("--indices must not be empty".to_string());
    }
    raw.split(',').map(parse_expr).collect()
}

fn resolve_single(i: i64, rowcount: u64) -> Result<u64> {
    let rc_i: i64 =
        i64::try_from(rowcount).map_err(|_| Error::IndexOutOfRange { index: i, rowcount })?;
    let resolved = if i < 0 { rc_i + i } else { i };
    if resolved < 0 || resolved >= rc_i {
        return Err(Error::IndexOutOfRange { index: i, rowcount });
    }
    Ok(resolved as u64)
}

/// Parse `--indices` and expand against the dataset row count.
pub fn resolve(raw: &str, rowcount: u64) -> Result<Vec<u64>> {
    let exprs = parse(raw).map_err(Error::IndexParse)?;
    let rc_i: i64 =
        i64::try_from(rowcount).map_err(|_| Error::IndexOutOfRange { index: 0, rowcount })?;
    let mut out = Vec::new();
    for expr in exprs {
        match expr {
            Expr::Single(i) => {
                if rowcount == 0 {
                    return Err(Error::IndexOutOfRange { index: i, rowcount });
                }
                out.push(resolve_single(i, rowcount)?);
            }
            Expr::Range(start, end) => {
                if rowcount == 0 {
                    return Err(Error::IndexOutOfRange {
                        index: start.or(end).unwrap_or(0),
                        rowcount,
                    });
                }
                let start_raw = start.unwrap_or(0);
                let end_raw = end.unwrap_or(rc_i - 1);
                let s = resolve_single(start_raw, rowcount)?;
                let e = resolve_single(end_raw, rowcount)?;
                if s > e {
                    return Err(Error::EmptyRange {
                        start: start_raw,
                        end: end_raw,
                    });
                }
                out.extend(s..=e);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_positive() {
        assert_eq!(resolve("5", 10).unwrap(), vec![5]);
    }

    #[test]
    fn single_negative() {
        assert_eq!(resolve("-1", 10).unwrap(), vec![9]);
        assert_eq!(resolve("-10", 10).unwrap(), vec![0]);
    }

    #[test]
    fn range_closed() {
        assert_eq!(resolve("2:5", 10).unwrap(), vec![2, 3, 4, 5]);
    }

    #[test]
    fn range_open_start() {
        assert_eq!(resolve(":3", 10).unwrap(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn range_open_end() {
        assert_eq!(resolve("7:", 10).unwrap(), vec![7, 8, 9]);
    }

    #[test]
    fn range_full() {
        assert_eq!(resolve(":", 4).unwrap(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn range_negative_to_negative() {
        // Last 5 of 10 rows = indices 5..=9
        assert_eq!(resolve("-5:-1", 10).unwrap(), vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn range_negative_to_positive() {
        assert_eq!(resolve("-5:9", 10).unwrap(), vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn order_preserved_and_dupes_kept() {
        assert_eq!(resolve("3,1,1,0:2", 5).unwrap(), vec![3, 1, 1, 0, 1, 2]);
    }

    #[test]
    fn out_of_range_positive() {
        assert!(matches!(
            resolve("10", 10),
            Err(Error::IndexOutOfRange { .. })
        ));
    }

    #[test]
    fn out_of_range_negative() {
        assert!(matches!(
            resolve("-11", 10),
            Err(Error::IndexOutOfRange { .. })
        ));
    }

    #[test]
    fn empty_range_error() {
        assert!(matches!(resolve("5:2", 10), Err(Error::EmptyRange { .. })));
    }

    #[test]
    fn invalid_int_error() {
        assert!(matches!(resolve("abc", 10), Err(Error::IndexParse(_))));
    }

    #[test]
    fn empty_input_error() {
        assert!(matches!(resolve("", 10), Err(Error::IndexParse(_))));
    }

    #[test]
    fn empty_dataset_single_error() {
        assert!(matches!(
            resolve("0", 0),
            Err(Error::IndexOutOfRange { .. })
        ));
    }
}
