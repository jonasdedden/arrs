//! Resolve `--columns` / `--exclude-columns` against an arrow schema.

use std::collections::HashSet;

use arrow_schema::SchemaRef;

use crate::Result;
use crate::error::Error;

/// Resolve the requested projection against `schema`. Returns `None` when neither
/// flag was provided (callers interpret as "all columns, no filtering").
///
/// `exclude` takes precedence over `include`: if both are set, the result is
/// `<all columns> \ exclude` (the include list is ignored, matching the spec).
pub fn resolve(
    schema: &SchemaRef,
    include: Option<&[String]>,
    exclude: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    if include.is_none() && exclude.is_none() {
        return Ok(None);
    }

    let all: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let available_set: HashSet<&str> = all.iter().copied().collect();

    if let Some(excl) = exclude {
        check_dupes(excl)?;
        check_present(excl, &available_set, &all)?;
        let excl_set: HashSet<&str> = excl.iter().map(String::as_str).collect();
        let remaining: Vec<String> = all
            .iter()
            .filter(|c| !excl_set.contains(*c))
            .map(|c| (*c).to_string())
            .collect();
        return Ok(Some(remaining));
    }

    // Only --columns is set.
    let incl = include.expect("checked above");
    check_dupes(incl)?;
    check_present(incl, &available_set, &all)?;
    Ok(Some(incl.to_vec()))
}

fn check_dupes(cols: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for c in cols {
        if !seen.insert(c.as_str()) {
            return Err(Error::DuplicateColumn(c.clone()));
        }
    }
    Ok(())
}

fn check_present(cols: &[String], available: &HashSet<&str>, all_ordered: &[&str]) -> Result<()> {
    for c in cols {
        if !available.contains(c.as_str()) {
            return Err(Error::UnknownColumn {
                name: c.clone(),
                available: all_ordered.join(", "),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]))
    }

    #[test]
    fn none_means_all() {
        let s = schema();
        assert!(resolve(&s, None, None).unwrap().is_none());
    }

    #[test]
    fn include_preserves_user_order() {
        let s = schema();
        let incl = vec!["c".to_string(), "a".to_string()];
        let got = resolve(&s, Some(&incl), None).unwrap().unwrap();
        assert_eq!(got, vec!["c".to_string(), "a".to_string()]);
    }

    #[test]
    fn exclude_keeps_schema_order() {
        let s = schema();
        let excl = vec!["b".to_string()];
        let got = resolve(&s, None, Some(&excl)).unwrap().unwrap();
        assert_eq!(got, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn exclude_takes_precedence() {
        let s = schema();
        let incl = vec!["a".to_string(), "b".to_string()];
        let excl = vec!["b".to_string()];
        let got = resolve(&s, Some(&incl), Some(&excl)).unwrap().unwrap();
        assert_eq!(got, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn unknown_column_errors() {
        let s = schema();
        let incl = vec!["zzz".to_string()];
        assert!(matches!(
            resolve(&s, Some(&incl), None),
            Err(Error::UnknownColumn { .. })
        ));
    }

    #[test]
    fn duplicate_errors() {
        let s = schema();
        let incl = vec!["a".to_string(), "a".to_string()];
        assert!(matches!(
            resolve(&s, Some(&incl), None),
            Err(Error::DuplicateColumn(_))
        ));
    }
}
