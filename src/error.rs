use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("no inputs provided")]
    EmptyInputs,

    #[error("unknown column '{name}' (available: {available})")]
    UnknownColumn { name: String, available: String },

    #[error("column '{0}' appears more than once in --columns/--exclude-columns")]
    DuplicateColumn(String),

    #[error(
        "input schemas differ: '{left}' and '{right}' do not match on field '{field}'; cat requires strictly matching schemas"
    )]
    SchemaMismatch {
        left: String,
        right: String,
        field: String,
    },

    #[error(
        "arrow type {data_type} in column '{column}' cannot be represented in CSV; use --format jsonl"
    )]
    UnsupportedCsvType { column: String, data_type: String },

    #[error("--indices parse error: {0}")]
    IndexParse(String),

    #[error("index {index} is out of range for dataset with {rowcount} rows")]
    IndexOutOfRange { index: i64, rowcount: u64 },

    #[error("range {start}:{end} is empty (start > end)")]
    EmptyRange { start: i64, end: i64 },

    #[error("sample size {requested} is larger than the available row count {rowcount}")]
    SampleTooLarge { requested: u64, rowcount: u64 },

    #[error("invalid --where predicate: {0}")]
    InvalidPredicate(String),

    #[error(
        "--where cannot be combined with 'take --indices'; indices are positional. Filter rows with another command (e.g. 'head --where ...') instead"
    )]
    TakeWhereConflict,

    #[error(
        "column '{column}' is not a vector column (found {data_type}, expected a fixed-size list of f16/f32/f64)"
    )]
    NotVectorColumn { column: String, data_type: String },

    #[error("query has {query} dims, column {column} has {column_dims}")]
    VectorDimMismatch {
        query: usize,
        column: String,
        column_dims: usize,
    },

    #[error("could not parse query vector as a JSON array of numbers: {0}")]
    VectorParse(String),

    #[error("failed to read query vector file {path}")]
    VectorFileRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to open lance dataset at {path}")]
    LanceOpen {
        path: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("lance operation failed")]
    Lance(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("unrecognised dataset format at {path}")]
    UnknownFormat { path: String },

    #[error("--branch/--version/--tag/--as-of are only valid for Lance datasets ({path})")]
    LanceFlagsOnNonLance { path: String },

    #[error("'{command}' is only valid for Lance datasets ({path})")]
    NotLance { command: &'static str, path: String },

    #[error(
        "tag '{tag}' is on branch '{tag_branch}', not '{requested_branch}'; remove --branch or pass --branch {tag_branch}"
    )]
    TagBranchMismatch {
        tag: String,
        tag_branch: String,
        requested_branch: String,
    },

    #[error(
        "could not parse --as-of value '{0}': expected RFC 3339 with offset (e.g. 2026-07-01T12:00:00Z), a naive datetime interpreted as UTC (e.g. 2026-07-01T12:00:00), or a date interpreted as midnight UTC (e.g. 2026-07-01)"
    )]
    InvalidAsOf(String),

    #[error(
        "--as-of {requested} predates the earliest version on this branch (earliest: {earliest}); pass a timestamp at or after it"
    )]
    AsOfBeforeFirstVersion { requested: String, earliest: String },

    #[error("--format is not applicable to '{command}' (it does not emit row-shaped output)")]
    FormatNotApplicable { command: &'static str },

    #[error("arrow error")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("csv writer error")]
    Csv(#[from] csv::Error),

    #[error("json serialization error")]
    Json(#[from] serde_json::Error),
}
