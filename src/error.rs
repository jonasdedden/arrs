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
        left: PathBuf,
        right: PathBuf,
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

    #[error("sample size {requested} is larger than dataset row count {rowcount}")]
    SampleTooLarge { requested: u64, rowcount: u64 },

    #[error("failed to open lance dataset at {path}")]
    LanceOpen {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("lance operation failed")]
    Lance(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("unrecognised dataset format at {path}")]
    UnknownFormat { path: PathBuf },

    #[error("--branch/--version/--tag are only valid for Lance datasets ({path})")]
    LanceFlagsOnNonLance { path: PathBuf },

    #[error("'{command}' is only valid for Lance datasets ({path})")]
    NotLance {
        command: &'static str,
        path: PathBuf,
    },

    #[error(
        "tag '{tag}' is on branch '{tag_branch}', not '{requested_branch}'; remove --branch or pass --branch {tag_branch}"
    )]
    TagBranchMismatch {
        tag: String,
        tag_branch: String,
        requested_branch: String,
    },

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
