use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Csv,
    Jsonl,
    /// Pretty-printed table for interactive use; nested cells are JSON-encoded.
    /// Buffers all rows before printing, so prefer `jsonl`/`csv` for very large
    /// inputs piped through `cat`/`head`/etc.
    Table,
}

/// How to render Binary / LargeBinary / FixedSizeBinary / BinaryView values.
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum BinaryFormat {
    /// Drop top-level binary columns from output; render nested binary as null.
    None,
    /// `\xHH` lowercase-hex-escaped strings.
    Hex,
    /// Standard-alphabet base64 strings.
    Base64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum SchemaType {
    /// Logical arrow schema.
    Arrow,
    /// Physical (format-native) schema.
    Physical,
}

/// Lance-specific selectors for which version of a dataset to read.
///
/// `--branch` is independent and can be combined with either `--version` or
/// `--tag`. `--version` and `--tag` are mutually exclusive (a tag is just a
/// named version). With no flags set, the latest version of `main` is used.
#[derive(Debug, Clone, Args, Default)]
pub struct LanceArgs {
    /// Read from the named Lance branch (default: main).
    #[arg(long)]
    pub branch: Option<String>,

    /// Read from a specific Lance version on the chosen branch.
    #[arg(long, conflicts_with = "tag")]
    pub version: Option<u64>,

    /// Read from a specific Lance tag on the chosen branch.
    #[arg(long, conflicts_with = "version")]
    pub tag: Option<String>,
}

impl LanceArgs {
    /// True when at least one Lance-specific selector was supplied.
    pub fn is_any_set(&self) -> bool {
        self.branch.is_some() || self.version.is_some() || self.tag.is_some()
    }
}

#[derive(Debug, Parser)]
#[command(name = "arrs", about = "Inspect Arrow-based datasets.", version)]
pub struct Cli {
    /// Output format for row-producing commands. When unset, metadata commands
    /// (versions/branches/tags/indices) default to `table` (fully buffered to enable column
    /// alignment); everything else to `jsonl` (streaming).
    #[arg(long, global = true, value_enum)]
    pub format: Option<Format>,

    /// How to render binary columns in the output.
    #[arg(long = "binary-format", global = true, value_enum, default_value_t = BinaryFormat::None)]
    pub binary_format: BinaryFormat,

    /// Comma-separated list of columns to include.
    #[arg(long, global = true, value_delimiter = ',')]
    pub columns: Option<Vec<String>>,

    /// Comma-separated list of columns to exclude. Takes precedence over --columns.
    #[arg(long = "exclude-columns", global = true, value_delimiter = ',')]
    pub exclude_columns: Option<Vec<String>>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Concatenate one or more datasets and print every row.
    Cat {
        /// Dataset paths (at least one).
        #[arg(required = true)]
        inputs: Vec<PathBuf>,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the first N rows.
    Head {
        input: PathBuf,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the last N rows.
    Tail {
        input: PathBuf,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print rows at the given indices (comma-separated; supports `a:b`, `a:`, `:b`, negatives).
    Take {
        input: PathBuf,
        #[arg(long, allow_hyphen_values = true)]
        indices: String,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the number of rows.
    Rowcount {
        input: PathBuf,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Randomly sample N rows without replacement.
    Sample {
        input: PathBuf,
        #[arg(short = 'n', long)]
        limit: u64,
        /// Optional u64 seed for reproducibility.
        #[arg(long)]
        seed: Option<u64>,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the schema of the dataset.
    Schema {
        input: PathBuf,
        /// Which schema flavor to print.
        #[arg(long = "type", value_enum, default_value_t = SchemaType::Arrow)]
        ty: SchemaType,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// (Lance only) Print versions of the dataset.
    Versions {
        input: PathBuf,
        /// Scope to a specific branch (default: main).
        #[arg(long)]
        branch: Option<String>,
        /// Hide versions that have no tag (default: show all versions).
        #[arg(long = "tagged-only", default_value_t = false)]
        tagged_only: bool,
    },

    /// (Lance only) Print branches available for the dataset.
    Branches { input: PathBuf },

    /// (Lance only) Print tags defined on the dataset, across all branches.
    Tags { input: PathBuf },

    /// (Lance only) Print indices defined on the dataset.
    Indices {
        input: PathBuf,
        #[command(flatten)]
        lance: LanceArgs,
    },
}
