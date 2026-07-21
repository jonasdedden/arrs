use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

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

/// Ordering for `freq` output rows.
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum FreqSort {
    /// Most frequent values first; ties broken by value (ascending, NULL last).
    Count,
    /// Values in ascending order (NULL last).
    Value,
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
/// `--branch` is independent and can be combined with any of `--version`,
/// `--tag`, or `--as-of`. `--version`, `--tag`, and `--as-of` all name a
/// single version and are therefore mutually exclusive. With no flags set,
/// the latest version of `main` is used.
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

    /// Read the latest version whose commit timestamp is at or before this
    /// instant, on the chosen branch. Accepts RFC 3339 with an offset
    /// (`2026-07-01T12:00:00Z`), a naive datetime interpreted as UTC
    /// (`2026-07-01T12:00:00`), or a date interpreted as midnight UTC
    /// (`2026-07-01`).
    #[arg(long = "as-of", conflicts_with_all = ["version", "tag"])]
    pub as_of: Option<String>,
}

impl LanceArgs {
    /// True when at least one Lance-specific selector was supplied.
    pub fn is_any_set(&self) -> bool {
        self.branch.is_some()
            || self.version.is_some()
            || self.tag.is_some()
            || self.as_of.is_some()
    }
}

/// SQL-style row predicate shared by every row-producing command (and
/// `rowcount`). Kept as its own flattened `Args` group so the flag definition
/// lives in exactly one place.
#[derive(Debug, Clone, Args, Default)]
pub struct FilterArg {
    /// Keep only rows matching this SQL-style predicate (e.g.
    /// `"score > 0.5 AND split = 'test'"`). Applied before row selection, so
    /// `head`/`tail`/`sample` operate on the matching rows.
    #[arg(long = "where", value_name = "PREDICATE")]
    pub predicate: Option<String>,
}

#[derive(Debug, Parser)]
#[command(name = "arrs", about = "Inspect Arrow-based datasets.", version)]
pub struct Cli {
    /// Output format for row-producing commands. When unset, metadata commands
    /// (versions/branches/tags/indices/fragments) default to `table` (fully buffered to enable
    /// column alignment); everything else to `jsonl` (streaming).
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
        /// Dataset paths or object-store URIs (at least one).
        #[arg(required = true)]
        inputs: Vec<String>,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the first N rows.
    Head {
        input: String,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the last N rows.
    Tail {
        input: String,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u64,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print rows at the given indices (comma-separated; supports `a:b`, `a:`, `:b`, negatives).
    Take {
        input: String,
        #[arg(long, allow_hyphen_values = true)]
        indices: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the number of rows.
    Rowcount {
        input: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Randomly sample N rows without replacement.
    Sample {
        input: String,
        #[arg(short = 'n', long)]
        limit: u64,
        /// Optional u64 seed for reproducibility.
        #[arg(long)]
        seed: Option<u64>,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print per-column summary statistics (like `df.describe()`).
    ///
    /// One row per column: count (non-null), nulls, min/max (numeric, temporal,
    /// string, boolean), mean/stddev (numeric only), and an approximate distinct
    /// count (exact up to a cap, then reported as `>N`). Nested/binary/decimal
    /// columns report count and nulls only.
    Stats {
        input: String,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Count occurrences of each distinct value in a column (value counts).
    Freq {
        input: String,
        /// Column to compute value counts for.
        #[arg(long)]
        column: String,
        /// Show only the top N rows; the remaining values are summarized as an
        /// `<other>` row. Without this, every distinct value is listed. Must be
        /// at least 1.
        #[arg(short = 'n', long, value_parser = clap::value_parser!(u64).range(1..))]
        limit: Option<u64>,
        /// Row ordering: `count` (most frequent first, default) or `value`.
        #[arg(long, value_enum, default_value_t = FreqSort::Count)]
        sort: FreqSort,
        #[command(flatten)]
        filter: FilterArg,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Print the schema of the dataset.
    Schema {
        input: String,
        /// Which schema flavor to print.
        #[arg(long = "type", value_enum, default_value_t = SchemaType::Arrow)]
        ty: SchemaType,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// (Lance only) Print versions of the dataset.
    Versions {
        input: String,
        /// Scope to a specific branch (default: main).
        #[arg(long)]
        branch: Option<String>,
        /// Hide versions that have no tag (default: show all versions).
        #[arg(long = "tagged-only", default_value_t = false)]
        tagged_only: bool,
    },

    /// (Lance only) Print branches available for the dataset.
    Branches { input: String },

    /// (Lance only) Print tags defined on the dataset, across all branches.
    Tags { input: String },

    /// (Lance only) Print indices defined on the dataset.
    Indices {
        input: String,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// (Lance only) List fragments with row, deletion, file, and size info.
    Fragments {
        input: String,
        /// Show each fragment's data file paths in table output (they are
        /// always included in jsonl/csv output).
        #[arg(long)]
        verbose: bool,
        /// Skip on-disk size computation, avoiding object-store lookups on very
        /// remote or huge datasets. The `size` column is left empty.
        #[arg(long = "no-size")]
        no_size: bool,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// (Lance only) Nearest-neighbor vector search; appends a `_distance` column.
    ///
    /// Uses an ANN index on the column when present, otherwise falls back to
    /// flat (brute-force) KNN and prints a note to stderr. The output honours
    /// the global `--columns`/`--exclude-columns` projection, with `_distance`
    /// always included.
    #[command(group(ArgGroup::new("query_vector").required(true).args(["vector", "vector_file"])))]
    Search {
        input: String,
        /// Vector column to search (a fixed-size-list-of-float column).
        #[arg(long)]
        column: String,
        /// Query vector as an inline JSON array, e.g. '[0.1, 0.2, 0.3]'.
        #[arg(long)]
        vector: Option<String>,
        /// Read the query vector (a JSON array) from a file, or '-' for stdin.
        #[arg(long = "vector-file")]
        vector_file: Option<PathBuf>,
        /// Number of nearest neighbors to return.
        #[arg(short = 'k', default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        k: u64,
        /// IVF partitions to probe (index tuning; no effect without an index).
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        nprobes: Option<u64>,
        /// Refine factor for re-ranking search results (index tuning).
        #[arg(long = "refine-factor")]
        refine_factor: Option<u32>,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// (Lance only) Print per-index coverage: indexed vs unindexed row counts.
    IndexStats {
        input: String,
        #[command(flatten)]
        lance: LanceArgs,
    },
}
