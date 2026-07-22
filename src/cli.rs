use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Csv,
    Jsonl,
    /// Pretty-printed table for interactive use; nested cells are JSON-encoded.
    /// Buffers all rows before printing, so prefer `jsonl`/`csv` for very large
    /// inputs piped through `cat`/`head`/etc.
    Table,
    /// Arrow IPC streaming format written to stdout, for lossless composition
    /// with the Arrow ecosystem (DuckDB, Polars, pyarrow, ADBC). Batches go
    /// straight from the scan to `arrow::ipc::writer::StreamWriter`, so output is
    /// fully streaming at constant memory. Only valid on the row-producing
    /// commands (`cat`/`head`/`tail`/`take`/`sample`); the value-rendering flags
    /// (`--binary-format`, `--max-list-items`, `--max-cell-width`,
    /// `--float-precision`) do not apply to it and are rejected. arrs refuses to
    /// write it to a terminal — redirect or pipe it.
    Ipc,
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

/// The `--with-row-id` / `--with-row-addr` pseudo-column flags, shared by the
/// row-producing commands (`cat`/`head`/`tail`/`take`/`sample`). Lance-only; a
/// clear "not supported by this format" error is raised on any other backend.
/// Kept as its own flattened `Args` group so the flag definitions live in one
/// place.
#[derive(Debug, Clone, Args, Default)]
pub struct RowIdArgs {
    /// (Lance only) Append a `_rowid` column: the per-row identity. Stable
    /// across deletions; stable across compaction only for datasets written with
    /// Lance's stable row ids enabled (off by default, in which case `_rowid` is
    /// address-based and is rewritten by compaction). Always emitted regardless
    /// of --columns/--exclude-columns.
    #[arg(long = "with-row-id")]
    pub with_row_id: bool,

    /// (Lance only) Append a `_rowaddr` column: the physical address of the row
    /// (`fragment_id << 32 | offset`) in the current version. Always emitted
    /// regardless of --columns/--exclude-columns.
    #[arg(long = "with-row-addr")]
    pub with_row_addr: bool,
}

impl RowIdArgs {
    /// Convert the parsed flags into the [`crate::row_id::RowIds`] threaded
    /// through `ScanOptions` and `Dataset::take`.
    pub fn flags(&self) -> crate::row_id::RowIds {
        crate::row_id::RowIds {
            with_row_id: self.with_row_id,
            with_row_addr: self.with_row_addr,
        }
    }
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

    /// Truncate list / large-list / fixed-size-list rendering to the first N
    /// elements, appending an explicit marker element `… (K more)`. Applies to
    /// jsonl/json output and to nested table cells, at every nesting level.
    /// Lossy: truncated output is for viewing, not round-tripping. Default:
    /// unlimited (current output preserved byte-for-byte).
    #[arg(long = "max-list-items", global = true, value_name = "N")]
    pub max_list_items: Option<usize>,

    /// Table format only: truncate each rendered *data* cell to at most N
    /// characters, ending with `…` when shortened. Header cells (column names)
    /// are never truncated. Counts characters and never splits a multi-byte
    /// UTF-8 codepoint (CJK display width is out of scope). `N = 0` renders every
    /// non-empty cell as a bare `…`. Lossy. Default: unlimited.
    #[arg(long = "max-cell-width", global = true, value_name = "N")]
    pub max_cell_width: Option<usize>,

    /// Render f16 / f32 / f64 values with exactly N fractional digits in every
    /// format (`NaN`/`Infinity` are unaffected). Uses `format!`-style
    /// round-half-to-even. Lossy. Default: full round-trip precision.
    #[arg(long = "float-precision", global = true, value_name = "N")]
    pub float_precision: Option<usize>,

    /// Comma-separated list of columns to include.
    #[arg(long, global = true, value_delimiter = ',')]
    pub columns: Option<Vec<String>>,

    /// Comma-separated list of columns to exclude. Takes precedence over --columns.
    #[arg(long = "exclude-columns", global = true, value_delimiter = ',')]
    pub exclude_columns: Option<Vec<String>>,

    /// Disable the scan progress indicator on stderr. Progress is drawn only for
    /// long scans and only when stderr is a TTY, so it is already absent when
    /// output is piped; this flag suppresses it unconditionally.
    #[arg(long = "no-progress", global = true)]
    pub no_progress: bool,

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
        row_ids: RowIdArgs,
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
        row_ids: RowIdArgs,
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
        row_ids: RowIdArgs,
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
        row_ids: RowIdArgs,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Extract one cell's binary/blob payload to a file or stdout.
    ///
    /// Works on plain `Binary`/`LargeBinary`/`FixedSizeBinary`/`BinaryView`
    /// columns and on Lance blob-encoded columns (the latter streamed with
    /// bounded memory). Writes the raw bytes to `-o <file>` (overwriting any
    /// existing file), or to stdout when `-o` is omitted (refused when stdout is
    /// a terminal — redirect or use `-o`). Output is raw bytes, not rows, so the
    /// global `--format` flag is rejected; `--columns`/`--binary-format` do not
    /// apply and are ignored.
    Blob {
        input: String,
        /// Binary or Lance blob-encoded column to extract from.
        #[arg(long)]
        column: String,
        /// Row index to extract (single value; negatives count from the end,
        /// like `take`, so `-1` is the last row).
        #[arg(long, allow_hyphen_values = true)]
        index: i64,
        /// Write the payload to this file instead of stdout, overwriting any
        /// existing file. On success the file is written atomically; a failed
        /// extraction leaves no partial file.
        #[arg(short = 'o', long, value_name = "FILE")]
        output: Option<PathBuf>,
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
        row_ids: RowIdArgs,
        #[command(flatten)]
        lance: LanceArgs,
    },

    /// Per-column summary statistics, one row per column (like `df.describe()`).
    ///
    /// Scans the data to report count (non-null), nulls, min/max (numeric,
    /// temporal, string, boolean), mean/stddev (numeric only), and an approximate
    /// distinct count (exact up to a cap, then reported as `>N`). Nested/binary/
    /// decimal columns report count and nulls only. For a whole-dataset health
    /// summary (rows, fragments, size, versions) instead, see `stat`.
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

    /// (Lance only) One-screen dataset health summary — the `stat(1)` of datasets.
    ///
    /// Metadata-only (no data scan): rows, deleted rows + ratio, columns,
    /// fragment row-count spread, on-disk size, and version/branch/tag/index
    /// counts, with a conservative compaction hint. For per-column statistics
    /// (the `df.describe()`-style breakdown) use `stats` instead.
    Stat {
        input: String,
        /// Skip on-disk size computation, avoiding object-store lookups on very
        /// remote or huge datasets. The `data size` field is left empty/null.
        #[arg(long = "no-size")]
        no_size: bool,
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

    /// Diff two datasets, or two versions of one Lance dataset.
    ///
    /// One verb, two modes, chosen by how many datasets you name:
    ///
    /// * DATASET-VS-DATASET (`arrs diff A B`): compares two different datasets
    ///   (any backend) by schema, schema-metadata and row count. Being
    ///   backend-generic, it takes no Lance version selectors; passing
    ///   --from/--to/--from-tag/--to-tag/--branch alongside a second dataset is
    ///   an error.
    ///
    /// * VERSION (`arrs diff DS --from <ref>`): compares two versions of one
    ///   Lance dataset (row, schema, fragment, index and version-log deltas).
    ///   Selected by giving a single dataset plus at least one of --from or
    ///   --from-tag.
    ///
    /// Exit codes follow diff(1): 0 when the two sides are identical, 1 when
    /// they differ, 2 on error.
    ///
    /// Human-readable summary by default; pass --format jsonl for a single
    /// machine-readable JSON record. In dataset-vs-dataset mode,
    /// --columns/--exclude-columns scope the comparison to the projected
    /// columns.
    Diff {
        /// First dataset. In version mode, the single dataset to diff across
        /// versions.
        input: String,
        /// Second dataset. Its presence selects dataset-vs-dataset mode, in
        /// which Lance version selectors are rejected.
        other: Option<String>,
        /// (Version mode) Left-hand ("from") version number.
        #[arg(long, conflicts_with = "from_tag")]
        from: Option<u64>,
        /// (Version mode) Left-hand ("from") tag; resolves to its `(branch, version)`.
        #[arg(long = "from-tag", conflicts_with = "from")]
        from_tag: Option<String>,
        /// (Version mode) Right-hand ("to") version number. Defaults to the
        /// latest version of the same branch as the "from" endpoint.
        #[arg(long, conflicts_with = "to_tag")]
        to: Option<u64>,
        /// (Version mode) Right-hand ("to") tag; resolves to its `(branch, version)`.
        #[arg(long = "to-tag", conflicts_with = "to")]
        to_tag: Option<String>,
        /// (Version mode) Scope both endpoints to this branch (default: main).
        /// A tag on a different branch is rejected.
        #[arg(long)]
        branch: Option<String>,
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

    /// (Setup) Generate a shell completion script and print it to stdout.
    ///
    /// Takes no dataset input; writes the script for the given shell and exits.
    /// See the README install section for where to install each shell's script,
    /// e.g. `arrs completions fish > ~/.config/fish/completions/arrs.fish`.
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, powershell, elvish).
        #[arg(value_enum)]
        shell: Shell,
    },
}
