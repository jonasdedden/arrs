# arrs

A command-line tool for inspecting Arrow-based datasets. It reads
[Lance](https://lance.org/) datasets today; the core is format-agnostic, so
other Arrow-backed formats can be added without changing the commands or the
output layer.

```sh
arrs rowcount dataset.lance
arrs head -n 5 dataset.lance
arrs stats --where "split = 'test'" dataset.lance
arrs search --column embedding --vector-file q.json -k 10 dataset.lance
```

## Install

```sh
uv tool install rust-arrs   # prebuilt binary, from PyPI
cargo install arrs-cli      # from crates.io
```

Building from a clone and installing shell completions are covered in
[doc/install.md](https://github.com/jonasdedden/arrs/blob/main/doc/install.md).

## Commands

Every command takes a local path or an object-store URI (`s3://`, `gs://`,
`az://`, `file://`).

| Command | Description |
|---|---|
| `cat` | Print every row of one or more datasets. |
| `head` / `tail` | First / last `N` rows (default 10). |
| `take` | Rows at given indices, e.g. `--indices '-1,0,2:4'`. |
| `sample` | `N` random rows without replacement; `--seed` for reproducibility. |
| `rowcount` | Number of rows. |
| `schema` | Logical (Arrow) or physical (format-native) schema. |
| `stats` | Per-column summary statistics, the `df.describe()` of a dataset. |
| `freq` | Value counts for one column: each distinct value, its count and percent. |
| `diff` | Compare two datasets, or two versions of one Lance dataset. |
| `blob` | Write one cell's binary payload to a file or stdout. |
| `completions` | Print a shell completion script. |

Lance-only commands:

| Command | Description |
|---|---|
| `stat` | One-screen dataset health summary, from metadata only. |
| `versions` / `branches` / `tags` | List versions, branches, and tags. |
| `indices` | Indices on the dataset: name, type, columns, version. |
| `index-stats` | Indexed vs unindexed row counts per index. |
| `fragments` | Fragments with row, deletion, file, and size information. |
| `search` | Nearest-neighbor vector search; appends a `_distance` column. |

Per-command options and output are documented in
[doc/commands.md](https://github.com/jonasdedden/arrs/blob/main/doc/commands.md).

## Global flags

| Flag | Purpose |
|---|---|
| `--format <csv\|jsonl\|table\|ipc>` | Output format. Defaults to `table` for the summary and metadata commands, `jsonl` elsewhere. `ipc` is a lossless Arrow stream, valid on `cat`/`head`/`tail`/`take`/`sample`. |
| `--columns` / `--exclude-columns` | Comma-separated include/exclude lists. Accept globs and nested paths. |
| `--where <predicate>` | SQL-style row filter, applied before row selection. |
| `--binary-format <none\|hex\|base64>` | How binary values are rendered. Default `none` prints a `BINARY_DATA` placeholder. |
| `--max-list-items <N>` | Truncate lists to the first `N` elements. |
| `--max-cell-width <N>` | Table only: cut each data cell to `N` characters. |
| `--float-precision <N>` | Render floats with exactly `N` fractional digits. |
| `--with-row-id` / `--with-row-addr` | Lance only: append `_rowid` / `_rowaddr`. |
| `--branch` / `--version` / `--tag` / `--as-of` | Lance only: which version to read. |
| `--no-progress` | Turn off the scan progress indicator on stderr. |

`--max-list-items`, `--max-cell-width`, and `--float-precision` are lossy and
affect display only. Combining them with `--format ipc` is an error rather than
a silent no-op.

## Examples

```sh
# Last 3 rows as CSV, without a noisy column.
arrs tail -n 3 --format csv --exclude-columns raw_tokens dataset.lance

# Reproducible random sample.
arrs sample -n 100 --seed 42 dataset.lance

# Class balance of a label column.
arrs freq --column label dataset.lance

# Show only the first 4 elements of each embedding.
arrs head --max-list-items 4 dataset.lance
# {"id":1,"emb":[0.12,0.98,0.33,0.41,"… (1532 more)"]}

# Concatenate partitions matching a glob (quote it, arrs expands it).
arrs cat --columns id,score 'data/part_*.lance'

# Read an older version.
arrs head -n 5 --as-of 2026-07-01 dataset.lance

# Pipe a lossless Arrow stream into DuckDB.
arrs cat --where "score > 0.9" dataset.lance --format ipc \
  | duckdb -c "SELECT count(*) FROM read_arrow('/dev/stdin')"
```

## Documentation

- [Install and shell completions](https://github.com/jonasdedden/arrs/blob/main/doc/install.md)
- [Command reference](https://github.com/jonasdedden/arrs/blob/main/doc/commands.md)
- [Columns and filters](https://github.com/jonasdedden/arrs/blob/main/doc/columns-and-filters.md)
- [Output formats](https://github.com/jonasdedden/arrs/blob/main/doc/output.md)
- [Lance versions, branches, tags, and row identity](https://github.com/jonasdedden/arrs/blob/main/doc/lance.md)
- [Diffing datasets and versions](https://github.com/jonasdedden/arrs/blob/main/doc/diff.md)
- [Remote object stores](https://github.com/jonasdedden/arrs/blob/main/doc/remote-storage.md)
- [Changelog](https://github.com/jonasdedden/arrs/blob/main/CHANGELOG.md)

## License

MIT.
