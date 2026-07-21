# arrs

A small command-line tool for inspecting Arrow-based datasets. Today it reads
[Lance](https://lance.org/) datasets; the core is format-agnostic
so other Arrow-backed formats can be added without touching commands or output.

## Features

- Stream or random-access any Lance dataset from the shell.
- Print rows as **JSONL**, **CSV**, or a
  pretty **table**.
- Filter rows by content with a SQL-style `--where` predicate.
- Project columns with `--columns` / `--exclude-columns`.
- Choose how binary payloads are rendered: hidden behind a placeholder, hex
  (`\xHH`), or base64.
- ISO-8601 timestamps, `NaN`/`Infinity` handled, nested lists & structs
  preserved in JSONL.

## Install

### Via `uv`

```sh
uv tool install rust-arrs
```

### From the repository

```sh
# From a clone of this repo:
cargo install --path .

# Or run directly from the checkout:
cargo run --release -- <command> [args…]
```

## Commands

| Command    | What it does                                                        |
|------------|---------------------------------------------------------------------|
| `cat`      | Concatenate one or more datasets and print every row.               |
| `head`     | Print the first `N` rows (default `10`).                            |
| `tail`     | Print the last `N` rows (default `10`).                             |
| `take`     | Print specific rows by index (see grammar below).                   |
| `rowcount` | Print the number of rows.                                           |
| `sample`   | Print `N` random rows, no replacement. `--seed` for reproducibility.|
| `schema`   | Print the logical (Arrow) or physical (Lance-native) schema.        |
| `versions` | (Lance) List versions of the dataset.                               |
| `branches` | (Lance) List branches of the dataset.                               |
| `tags`     | (Lance) List tags across every branch.                              |
| `indices`  | (Lance) List indices defined on the dataset.                        |
| `fragments` | (Lance) List fragments with row, deletion, file, and size info.    |

## Global flags

| Flag                        | Default | Purpose                                                     |
|-----------------------------|---------|-------------------------------------------------------------|
| `--format <csv\|jsonl\|table>` | per-cmd | Output format. Defaults to `table` for `versions`/`branches`/`tags`/`indices`/`fragments`, `jsonl` everywhere else. |
| `--binary-format <...>`     | `none`  | `none` → `BINARY_DATA` placeholder; `hex` → `\xHH`; `base64`.|
| `--columns <a,b,…>`         | –       | Comma-separated include list. User order is preserved.      |
| `--exclude-columns <a,b,…>` | –       | Comma-separated exclude list. Takes precedence over `--columns`.|
| `--where <predicate>`       | –       | Keep only rows matching a SQL-style predicate. Supported by `cat`, `head`, `tail`, `rowcount`, `sample`. See below. |

## Examples

```sh
# How many rows?
arrs rowcount dataset.lance

# Peek at the first 5 rows as JSONL (default).
arrs head -n 5 dataset.lance

# Last 3 rows, CSV, dropping a noisy column.
arrs tail -n 3 --format csv --exclude-columns raw_tokens dataset.lance

# Specific rows by index: last row, row 0, rows 2 through 4.
arrs take --indices '-1,0,2:4' dataset.lance

# Reproducible random sample of 100 rows.
arrs sample -n 100 --seed 42 dataset.lance

# Concatenate two partitions (must share the same schema) and keep two columns.
arrs cat --columns id,score part_a.lance part_b.lance

# Inspect schemas.
arrs schema dataset.lance                 # arrow (logical)
arrs schema --type physical dataset.lance # lance-native (field ids, encoding…)
```

### Filtering rows with `--where`

`--where` takes a SQL-style predicate (parsed by the backend — DataFusion SQL
for Lance) and keeps only the rows that match. It is available on `cat`,
`head`, `tail`, `sample`, and `rowcount` (but not `take`, which addresses rows
positionally — see below):

| Command    | With `--where`                                                   |
|------------|------------------------------------------------------------------|
| `cat`      | Print every matching row.                                        |
| `head`     | Print the first `N` *matching* rows.                             |
| `tail`     | Print the last `N` *matching* rows.                              |
| `rowcount` | Count matching rows (pushed into scalar indices when available). |
| `sample`   | Randomly sample `N` of the *matching* rows.                      |

The filter is applied **before** row selection, so `head`/`tail`/`sample`
operate on the matching rows rather than filtering a positional slice.

```sh
# First 20 rows where two conditions hold.
arrs head -n 20 --where "score > 0.5 AND name LIKE 'a%'" dataset.lance

# Count matching rows without a full scan when the column is indexed.
arrs rowcount --where "label = 'spam'" dataset.lance

# Filter and project together.
arrs cat --where "created_at >= TIMESTAMP '2026-01-01'" --columns id,score dataset.lance

# Random sample drawn only from the test split.
arrs sample -n 100 --where "split = 'test'" dataset.lance
```

`take --indices` addresses rows *positionally*, so combining it with `--where`
is ambiguous and is rejected with a clear error — filter with `head`/`cat`
instead. Invalid predicates surface the backend's parse error as
`invalid --where predicate: …`.

### Lance versioning, branches and tags

Lance datasets carry a per-branch linear version history; tags are named
references to specific `(branch, version)` pairs. Three flags select which
state to read from:

| Flag              | Meaning                                                          |
|-------------------|------------------------------------------------------------------|
| `--branch <name>` | Read from the named branch (default: `main`).                    |
| `--version <N>`   | Read version `N` on the chosen branch. (default: latest version) |
| `--tag <name>`    | Read the tagged `(branch, version)`                              |

`--version` and `--tag` are mutually exclusive. 

```sh
# Inspect a previous snapshot.
arrs head -n 5 --version 3 dataset.lance
arrs rowcount --tag release-2026-04 dataset.lance
arrs cat --branch dev --columns id,score dataset.lance

# List metadata.
arrs versions dataset.lance                       # every version on main
arrs versions --tagged-only dataset.lance         # only tagged versions
arrs versions --branch dev dataset.lance          # every version on `dev`
arrs branches dataset.lance
arrs tags dataset.lance                           # cross-branch tag listing
arrs indices dataset.lance
```

### Fragments

Fragments are the physical unit of a Lance dataset. `arrs fragments` lists one
row per fragment — physical rows, tombstoned (deleted) rows, data file count and
on-disk size — plus a summary line of totals in table mode. Row/deletion/file
info comes from the manifest, so it stays fast; sizes come from the manifest
where cached and otherwise from concurrent object-store lookups. It honours the
`--branch`/`--version`/`--tag` selectors since fragments are per-version.

```sh
arrs fragments dataset.lance                       # fragments of main's latest version
arrs fragments --version 3 dataset.lance           # fragments as of version 3
arrs fragments --branch dev dataset.lance          # fragments on `dev`
arrs fragments --verbose dataset.lance             # include data file paths in the table
arrs fragments --no-size dataset.lance             # skip size lookups (fast/remote datasets)
arrs fragments --format jsonl dataset.lance        # machine-readable, raw byte sizes
```

### Binary columns

Binary payloads can blow up output size and clutter a terminal, so by default
they are collapsed to a placeholder:

```sh
$ arrs head -n 1 dataset.lance
{"id":1,"data":"BINARY_DATA",…}

$ arrs head -n 1 --binary-format hex dataset.lance
{"id":1,"data":"\\x48\\x65\\x6c\\x6c\\x6f",…}

$ arrs head -n 1 --binary-format base64 dataset.lance
{"id":1,"data":"SGVsbG8=",…}
```

The placeholder semantics apply recursively: binary nested inside a struct or
list is also rendered as `BINARY_DATA` under the default.

### `--indices` grammar

`take --indices` accepts a comma-separated list of expressions. Order is
preserved and duplicates are emitted as-is.

| Expression  | Meaning                                           |
|-------------|---------------------------------------------------|
| `N`         | single row (negatives count from the end)         |
| `A:B`       | inclusive range, both ends                        |
| `:B`        | rows 0 through B                                  |
| `A:`        | row A through the last row                        |
| `-5:-1`     | last 5 rows                                       |
| `3,1,1,0:2` | `[3, 1, 1, 0, 1, 2]`                              |

## Output format notes

**JSONL**
- One JSON object per line; keys match the projected column order.
- `NaN` / `±Infinity` emit the strings `"NaN"` / `"Infinity"` / `"-Infinity"`.
- Timestamps are ISO-8601 (`2024-01-01T00:00:00.000000`, with offset when the
  arrow type carries a timezone).
- Lists → JSON arrays; structs → JSON objects; maps → JSON objects with
  stringified keys.

**CSV**
- First line is a header row: `col1,col2,col3`. Column names
  containing `,`, newlines, or quotes are quoted per RFC 4180.
- Nulls emit as empty cells; `NaN` / `inf` / `-inf` for floats.
- Nested types (list, struct, map, duration, interval) are rejected, use
  JSONL for those.

**Table**
- Pretty Unicode borders on a TTY, ASCII grid when piped (so
  `arrs … | grep` stays sane).
- Same primitive rendering as CSV (ISO-8601, `NaN`/`inf`, empty for null).
- Nested cells (list, struct, map) are JSON-encoded inside the cell —
  e.g. `["id"]` for a single-element list. Strictly more permissive than CSV.
- **Buffers all rows** before emitting (column widths require the full table).
  Default for the four metadata commands (small row counts), opt-in for
  row-producing commands; prefer `jsonl`/`csv` when streaming large datasets.
