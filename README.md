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
| `stats`    | Per-column summary statistics (a `df.describe()` for datasets).     |
| `schema`   | Print the logical (Arrow) or physical (Lance-native) schema.        |
| `versions` | (Lance) List versions of the dataset.                               |
| `branches` | (Lance) List branches of the dataset.                               |
| `tags`     | (Lance) List tags across every branch.                              |
| `indices`  | (Lance) List indices defined on the dataset (name, type, columns, …).|
| `index-stats` | (Lance) Per-index coverage: indexed vs unindexed row counts.     |
| `fragments` | (Lance) List fragments with row, deletion, file, and size info.    |
| `search`   | (Lance) Nearest-neighbor vector search; appends a `_distance` column.|

## Remote datasets

Every command accepts an object-store URI wherever it accepts a local path, so
you can point `arrs` straight at a bucket:

```sh
arrs head -n 5 s3://my-bucket/datasets/embeddings.lance
arrs rowcount gs://analytics/events.lance
arrs schema az://container/data.lance
arrs versions s3://my-bucket/datasets/embeddings.lance
```

| Scheme     | Backend                     | Credentials (ambient environment)                                    |
|------------|-----------------------------|----------------------------------------------------------------------|
| `s3://`    | AWS S3 (and S3-compatible)  | Standard AWS SDK chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION`, instance/role metadata. |
| `gs://`    | Google Cloud Storage        | `GOOGLE_APPLICATION_CREDENTIALS` (service-account JSON), or `gcloud` application-default credentials. |
| `az://`    | Azure Blob Storage          | `AZURE_STORAGE_ACCOUNT_NAME` plus `AZURE_STORAGE_ACCOUNT_KEY` / `AZURE_STORAGE_SAS_TOKEN` (and other standard Azure env vars). |
| `file://`  | Local filesystem            | none.                                                                 |
| *(none)*   | Local filesystem            | none.                                                                 |

Credentials are read exclusively from the ambient environment — there are no
`arrs`-specific credential flags. A bare path (relative or absolute), with or
without a `file://` prefix, always resolves to the local filesystem exactly as
before. Errors from the object store (missing credentials, 404, permission
denied) are surfaced with the offending URI and the underlying cause.

> **Note:** `file://` URIs must be absolute (`file:///abs/path.lance`); a
> relative `file://path` resolves confusingly against the current directory —
> use a bare relative path instead.

> **Note:** a `gs://` URI with no ambient credentials (neither
> `GOOGLE_APPLICATION_CREDENTIALS` nor application-default credentials) can
> stall ~90–100s while the underlying `object_store` probes the GCE metadata
> server before erroring — this is upstream behavior, not arrs. (`s3://` errors
> within seconds; `az://` errors instantly.)

## Global flags

| Flag                        | Default | Purpose                                                     |
|-----------------------------|---------|-------------------------------------------------------------|
| `--format <csv\|jsonl\|table>` | per-cmd | Output format. Defaults to `table` for `versions`/`branches`/`tags`/`indices`/`index-stats`/`fragments`/`stats`, `jsonl` everywhere else. |
| `--binary-format <...>`     | `none`  | `none` → `BINARY_DATA` placeholder; `hex` → `\xHH`; `base64`.|
| `--columns <a,b,…>`         | –       | Comma-separated include list. User order is preserved.      |
| `--exclude-columns <a,b,…>` | –       | Comma-separated exclude list. Takes precedence over `--columns`.|
| `--where <predicate>`       | –       | Keep only rows matching a SQL-style predicate. Supported by `cat`, `head`, `tail`, `rowcount`, `sample`, `stats`. See below. |

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

# Per-column summary statistics (like df.describe()).
arrs stats dataset.lance
arrs stats --columns score,label dataset.lance    # only these columns
arrs stats --where "split = 'test'" dataset.lance # over a filtered subset
arrs stats --format jsonl dataset.lance           # machine-readable
```

### Per-column statistics with `stats`

`arrs stats` streams the dataset once and prints one row per column, with memory
independent of the row count. It is the shell equivalent of `df.describe()`:

| Field       | Meaning                                                             |
|-------------|--------------------------------------------------------------------|
| `column`    | Column name.                                                       |
| `type`      | Arrow data type.                                                   |
| `count`     | Number of non-null values.                                         |
| `nulls`     | Number of null values (`count + nulls` = rows considered).         |
| `min`/`max` | Numeric, temporal, string, and boolean columns (strings compare lexicographically). |
| `mean`/`stddev` | Numeric columns only. `stddev` is the sample standard deviation (ddof = 1). |
| `distinct`  | Distinct-value count, exact up to a cap (10 000) then reported as `>10000`. |

Nested, binary, decimal, and dictionary columns report only `count`/`nulls`
(never an error). A column containing any `NaN` reports `NaN` for `mean`/`stddev`,
while `min`/`max` ignore `NaN` and report the real numeric range. `stats`
respects `--columns`/`--exclude-columns` and `--where`, and honours `--format`
(`table` by default, plus `jsonl`/`csv`).

### Filtering rows with `--where`

`--where` takes a SQL-style predicate (parsed by the backend — DataFusion SQL
for Lance) and keeps only the rows that match. It is available on `cat`,
`head`, `tail`, `sample`, `rowcount`, and `stats` (but not `take`, which
addresses rows positionally — see below):

| Command    | With `--where`                                                   |
|------------|------------------------------------------------------------------|
| `cat`      | Print every matching row.                                        |
| `head`     | Print the first `N` *matching* rows.                             |
| `tail`     | Print the last `N` *matching* rows.                              |
| `rowcount` | Count matching rows (pushed into scalar indices when available). |
| `sample`   | Randomly sample `N` of the *matching* rows.                      |
| `stats`    | Compute statistics over only the *matching* rows.                |

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
references to specific `(branch, version)` pairs. Four flags select which
state to read from:

| Flag                | Meaning                                                                       |
|---------------------|-------------------------------------------------------------------------------|
| `--branch <name>`   | Read from the named branch (default: `main`).                                 |
| `--version <N>`     | Read version `N` on the chosen branch. (default: latest version)              |
| `--tag <name>`      | Read the tagged `(branch, version)`                                           |
| `--as-of <instant>` | Read the latest version whose commit timestamp is at or before `<instant>`.   |

`--version`, `--tag`, and `--as-of` all name a single version and are therefore
mutually exclusive; each combines with `--branch`.

`--as-of` accepts three timestamp formats:

- RFC 3339 with an offset — `2026-07-01T12:00:00Z`, `2026-07-01T14:00:00+02:00`.
- A naive datetime with no offset — `2026-07-01T12:00:00` — **interpreted as
  UTC**, never local time, so results are reproducible on any machine.
- A date with no time — `2026-07-01` — interpreted as **midnight UTC**.

The resolved version is echoed on stderr
(`resolved --as-of to version 7 (2026-07-01T11:48:02Z)`) so results stay
reproducible. If the instant predates the branch's first version, the error
reports the earliest valid timestamp. On a non-default branch the timeline
starts at the branch-creation version, so an instant before that (even if it
falls after the parent commit the branch forked from) is out of range for that
branch.

```sh
# Inspect a previous snapshot.
arrs head -n 5 --version 3 dataset.lance
arrs rowcount --tag release-2026-04 dataset.lance
arrs cat --branch dev --columns id,score dataset.lance

# Time travel by timestamp: "what did this look like then?"
arrs head -n 5 --as-of "2026-07-01T12:00:00Z" dataset.lance
arrs rowcount --as-of 2026-07-01 dataset.lance          # date-only → midnight UTC
arrs schema --branch dev --as-of "2026-06-15T09:30" dataset.lance

# List metadata.
arrs versions dataset.lance                       # every version on main
arrs versions --tagged-only dataset.lance         # only tagged versions
arrs versions --branch dev dataset.lance          # every version on `dev`
arrs branches dataset.lance
arrs tags dataset.lance                           # cross-branch tag listing
arrs indices dataset.lance                        # name, type, columns, version, …
arrs index-stats dataset.lance                    # per-index coverage
```

### Indices and coverage

`indices` lists every index on the active version, including its **type** (the
first thing you usually want — `BTree`, `IVF_PQ`, `INVERTED`, …):

```sh
$ arrs indices dataset.lance
| name    | type   | uuid | columns   | dataset_version | created_at |
| idx_id  | BTree  | …    | ["id"]    | 4               | …          |
```

Lance indices go stale as rows are appended after the index was built, so
`index-stats` reports how many rows are actually covered:

```sh
$ arrs index-stats dataset.lance
| name    | type   | indexed_rows | unindexed_rows | coverage |
| idx_id  | BTree  | 980000       | 20000          | 98.0%    |
```

`coverage` is `indexed_rows / (indexed_rows + unindexed_rows)`, shown as a
percentage (`n/a` for an empty index). In `--format jsonl` an extra `detail`
column carries Lance's raw statistics JSON verbatim, so type-specific internals
(IVF partition counts, PQ sub-vectors, per-delta row counts, …) pass straight
through without arrs needing to understand every field:

```sh
$ arrs --format jsonl index-stats dataset.lance
{"name":"idx_id","type":"BTree","indexed_rows":980000,"unindexed_rows":20000,"coverage":"98.0%","detail":"{\"index_type\":\"BTree\",\"num_indexed_rows\":980000,…}"}
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

### Vector search

`arrs search` runs a nearest-neighbor query against a vector column (an Arrow
`FixedSizeList` of `f16`/`f32`/`f64`) and returns the `k` closest rows ordered
by distance. A `_distance` column is appended to the output, so every format
(`jsonl`, `csv`, `table`) works unchanged. When the column has an ANN index
(e.g. `IVF_PQ`) it is used automatically; otherwise Lance falls back to flat
(brute-force) KNN and a note is printed to stderr.

The query vector is a JSON array of numbers, supplied inline, from a file, or on
stdin. Its length must match the column width, and it is cast to the column's
element type for you.

| Flag                   | Meaning                                                          |
|------------------------|------------------------------------------------------------------|
| `--column <name>`      | Vector column to search (required).                              |
| `--vector '[...]'`     | Inline JSON array query vector.                                  |
| `--vector-file <path>` | Read the query vector (JSON array) from a file, or `-` for stdin.|
| `-k <N>`               | Number of neighbors to return (default `10`).                    |
| `--nprobes <N>`        | IVF partitions to probe (index tuning; no effect when unindexed).|
| `--refine-factor <N>`  | Re-rank the top `k * N` candidates for better recall.            |

Exactly one of `--vector` / `--vector-file` is required. The global
`--columns` / `--exclude-columns` projection composes with the search, and
`_distance` is always included.

```sh
# Top-10 nearest rows to an inline query vector.
arrs search --column embedding --vector '[0.1, 0.2, 0.3]' -k 10 ds.lance

# Query vector from a file, or piped in on stdin.
arrs search --column embedding --vector-file query.json -k 10 ds.lance
cat query.json | arrs search --column embedding --vector-file - -k 10 ds.lance

# Tune the ANN search and re-rank the candidates.
arrs search --column embedding --vector-file q.json -k 10 --nprobes 32 --refine-factor 5 ds.lance

# Project only the columns you care about (plus the appended _distance).
arrs search --column embedding --vector-file q.json -k 10 --columns id,title ds.lance
```

> Full-text search (`--query` against inverted/FTS indices, emitting `_score`)
> is planned as a follow-up.

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
