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
| `freq`     | Value counts for a column: each distinct value, its count and percent.|
| `stat`     | (Lance) One-screen dataset health summary (metadata-only).          |
| `schema`   | Print the logical (Arrow) or physical (Lance-native) schema.        |
| `versions` | (Lance) List versions of the dataset.                               |
| `branches` | (Lance) List branches of the dataset.                               |
| `tags`     | (Lance) List tags across every branch.                              |
| `indices`  | (Lance) List indices defined on the dataset (name, type, columns, …).|
| `index-stats` | (Lance) Per-index coverage: indexed vs unindexed row counts.     |
| `fragments` | (Lance) List fragments with row, deletion, file, and size info.    |
| `search`   | (Lance) Nearest-neighbor vector search; appends a `_distance` column.|
| `diff`     | (Lance) Diff two versions of one dataset (rows, schema, fragments, indices). |

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
| `--format <csv\|jsonl\|json\|table>` | per-cmd | Output format. Defaults to `table` for `versions`/`branches`/`tags`/`indices`/`index-stats`/`fragments`/`stats`/`freq`/`stat`, `jsonl` everywhere else. `json` emits a single streamed JSON array. |
| `--binary-format <...>`     | `none`  | `none` → `BINARY_DATA` placeholder; `hex` → `\xHH`; `base64`.|
| `--max-list-items <N>`      | –       | Truncate lists / large-lists / fixed-size-lists to the first `N` elements, appending a `… (K more)` marker element (per nesting level). `jsonl`/`json` and nested table cells. Lossy. |
| `--max-cell-width <N>`      | –       | `table` only: truncate each rendered data cell to at most `N` characters, ending in `…` (header cells are exempt). Counts characters, never splits a UTF-8 codepoint. Lossy. |
| `--float-precision <N>`     | –       | Render `f16`/`f32`/`f64` with exactly `N` fractional digits in every format (`NaN`/`Infinity` unaffected). Lossy. |
| `--columns <a,b,…>`         | –       | Comma-separated include list. Supports glob patterns and nested paths (see below). User order is preserved. |
| `--exclude-columns <a,b,…>` | –       | Comma-separated exclude list. Supports the same patterns/paths. Takes precedence over `--columns`.|
| `--where <predicate>`       | –       | Keep only rows matching a SQL-style predicate. Supported by `cat`, `head`, `tail`, `rowcount`, `sample`, `stats`, `freq`. See below. |

The output-control flags (`--max-list-items`, `--max-cell-width`,
`--float-precision`) affect **rendering only**. Truncated or rounded output is
lossy and is not meant for round-tripping — in particular it is not a
CSV-parse-ability guarantee, and nested types remain rejected by CSV regardless.
List truncation appends a literal string element `… (K more)` (e.g.
`… (1532 more)`), which keeps `jsonl`/`json` arrays valid JSON.

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

# Value counts for a column (class balance, enum sanity checks).
arrs freq --column label dataset.lance

# Concatenate two partitions (must share the same schema) and keep two columns.
arrs cat --columns id,score part_a.lance part_b.lance

# Tame wide embedding columns: show only the first 4 elements of each list.
arrs head --max-list-items 4 dataset.lance
# {"id":1,"emb":[0.12,0.98,0.33,0.41,"… (1532 more)"]}

# Keep a table readable: cap every cell at 40 characters.
arrs head --format table --max-cell-width 40 dataset.lance

# Round floats to 3 fractional digits (works in every format).
arrs head --float-precision 3 dataset.lance

# Emit one streamed JSON array (for tools that can't read JSONL) and pipe to jq.
arrs cat --format json dataset.lance | jq '.[].id'

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

### Projecting columns with `--columns` / `--exclude-columns`

Both flags accept three kinds of token, resolved against the dataset schema:

```sh
# Exact top-level names (user order preserved).
arrs cat --columns id,score dataset.lance

# Glob patterns: '*' (any run of characters) and '?' (exactly one), matched
# against top-level column names. Matches expand in schema order at the
# position the pattern occupies.
arrs head --columns 'emb_*' dataset.lance
arrs cat  --exclude-columns 'raw_*,debug_*' dataset.lance

# Nested field paths: dot-separated access into struct columns.
arrs head --columns meta.user.id,id dataset.lance
```

Rules:

- **Globs** match top-level names only. A pattern that matches nothing is an
  error (like an unknown exact name). A column matched by both a glob and an
  explicit name appears once, at its first position.
- **Nested paths** are validated against the Arrow schema, so `meta.nope`
  (unknown field) and `score.x` (traversal into a non-struct) fail with a clear
  message instead of a backend panic.
- **Exact-match-first (escaping):** a token that exactly matches a real
  top-level column always selects that literal column — this is how you pick a
  column literally named `a*b` or `meta.user`, and it wins over glob and path
  interpretation.

**Nested output shape (JSONL).** A projected nested leaf is emitted as a single
**flat column named by its full dotted path** — this is what Lance's scanner
returns natively. For example `--columns meta.user.id,id` yields:

```json
{"meta.user.id": 10, "id": 1}
```

The same flat shape is produced by `head`, `cat`, `take`, `sample`, `tail` and
by `schema --type arrow` (which lists a `meta.user.id` field). Projecting a
whole struct column instead (e.g. `--columns meta`) keeps it nested:
`{"meta": {"user": {"id": 10, "name": "alice"}, "source": "web"}}`.
`schema --type physical` shows the Lance-native pruned field tree rather than
the flat view. Naming both a struct and a leaf inside it (e.g.
`--columns meta,meta.user.id`) emits the whole struct *and* a duplicate flat
`meta.user.id` column, and `head`/`cat`/`take` agree on that shape.

**Excluding a nested path** prunes that leaf (or subtree) and emits the struct's
surviving leaves as flat dotted columns: `--exclude-columns meta.user.id` yields
`meta.user.name` and `meta.source` (the rest of `meta`), while untouched struct
columns stay whole.

`--where` is applied *before* projection, so you can filter on a column you
project away (e.g. `--columns id --where 'score > 1.5'`).

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
| `freq`     | Value counts over the *matching* subset only.                    |

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

### Value counts with `freq`

`freq --column <name>` answers "what values does this column take, and how
often?" — class balance of a label, enum sanity checks, spotting typo'd
categories. It emits one row per distinct value with its `count` and `percent`
(of all scanned rows), defaulting to the `table` format:

```sh
$ arrs freq --column label dataset.lance
+-------+-------+---------+
| value | count | percent |
+-------+-------+---------+
| spam  | 91234 | 45.6%   |
| ham   | 88765 | 44.4%   |
| NULL  | 20001 | 10.0%   |
+-------+-------+---------+
```

- Nulls are counted and shown as an explicit `NULL` row.
- `-n/--limit N` (`N` ≥ 1) keeps only the top `N` rows; everything else is folded
  into a trailing `<other>` row so the percentages still add up.
- `--sort count` (default) orders by frequency, breaking ties by value; `--sort
  value` orders by value. Ordering is by the value's real type — numbers sort
  numerically (not as strings), temporals chronologically — with `NULL` and
  `NaN` always last. Distinct values that compare equal (e.g. `-0.0` and `0.0`)
  fall back to a string tie-break so the output stays deterministic.
- Only primitive columns are supported (strings, numbers, bools, dates,
  timestamps, decimals); nested and binary columns are rejected with a clear
  error.
- Works with `--where` (counts are computed over the matching subset) and with
  `--format jsonl|csv` for machine-readable output.

A note on how values are keyed: each value is identified by its CSV rendering
(the same text `cat --format csv` would print). Two consequences worth knowing:

- Floating-point `-0.0` and `0.0` render differently (`-0` vs `0`) and so are
  two distinct rows, while every `NaN` bit pattern renders as `NaN` and collapses
  into a single row.
- A literal string value of `"NULL"` (or `"<other>"`) renders identically to the
  real null row (or the truncation remainder). The counts are always tracked
  separately and correctly — only the printed label collides — but the rendered
  table cannot tell them apart. This holds in every format (`jsonl` also prints
  the label as the string `"NULL"`, not JSON `null`), so if you need to
  distinguish them unambiguously, filter the literal out with `--where` first.

```sh
# Top 20 values only, remainder summarized as `<other>`.
arrs freq --column url -n 20 dataset.lance

# Alphabetical, as CSV.
arrs freq --column country --sort value --format csv dataset.lance

# Class balance within the test split.
arrs freq --column label --where "split = 'test'" dataset.lance
```

Memory use is proportional to the column's cardinality; past ~1M distinct
values `freq` bails with a helpful error rather than eating RAM on a
high-cardinality column (an id or UUID).

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

### Dataset health with `stat`

`arrs stat` answers "how is this dataset doing?" on one screen. Where `stats`
(plural) scans the data for a per-column `df.describe()`, `stat` (singular, after
`stat(1)`) reads only Lance manifest metadata — so it stays instant no matter how
large the dataset is and never touches the data files (beyond summing their
sizes). All the underlying lookups (fragments, versions, branches, tags, indices)
run concurrently. It honours the `--branch`/`--version`/`--tag` selectors: the
row/fragment/size/index figures reflect the checked-out version, and the version
count is scoped to the selected branch.

```
$ arrs stat sample.lance
+--------------+---------------------------------------+
| metric       | value                                 |
+======================================================+
| path         | sample.lance                          |
| format       | lance (manifest version 4)            |
| rows         | 9                                     |
| deleted rows | 0  (0.0%)                             |
| columns      | 7                                     |
| fragments    | 3  (min 3 rows, max 3 rows, median 3) |
| data size    | 6.2 KiB                               |
| versions     | 4  (latest 2026-07-21T22:53:12Z)      |
| branches     | 2                                     |
| tags         | 2                                     |
| indices      | 1  (idx_id BTree)                     |
+--------------+---------------------------------------+
```

When the fragment spread or deleted-row ratio suggests it, a conservative note is
appended in table mode, e.g. `note: many small fragments; compaction would likely
help`. The thresholds are deliberately cautious: a median fragment under 100k rows
with 100+ fragments, and/or a deleted-row ratio of 10% or more.

```sh
arrs stat dataset.lance                    # health summary of main's latest version
arrs stat --branch dev dataset.lance       # summary of the dev branch tip
arrs stat --version 3 dataset.lance        # summary as of version 3
arrs stat --no-size dataset.lance          # skip size lookups (fast/remote datasets)
arrs stat --format jsonl dataset.lance     # one stable-schema JSON object (raw numbers)
```

`--format jsonl` emits a single object with raw numeric values (no human
formatting) and a **stable field set** for scripting:

| Field                      | Type          | Meaning                                                              |
|----------------------------|---------------|----------------------------------------------------------------------|
| `path`                     | string        | Input path/URI the dataset was opened from.                          |
| `format`                   | string        | Always `"lance"`.                                                     |
| `manifest_version`         | number        | Checked-out manifest (dataset) version.                              |
| `rows`                     | number        | Live rows (`physical_rows` − `deleted_rows`).                        |
| `physical_rows`            | number        | Rows physically stored, ignoring deletions.                          |
| `deleted_rows`             | number        | Tombstoned (soft-deleted) rows.                                      |
| `deleted_ratio`            | number        | `deleted_rows / physical_rows` in `0.0..=1.0` (`0` when empty).      |
| `columns`                  | number        | Column count of the (Arrow) schema.                                 |
| `fragments`                | number        | Fragment count.                                                     |
| `fragment_min_rows`        | number\|null  | Smallest fragment's physical rows (`null` with no fragments).        |
| `fragment_max_rows`        | number\|null  | Largest fragment's physical rows (`null` with no fragments).         |
| `fragment_median_rows`     | number\|null  | Median physical rows/fragment, rounded up on ties (`null` if empty). |
| `data_size_bytes`          | number\|null  | Summed data-file bytes; `null` under `--no-size`.                    |
| `versions`                 | number        | Version count on the selected branch.                               |
| `latest_version_timestamp` | string\|null  | RFC 3339 UTC timestamp of the latest version (`null` if none).       |
| `branches`                 | number        | Branch count (dataset-wide).                                        |
| `tags`                     | number        | Tag count (dataset-wide).                                           |
| `num_indices`              | number        | Index count on the checked-out version.                             |
| `indices`                  | array         | `{ "name", "type" }` objects, one per index.                        |
| `compaction_hint`          | string\|null  | The advisory note (as above), or `null` when nothing stands out.     |

`--format csv` mirrors the table's human-readable `metric,value` pairs (use
`jsonl` when you want the raw numbers).

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

### Diffing two versions with `diff`

`arrs diff` compares two versions of the *same* dataset and reports what changed
between them — answering "what happened between version 3 and version 7?".
Because Lance's manifests carry fragment, schema and index metadata, almost all
of it is derived without scanning data.

```sh
arrs diff --from 3 --to 7 dataset.lance
arrs diff --from-tag release-1 --to-tag release-2 dataset.lance
arrs diff --from 3 dataset.lance                  # --to defaults to branch latest
arrs diff --branch dev --from 2 --to 5 dataset.lance
```

| Flag                 | Meaning                                                              |
|----------------------|----------------------------------------------------------------------|
| `--from <N>`         | Left-hand version number (required unless `--from-tag`).             |
| `--from-tag <name>`  | Left-hand endpoint by tag; resolves to its `(branch, version)`.      |
| `--to <N>`           | Right-hand version number. Defaults to the latest version of the same branch as `--from`. |
| `--to-tag <name>`    | Right-hand endpoint by tag.                                          |
| `--branch <name>`    | Scope both endpoints to this branch (default: `main`).              |

It reports:

- **Row delta** — live row count at each version and the net change, split into
  rows added vs deleted from fragment metadata (appended fragments and
  un-tombstoned rows vs removed fragments and new tombstones) rather than only
  the net. The split is metadata-truthful, not a logical diff: because Lance
  rewrites whole fragments, a compaction that rewrites `N` unchanged rows counts
  them as `+N` added and `−N` deleted (net 0), and a version `restore` shows the
  rows it brings back as added.
- **Schema changes** — columns added, removed, or retyped (a nullability change
  counts as a retype, shown as `Int32` → `Int32?`).
- **Fragment changes** — fragments added, removed, or *rewritten*. Lance never
  reuses fragment ids, so compaction appears as removed + added; the *rewritten*
  bucket is reserved for a surviving fragment whose set of data files changed
  (e.g. a column added via schema evolution appends a new file to existing
  fragments). Tombstone-only changes are reflected in the row delta, not here.
- **Index changes** — indices created or dropped between the two versions.
- **Version log** — the versions in the `(from, to]` range with timestamps and
  commit messages.

Output is a human-readable summary by default; `--format jsonl` emits a single
machine-readable JSON record for scripting (the only non-default format
accepted — `csv`/`table` are rejected).

Exit codes follow `diff(1)` for CI use:

| Code | Meaning                          |
|------|----------------------------------|
| `0`  | the two versions are identical   |
| `1`  | the two versions differ          |
| `2`  | error (bad usage, missing dataset, cross-branch comparison, …) |

> **Note:** exit code `2` (rather than `1`) is used for *all* command errors
> across `arrs`, so that exit code `1` unambiguously means "`diff`: the versions
> differ" and is never confused with a failure.

```sh
# Machine-readable diff for a CI gate.
arrs diff --from 3 --to 7 --format jsonl dataset.lance

# Compare two tagged releases and act on the exit code.
if arrs diff --from-tag v1 --to-tag v2 dataset.lance; then
  echo "no changes"
fi
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

**JSON** (`--format json`)
- A single well-formed JSON array of the same objects `jsonl` would emit:
  `[{…},{…},…]`. Empty input yields `[]`.
- Streamed with constant memory (one object materialized at a time), so it is
  safe on large datasets — just not line-oriented like `jsonl`.
- Parses directly with `jq .`.

**CSV**
- First line is a header row: `col1,col2,col3`. Column names
  containing `,`, newlines, or quotes are quoted per RFC 4180.
- Nulls emit as empty cells; `NaN` / `inf` / `-inf` for floats.
- Nested types (list, struct, map, duration, interval) are rejected, use
  JSONL for those. `--max-list-items` does not change this — CSV rejects nested
  columns regardless.

**Output-control flags** (`--max-list-items`, `--max-cell-width`,
`--float-precision`)
- Purely cosmetic and **lossy**: they change how values are rendered, never the
  data. Do not use truncated/rounded output for round-tripping.
- `--max-list-items N`: after `N` elements, a list gets a trailing string
  element `… (K more)` where `K` is how many were dropped. Applied at every
  nesting level independently, and to `FixedSizeList` embedding columns. Because
  the marker is a JSON string, `jsonl`/`json` arrays stay valid JSON.
- `--max-cell-width N` (table only): each rendered data cell is cut to at most
  `N` characters, ending in `…` (the `…` counts toward `N`, so `N = 0` collapses
  a non-empty cell to a bare `…`). Header cells (column names) are never
  truncated. Character-based (never splits a multi-byte codepoint); CJK/
  full-width display columns are counted as one each.
- `--float-precision N`: `f16`/`f32`/`f64` render with exactly `N` fractional
  digits (`format!`-style round-half-to-even); `NaN`/`Infinity` are untouched.
  In `stats`, this applies to the numeric `mean`/`stddev` columns (`min`/`max`
  are pre-rendered strings and unaffected).

**Table**
- Pretty Unicode borders on a TTY, ASCII grid when piped (so
  `arrs … | grep` stays sane).
- Same primitive rendering as CSV (ISO-8601, `NaN`/`inf`, empty for null).
- Nested cells (list, struct, map) are JSON-encoded inside the cell —
  e.g. `["id"]` for a single-element list. Strictly more permissive than CSV.
- **Buffers all rows** before emitting (column widths require the full table).
  Default for `freq` and the four metadata commands (small row counts), opt-in
  for row-producing commands; prefer `jsonl`/`csv` when streaming large datasets.
