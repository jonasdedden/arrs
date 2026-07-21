# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `--where <predicate>` SQL-style predicate filtering for `cat`, `head`, `tail`,
  `rowcount`, and `sample`. The filter is applied before row selection, and
  filtered `rowcount` uses the backend's native filtered count (pushed into
  Lance scalar indices when available). (#6)
- `fragments` command (Lance only): lists dataset fragments with physical rows,
  deleted rows, data file count/paths and on-disk size, with a table-mode summary
  line, `--verbose` and `--no-size` flags, and `--branch`/`--version`/`--tag`
  support (#15).
- Support remote object-store URIs (`s3://`, `gs://`, `az://`, `file://`) as
  dataset inputs for every command, using ambient environment credentials (#7).
- `search` command: Lance nearest-neighbor (ANN / flat KNN) vector search with
  `--column`, `--vector`/`--vector-file`, `-k`, `--nprobes`, `--refine-factor`,
  an appended `_distance` column, and projection support (#16).
- `stats` command: per-column summary statistics (a `df.describe()` for
  datasets). Streams the dataset once with memory independent of the row count,
  reporting count, nulls, min/max (numeric, temporal, string, boolean),
  mean/stddev (numeric only, sample stddev), and an approximate distinct count
  (exact up to a cap, then reported as `>10000`). Nested, binary, decimal, and
  dictionary columns report count/nulls only and never error. Respects
  `--columns`/`--exclude-columns`, composes with `--where`, and renders in
  `table`/`jsonl`/`csv`. A default-`None` `Dataset::stats` trait hook leaves the
  door open for backends to answer from metadata instead of scanning. (#8)
- `index-stats` command reporting per-index coverage — `indexed_rows`,
  `unindexed_rows`, and a `coverage` percentage — which diverge as rows are
  appended after an index is built. `--format jsonl` adds a `detail` column
  carrying Lance's raw statistics JSON verbatim so type-specific internals
  (IVF partitions, PQ sub-vectors, …) pass through unmodified. (#17)

### Changed

- `Dataset::scan` now takes a `ScanOptions` struct (projection + filter) instead
  of a positional projection argument; `Dataset::count_rows` takes an optional
  filter. (#6)
- `indices` output now includes an index `type` column (`BTree`, `IVF_PQ`,
  `INVERTED`, …), sourced from Lance's index statistics. (#17)
