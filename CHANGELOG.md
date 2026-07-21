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

### Changed

- `Dataset::scan` now takes a `ScanOptions` struct (projection + filter) instead
  of a positional projection argument; `Dataset::count_rows` takes an optional
  filter. (#6)
