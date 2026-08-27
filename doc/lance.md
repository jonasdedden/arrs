# Lance versions, branches, tags, and row identity

## Selecting a version

A Lance dataset carries a per-branch linear version history. Tags are named
references to a `(branch, version)` pair. Four flags choose what to read:

| Flag | Meaning |
|---|---|
| `--branch <name>` | Read from the named branch. Default `main`. |
| `--version <N>` | Read version `N` on the chosen branch. Default: latest. |
| `--tag <name>` | Read the tagged `(branch, version)`. |
| `--as-of <instant>` | Read the latest version committed at or before `<instant>`. |

`--version`, `--tag`, and `--as-of` each name a single version and are mutually
exclusive. Each combines with `--branch`.

`--as-of` accepts three forms:

- RFC 3339 with an offset: `2026-07-01T12:00:00Z`, `2026-07-01T14:00:00+02:00`.
- A naive datetime: `2026-07-01T12:00:00`, interpreted as UTC rather than local
  time, so results are reproducible on any machine.
- A date: `2026-07-01`, interpreted as midnight UTC.

The resolved version is echoed on stderr, for example
`resolved --as-of to version 7 (2026-07-01T11:48:02Z)`. If the instant predates
the branch's first version, the error reports the earliest valid timestamp. On
a non-default branch the timeline starts at the branch-creation version, so an
instant before that is out of range for the branch even when it falls after the
parent commit the branch forked from.

```sh
arrs head -n 5 --version 3 dataset.lance
arrs rowcount --tag release-2026-04 dataset.lance
arrs cat --branch dev --columns id,score dataset.lance

arrs head -n 5 --as-of "2026-07-01T12:00:00Z" dataset.lance
arrs rowcount --as-of 2026-07-01 dataset.lance
arrs schema --branch dev --as-of "2026-06-15T09:30" dataset.lance
```

## Listing metadata

```sh
arrs versions dataset.lance                # every version on main
arrs versions --tagged-only dataset.lance  # only tagged versions
arrs versions --branch dev dataset.lance
arrs branches dataset.lance
arrs tags dataset.lance                    # across every branch
arrs indices dataset.lance
arrs index-stats dataset.lance
arrs fragments dataset.lance
arrs stat dataset.lance
```

## Row identity

Lance gives every row two 64-bit identifiers that no schema column carries.
`cat`, `head`, `tail`, `take`, and `sample` can surface them.

| Flag | Column | Meaning |
|---|---|---|
| `--with-row-id` | `_rowid` | The row's identity. Stable across deletions. Stable across compaction only for datasets written with Lance's stable row ids enabled (`enable_stable_row_ids`, off by default). With the default address-based ids, `_rowid` equals `_rowaddr` and compaction rewrites it. |
| `--with-row-addr` | `_rowaddr` | The row's physical address in the current version, `(fragment_id << 32) \| offset_in_fragment`. It is not stable across a rewrite or compaction. |

Both are `UInt64` and can be combined. They are appended after the projected
columns, `_rowid` first:

```sh
arrs head --with-row-id dataset.lance
# {"id":1,"score":0.5,…,"_rowid":0}

arrs head --columns id --with-row-id dataset.lance
# {"id":1,"_rowid":0}
```

They are not schema columns, so the flags rather than `--columns` decide
whether they appear: they are emitted whenever the flag is set, regardless of
`--columns` and `--exclude-columns`. Excluding a requested pseudo-column
(`--with-row-id --exclude-columns _rowid`) is contradictory and errors with a
hint to drop the flag.

Values are consistent across commands for the same version: `head`, `take`, and
`sample` report the same `_rowid` for a given row. They stay correct after
deletions, so the surviving `_rowid`s of a deleted range are non-contiguous.

These flags are Lance-only. A future backend that cannot provide row identity
rejects them with `not supported by this format`.
