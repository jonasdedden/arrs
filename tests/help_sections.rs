//! Snapshot-style tests for the grouped `--help` output (issue #47).
//!
//! Rather than pin the entire (long, wrap-sensitive) help text, these assert the
//! structural contract that matters: the section headings exist, and a
//! representative flag lands under the *correct* heading. That catches both
//! regressions (a flag drifting out of its group) and clap's classic pitfalls —
//! heading leakage (a flag inheriting the previous group's heading) and a
//! command-specific flag being swept into a named section.

use std::process::Command;

/// Run `arrs <args...> --help` and return stdout.
fn help(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_arrs"))
        .args(args)
        .arg("--help")
        .output()
        .expect("spawn arrs binary");
    assert!(
        out.status.success(),
        "`arrs {} --help` exited non-zero",
        args.join(" ")
    );
    String::from_utf8(out.stdout).expect("help output is UTF-8")
}

/// Parse `--help` stdout into `(heading, flags-under-it)` pairs, in render order.
///
/// A "heading" is any non-indented line ending in `:` (e.g. `Options:`,
/// `Lance options:`); a "flag" is an indented line whose first token starts with
/// `-`. Long value descriptions (indented prose) are ignored because they never
/// start with `-`.
fn sections(help_text: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for line in help_text.lines() {
        if !line.starts_with(char::is_whitespace) && line.ends_with(':') && !line.is_empty() {
            out.push((line.trim_end_matches(':').to_string(), Vec::new()));
        } else if let Some(current) = out.last_mut() {
            let trimmed = line.trim_start();
            if let Some(first) = trimmed.split_whitespace().next()
                && first.starts_with('-')
            {
                // Normalize `-n,` / `--limit` etc. to the leading token.
                current.1.push(first.trim_end_matches(',').to_string());
            }
        }
    }
    out
}

/// The set of flags rendered under `heading`, or `None` if the heading is absent.
fn flags_under<'a>(secs: &'a [(String, Vec<String>)], heading: &str) -> Option<&'a [String]> {
    secs.iter()
        .find(|(h, _)| h == heading)
        .map(|(_, f)| f.as_slice())
}

fn assert_grouped(cmd: &str, secs: &[(String, Vec<String>)]) {
    // Each named section exists and contains its representative flag.
    let selection = flags_under(secs, "Selection options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Selection options` heading"));
    assert!(
        selection.iter().any(|f| f == "--columns"),
        "`{cmd}`: --columns not under `Selection options` (got {selection:?})"
    );

    let output = flags_under(secs, "Output options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Output options` heading"));
    assert!(
        output.iter().any(|f| f == "--format"),
        "`{cmd}`: --format not under `Output options` (got {output:?})"
    );
    assert!(
        output.iter().any(|f| f == "--no-progress"),
        "`{cmd}`: --no-progress not under `Output options` (got {output:?})"
    );

    let lance = flags_under(secs, "Lance options")
        .unwrap_or_else(|| panic!("`{cmd}` help missing `Lance options` heading"));
    assert!(
        lance.iter().any(|f| f == "--branch"),
        "`{cmd}`: --branch not under `Lance options` (got {lance:?})"
    );

    // No leakage: the format/selection flags must NOT also appear in the default
    // `Options` block (which should only hold command-specific flags + --help).
    if let Some(default) = flags_under(secs, "Options") {
        for leaked in ["--format", "--columns", "--branch", "--no-progress"] {
            assert!(
                !default.iter().any(|f| f == leaked),
                "`{cmd}`: {leaked} leaked into the ungrouped `Options` block"
            );
        }
    }

    // Section ordering: Selection is rendered before Output for every command.
    let pos = |name: &str| secs.iter().position(|(h, _)| h == name);
    assert!(
        pos("Selection options") < pos("Output options"),
        "`{cmd}`: expected `Selection options` before `Output options`"
    );
}

#[test]
fn cat_help_groups_options_into_sections() {
    let secs = sections(&help(&["cat"]));
    assert_grouped("cat", &secs);

    // cat flattens RowIdArgs, so the row-id pseudo-column flags are Lance-grouped.
    let lance = flags_under(&secs, "Lance options").unwrap();
    assert!(
        lance.iter().any(|f| f == "--with-row-id"),
        "cat: --with-row-id should be under `Lance options` (got {lance:?})"
    );
    // --where (from the flattened FilterArg) is a Selection flag.
    let selection = flags_under(&secs, "Selection options").unwrap();
    assert!(
        selection.iter().any(|f| f == "--where"),
        "cat: --where should be under `Selection options` (got {selection:?})"
    );
}

#[test]
fn search_help_groups_options_into_sections() {
    let secs = sections(&help(&["search"]));
    assert_grouped("search", &secs);

    // search's own tuning flags stay ungrouped in the default `Options` block.
    let default = flags_under(&secs, "Options").expect("search has an `Options` block");
    for own in ["--column", "--vector", "--nprobes"] {
        assert!(
            default.iter().any(|f| f == own),
            "search: {own} should stay ungrouped in `Options` (got {default:?})"
        );
    }
}
