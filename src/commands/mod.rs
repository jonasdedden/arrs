mod cat;
mod common;
mod diff;
mod diff_common;
mod freq;
mod head;
mod lance;
mod rowcount;
mod sample;
mod schema;
mod stats;
mod tail;
mod take;

use crate::Result;
use crate::cli::{Cli, Command, Format};
use crate::error::Error;
use crate::output::RenderOptions;

/// What a successfully-run command signals to the process exit code.
///
/// Every command yields `Success` except `diff`, which yields `Different` when
/// the two versions it compared are not identical. `main` maps `Success` → 0,
/// `Different` → 1, and any `Err` → 2 (see `diff(1)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Different,
}

pub async fn dispatch(cli: Cli) -> Result<Outcome> {
    let columns = cli.columns.as_deref();
    let exclude = cli.exclude_columns.as_deref();
    let render = RenderOptions {
        binary_format: cli.binary_format,
        max_list_items: cli.max_list_items,
        max_cell_width: cli.max_cell_width,
        float_precision: cli.float_precision,
    };
    if let Some(name) = command_ignoring_format(&cli.command)
        && cli.format.is_some()
    {
        return Err(Error::FormatNotApplicable { command: name });
    }
    let explicit_format = cli.format;
    // `diff` owns its own format semantics (human summary vs `--format jsonl`)
    // and its own exit-code outcome, so it is branched out before the
    // row-format machinery the other commands share.
    //
    // A single `diff` verb spans two modes, chosen by the number of positional
    // datasets:
    //   * two datasets (`diff A B`)         -> generic dataset-vs-dataset diff;
    //   * one dataset + `--from`/`--from-tag`-> Lance version diff.
    // Conflicting combinations (a second dataset alongside version selectors, or
    // one dataset with no `--from`) are rejected here rather than by clap, which
    // cannot express "required only when the second positional is absent".
    if let Command::Diff {
        input,
        other,
        from,
        from_tag,
        to,
        to_tag,
        branch,
    } = cli.command
    {
        if let Some(other) = other {
            // Dataset-vs-dataset mode: Lance version selectors are ambiguous
            // across two different datasets, so any of them is a hard error.
            if from.is_some()
                || from_tag.is_some()
                || to.is_some()
                || to_tag.is_some()
                || branch.is_some()
            {
                return Err(Error::DiffSelectorsInTwoDatasetMode);
            }
            return diff::run(&input, &other, columns, exclude, explicit_format).await;
        }
        // Version mode: a left-hand selector is mandatory (the old clap
        // `from_ref` group, now enforced here so two-dataset mode can omit it).
        if from.is_none() && from_tag.is_none() {
            return Err(Error::DiffMissingFromRef);
        }
        let selectors = lance::diff::DiffSelectors {
            branch,
            from_version: from,
            from_tag,
            to_version: to,
            to_tag,
        };
        return lance::diff::run(&input, selectors, explicit_format).await;
    }
    let format = resolve_format(explicit_format, &cli.command);
    run_command(cli.command, format, render, columns, exclude).await?;
    Ok(Outcome::Success)
}

async fn run_command(
    command: Command,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
) -> Result<()> {
    match command {
        Command::Cat {
            inputs,
            filter,
            lance,
        } => {
            cat::run(
                &inputs,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Head {
            input,
            limit,
            filter,
            lance,
        } => {
            head::run(
                &input,
                limit,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Tail {
            input,
            limit,
            filter,
            lance,
        } => {
            tail::run(
                &input,
                limit,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Take {
            input,
            indices,
            filter,
            lance,
        } => {
            take::run(
                &input,
                &indices,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Rowcount {
            input,
            filter,
            lance,
        } => rowcount::run(&input, filter.predicate.as_deref(), &lance).await,
        Command::Sample {
            input,
            limit,
            seed,
            filter,
            lance,
        } => {
            sample::run(
                &input,
                limit,
                seed,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Freq {
            input,
            column,
            limit,
            sort,
            filter,
            lance,
        } => {
            freq::run(
                &input,
                &column,
                limit,
                sort,
                format,
                render,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Schema { input, ty, lance } => {
            schema::run(&input, ty, columns, exclude, &lance).await
        }
        Command::Stats {
            input,
            filter,
            lance,
        } => {
            stats::run(
                &input,
                format,
                render,
                columns,
                exclude,
                filter.predicate.as_deref(),
                &lance,
            )
            .await
        }
        Command::Versions {
            input,
            branch,
            tagged_only,
        } => lance::versions::run(&input, branch.as_deref(), tagged_only, format, render).await,
        Command::Branches { input } => lance::branches::run(&input, format, render).await,
        Command::Tags { input } => lance::tags::run(&input, format, render).await,
        Command::Indices {
            input,
            lance: lance_args,
        } => lance::indices::run(&input, &lance_args, format, render).await,
        Command::Fragments {
            input,
            verbose,
            no_size,
            lance: lance_args,
        } => lance::fragments::run(&input, &lance_args, verbose, no_size, format, render).await,
        Command::Search {
            input,
            column,
            vector,
            vector_file,
            k,
            nprobes,
            refine_factor,
            lance,
        } => {
            // clap's `query_vector` group guarantees exactly one of these is set.
            let source = match (vector.as_deref(), vector_file.as_deref()) {
                (Some(inline), _) => lance::search::QuerySource::Inline(inline),
                (None, Some(path)) => lance::search::QuerySource::File(path),
                (None, None) => unreachable!("clap requires one of --vector/--vector-file"),
            };
            lance::search::run(
                &input,
                &column,
                source,
                k as usize,
                nprobes.map(|n| n as usize),
                refine_factor,
                format,
                render,
                columns,
                exclude,
                &lance,
            )
            .await
        }
        Command::IndexStats {
            input,
            lance: lance_args,
        } => lance::index_stats::run(&input, &lance_args, format, render).await,
        Command::Stat {
            input,
            no_size,
            lance: lance_args,
        } => lance::stat::run(&input, &lance_args, no_size, format, render).await,
        // `diff` is intercepted in `dispatch` (distinct format + exit-code
        // handling) and never reaches this shared row-format path.
        Command::Diff { .. } => unreachable!("diff is dispatched separately"),
    }
}

/// Apply the per-command default when `--format` was not given on the CLI.
/// Summary/metadata commands (freq/versions/branches/tags/indices/index-stats/
/// fragments/stats/stat) default to
/// `Table`; everything else defaults to `Jsonl`.
fn resolve_format(explicit: Option<Format>, cmd: &Command) -> Format {
    if let Some(f) = explicit {
        return f;
    }
    match cmd {
        Command::Freq { .. }
        | Command::Versions { .. }
        | Command::Branches { .. }
        | Command::Tags { .. }
        | Command::Indices { .. }
        | Command::IndexStats { .. }
        | Command::Fragments { .. }
        | Command::Stats { .. }
        | Command::Stat { .. } => Format::Table,
        _ => Format::Jsonl,
    }
}

/// Returns the command name for those that don't emit row-shaped output, so
/// `--format` against them is a hard error rather than a silent no-op.
fn command_ignoring_format(cmd: &Command) -> Option<&'static str> {
    match cmd {
        Command::Rowcount { .. } => Some("rowcount"),
        Command::Schema { .. } => Some("schema"),
        _ => None,
    }
}
