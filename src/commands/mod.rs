mod cat;
mod common;
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

pub async fn dispatch(cli: Cli) -> Result<()> {
    let columns = cli.columns.as_deref();
    let exclude = cli.exclude_columns.as_deref();
    let binary_format = cli.binary_format;
    if let Some(name) = command_ignoring_format(&cli.command)
        && cli.format.is_some()
    {
        return Err(Error::FormatNotApplicable { command: name });
    }
    let format = resolve_format(cli.format, &cli.command);
    match cli.command {
        Command::Cat {
            inputs,
            filter,
            lance,
        } => {
            cat::run(
                &inputs,
                format,
                binary_format,
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
                binary_format,
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
                binary_format,
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
                binary_format,
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
                binary_format,
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
                binary_format,
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
                binary_format,
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
        } => {
            lance::versions::run(
                &input,
                branch.as_deref(),
                tagged_only,
                format,
                binary_format,
            )
            .await
        }
        Command::Branches { input } => lance::branches::run(&input, format, binary_format).await,
        Command::Tags { input } => lance::tags::run(&input, format, binary_format).await,
        Command::Indices {
            input,
            lance: lance_args,
        } => lance::indices::run(&input, &lance_args, format, binary_format).await,
        Command::Fragments {
            input,
            verbose,
            no_size,
            lance: lance_args,
        } => {
            lance::fragments::run(&input, &lance_args, verbose, no_size, format, binary_format)
                .await
        }
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
                binary_format,
                columns,
                exclude,
                &lance,
            )
            .await
        }
        Command::IndexStats {
            input,
            lance: lance_args,
        } => lance::index_stats::run(&input, &lance_args, format, binary_format).await,
    }
}

/// Apply the per-command default when `--format` was not given on the CLI.
/// Summary/metadata commands (freq/versions/branches/tags/indices/index-stats/
/// fragments/stats) default to
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
        | Command::Stats { .. } => Format::Table,
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
