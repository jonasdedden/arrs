mod cat;
mod common;
mod head;
mod lance;
mod rowcount;
mod sample;
mod schema;
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
        Command::Cat { inputs, lance } => {
            cat::run(&inputs, format, binary_format, columns, exclude, &lance).await
        }
        Command::Head {
            input,
            limit,
            lance,
        } => {
            head::run(
                &input,
                limit,
                format,
                binary_format,
                columns,
                exclude,
                &lance,
            )
            .await
        }
        Command::Tail {
            input,
            limit,
            lance,
        } => {
            tail::run(
                &input,
                limit,
                format,
                binary_format,
                columns,
                exclude,
                &lance,
            )
            .await
        }
        Command::Take {
            input,
            indices,
            lance,
        } => {
            take::run(
                &input,
                &indices,
                format,
                binary_format,
                columns,
                exclude,
                &lance,
            )
            .await
        }
        Command::Rowcount { input, lance } => rowcount::run(&input, &lance).await,
        Command::Sample {
            input,
            limit,
            seed,
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
                &lance,
            )
            .await
        }
        Command::Schema { input, ty, lance } => {
            schema::run(&input, ty, columns, exclude, &lance).await
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
    }
}

/// Apply the per-command default when `--format` was not given on the CLI.
/// Metadata commands (versions/branches/tags/indices) default to `Table`;
/// everything else defaults to `Jsonl`.
fn resolve_format(explicit: Option<Format>, cmd: &Command) -> Format {
    if let Some(f) = explicit {
        return f;
    }
    match cmd {
        Command::Versions { .. }
        | Command::Branches { .. }
        | Command::Tags { .. }
        | Command::Indices { .. } => Format::Table,
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
