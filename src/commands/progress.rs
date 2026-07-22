//! Opt-out scan progress indicator, drawn on **stderr**.
//!
//! Long full scans (`cat`, `stats`, `freq`, and the filtered `head`/`tail`/
//! `sample` paths) otherwise give no feedback. [`ScanProgress`] wraps the batch
//! stream so each yielded batch advances the indicator, without any change to
//! the `Dataset` trait.
//!
//! Two hard rules keep piping safe:
//!
//! * The indicator is drawn only when it is *enabled* — the caller folds in the
//!   `--no-progress` flag and a `stderr` TTY check, so a redirected/piped
//!   `stderr` yields a fully disabled ([`ScanProgress::disabled`]) handle whose
//!   methods are no-ops. Nothing is ever written in that case.
//! * indicatif draws exclusively to `stderr`, so **stdout stays byte-identical**
//!   whether or not a bar is shown. On completion the bar is cleared
//!   (`finish_and_clear`) so a captured `stderr` log is left clean too.

use std::time::Duration;

use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use crate::dataset::BatchStream;

/// A scan progress indicator, or a no-op handle when progress is disabled.
///
/// Cloneable and cheap: `ProgressBar` is internally reference-counted, so a
/// clone captured by [`ScanProgress::wrap`]'s stream closure shares the same
/// underlying bar as the handle the caller keeps to [`ScanProgress::finish`].
#[derive(Clone)]
pub struct ScanProgress {
    bar: Option<ProgressBar>,
}

impl ScanProgress {
    /// A disabled indicator whose methods do nothing. Used when `--no-progress`
    /// is set, when `stderr` is not a TTY, and throughout the tests.
    pub fn disabled() -> Self {
        Self { bar: None }
    }

    /// Build a scan progress indicator.
    ///
    /// `enabled` must already fold in the `--no-progress` flag **and** a
    /// `stderr` TTY check; when it is `false` this returns a disabled handle.
    /// `total` selects the style: `Some(rows)` shows a bar with an ETA (used
    /// when `count_rows` is cheap and no filter narrows the scan), `None` shows
    /// a spinner with a running rows-processed count.
    pub fn new(enabled: bool, total: Option<u64>) -> Self {
        if !enabled {
            return Self::disabled();
        }
        let bar = match total {
            Some(total) => {
                let bar = ProgressBar::new(total);
                bar.set_style(bar_style());
                bar
            }
            None => {
                let bar = ProgressBar::new_spinner();
                bar.set_style(spinner_style());
                // Animate the spinner even while a batch is in flight.
                bar.enable_steady_tick(Duration::from_millis(120));
                bar
            }
        };
        Self { bar: Some(bar) }
    }

    /// Wrap a batch stream so each yielded batch advances the indicator by its
    /// row count. A disabled handle returns the stream untouched.
    pub fn wrap(&self, stream: BatchStream) -> BatchStream {
        match &self.bar {
            None => stream,
            Some(bar) => {
                let bar = bar.clone();
                Box::pin(stream.inspect(move |item| {
                    if let Ok(batch) = item {
                        bar.inc(batch.num_rows() as u64);
                    }
                }))
            }
        }
    }

    /// Clear the indicator from the terminal. A no-op when disabled. Idempotent,
    /// so calling it after the scan (even on an error path) is always safe.
    pub fn finish(&self) {
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
        }
    }
}

/// Bar style for a known row total: position/length, a percentage, and an ETA.
fn bar_style() -> ProgressStyle {
    // Templates are static and validated by the tests below, so `expect` here
    // can only fire on a programming error, never at runtime on user input.
    ProgressStyle::with_template(
        "{spinner} {human_pos}/{human_len} rows [{bar:30}] {percent}% (ETA {eta})",
    )
    .expect("valid progress bar template")
    .progress_chars("=> ")
}

/// Spinner style for an unknown total: a running rows-processed count.
fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner} {human_pos} rows scanned ({elapsed})")
        .expect("valid spinner template")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_are_valid() {
        // Guards the `expect`s above: a malformed template would panic here.
        let _ = bar_style();
        let _ = spinner_style();
    }

    #[test]
    fn disabled_handle_is_inert() {
        let p = ScanProgress::disabled();
        // None of these should draw anything or panic.
        p.inc_for_test(10);
        p.finish();
        assert!(p.bar.is_none());
    }

    #[test]
    fn new_disabled_when_not_enabled() {
        assert!(ScanProgress::new(false, Some(100)).bar.is_none());
        assert!(ScanProgress::new(false, None).bar.is_none());
    }

    impl ScanProgress {
        /// Test-only shim to exercise the increment path on a disabled handle.
        fn inc_for_test(&self, rows: u64) {
            if let Some(bar) = &self.bar {
                bar.inc(rows);
            }
        }
    }
}
