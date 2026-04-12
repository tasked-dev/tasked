//! Cron schedule utilities.

use chrono::{DateTime, Utc};

/// Normalize a cron expression: the `cron` crate uses 7-field expressions (with seconds).
/// Standard cron uses 5 fields. If the expression has 5 fields, prepend "0 " for seconds.
pub fn normalize_cron(expr: &str) -> String {
    let fields = expr.split_whitespace().count();
    if fields == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    }
}

/// Compute the next run time after `after` for the given cron expression.
pub fn compute_next_run(cron_expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    use std::str::FromStr;
    cron::Schedule::from_str(cron_expr)
        .ok()
        .and_then(|s| s.after(&after).next())
}

