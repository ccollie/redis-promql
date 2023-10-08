use chrono::Duration;
use crate::common::humanize::humanize_duration;

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    // todo: specifically get from redis
    chrono::Utc::now().timestamp_millis()
}


pub fn parse_duration(s: &str) -> Result<Duration, String> {
    metricsql_parser::prelude::parse_duration_value(s, 1)
        .map_or(Err(format!("Failed to parse duration: {}", s)), |v| Ok(Duration::milliseconds(v)))
}

pub fn format_duration(d: std::time::Duration) -> String {
    humanize_duration(&d)
}