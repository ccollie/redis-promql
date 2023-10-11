use chrono::Duration;
use crate::common::humanize::humanize_duration;

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    // todo: specifically get from redis
    chrono::Utc::now().timestamp_millis()
}

pub fn format_duration(d: std::time::Duration) -> String {
    humanize_duration(&d)
}
pub fn duration_to_chrono(duration: std::time::Duration) -> Duration {
    Duration::milliseconds(duration.as_millis() as i64)
}