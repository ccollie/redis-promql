use std::time::Duration;


/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    // todo: specifically get from redis
    // see https://redis.io/docs/reference/modules/modules-api-ref/#section-module-information-and-time-measurement
    chrono::Utc::now().timestamp_millis()
}


pub fn parse_duration(s: &str) -> Result<Duration, String> {
    // todo: make sure its positive
    metricsql_parser::prelude::parse_duration_value(s, 1)
        .map_or(Err(format!("Failed to parse duration: {}", s)), |v| Ok(Duration::from_millis(v as u64)))
}
