use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use redis_module::{Context, RedisError, RedisString};
use serde::{Deserialize, Serialize};
use crate::common::types::Timestamp;
use crate::ts::{DEFAULT_CHUNK_SIZE_BYTES, DuplicatePolicy};
use crate::ts::time_series::TimeSeries;


#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub enum Encoding {
    #[default]
    Compressed,
    Uncompressed,
}

impl Encoding {
    pub fn is_compressed(&self) -> bool {
        match self {
            Encoding::Compressed => true,
            Encoding::Uncompressed => false,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Compressed => "COMPRESSED",
            Encoding::Uncompressed => "UNCOMPRESSED",
        }
    }
}

impl Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Encoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "compressed" => Ok(Encoding::Compressed),
            "uncompressed" => Ok(Encoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", s)),
        }
    }
}



type RedisContext = redis_module::Context;

#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: String,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub labels: HashMap<String, String>,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: key.to_string(),
            encoding: None,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            duplicate_policy: None,
            labels: Default::default(),
        }
    }

    pub fn encoding(&mut self, encoding: Encoding) {
        self.encoding = Some(encoding);
    }

    pub fn chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = Some(chunk_size);
    }

    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn duplicate_policy(&mut self, duplicate_policy: DuplicatePolicy) {
        self.duplicate_policy = Some(duplicate_policy);
    }

    pub fn labels(&mut self, labels: HashMap<String, String>) {
        self.labels = labels;
    }
}


pub(super) fn internal_add(
    ctx: &Context,
    series: &mut TimeSeries,
    timestamp: Timestamp,
    value: f64,
    dp_override: DuplicatePolicy,
) -> Result<(), RedisError> {
    let last_ts = series.last_timestamp;
    // ensure inside retention period.
    if series.is_older_than_retention(timestamp) {
        return Err(RedisError::Str("TSDB: Timestamp is older than retention"));
    }

    if let Some(dedup_interval) = series.dedupe_interval {
        let millis = dedup_interval.as_millis() as i64;
        if millis > 0 && (timestamp - last_ts) < millis {
            return Err(RedisError::Str(
                "TSDB: new sample in less than dedupe interval",
            ));
        }
    }

    if timestamp <= series.last_timestamp && series.total_samples != 0 {
        let val = series.upsert_sample(timestamp, value, Some(dp_override));
        if val.is_err() {
            let msg = "TSDB: Error at upsert, cannot update when DUPLICATE_POLICY is set to BLOCK";
            return Err(RedisError::Str(msg));
        }
        return Ok(());
    }

    series
        .append(timestamp, value)
        .map_err(|_| RedisError::Str("TSDB: Error at append"))
}
