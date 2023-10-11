// Utility functions for the redis timeseries API
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use ahash::AHashMap;
use metricsql_engine::{MetricName, QueryResult, Tag, Timestamp};
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use serde::{Deserialize, Serialize};
use crate::common::METRIC_NAME_LABEL;
use crate::module::{call_redis_command, redis_value_as_str, redis_value_as_string, redis_value_key_as_str, redis_value_to_f64, redis_value_to_i64};

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub type Labels = AHashMap<String, String>;  // todo use ahash

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

#[derive(Debug, Default, Hash, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    First,
    /// overwrite the existing value with the new value
    Last,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum
}

impl DuplicatePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::First => "first",
            DuplicatePolicy::Last => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for DuplicatePolicy {
    fn from(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "block" => DuplicatePolicy::Block,
            "first" => DuplicatePolicy::First,
            "last" => DuplicatePolicy::Last,
            "min" => DuplicatePolicy::Min,
            "max" => DuplicatePolicy::Max,
            "sum" => DuplicatePolicy::Sum,
            _ => panic!("invalid duplicate policy: {}", s),
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

/// https://redis.io/commands/ts.info/
#[derive(Debug, Clone, Default)]
pub struct TimeSeriesInfo {
    /// Total number of samples in this time series
    pub total_samples: usize,
    /// Total number of bytes allocated for this time series
    pub memory_usage: usize,
    /// First timestamp present in this time series (Unix timestamp in milliseconds)
    pub first_timestamp: u64,
    /// Last timestamp present in this time series (Unix timestamp in milliseconds)
    pub last_timestamp: u64,
    /// Number of chunks in this time series
    pub chunk_count: usize,
    /// The initial allocation size, in bytes, for the data part of each new chunk.
    pub chunk_size: usize,
    pub chunk_type: Encoding,
    pub duplicate_policy: DuplicatePolicy,
    pub labels: AHashMap<String, String>,
    pub source_key: String,
    pub retention: u64,
}


pub fn series_exists(ctx: &RedisContext, key: &str) -> bool {
    ctx.call("TS.GET", &[key, "LATEST"]).is_ok()
}

pub fn get_series_labels(ctx: &RedisContext, key: &str) -> AHashMap<String, String> {
    if let Ok(Some(reply)) = get_series_labels_ex(ctx, key.to_string()) {
        return parse_labels(reply).unwrap_or_default();
    }
    AHashMap::default()
}

pub(crate) fn get_series_labels_as_metric_name(ctx: &RedisContext, key: &str) -> RedisResult<MetricName> {
    if let Some(reply) = get_series_labels_ex(ctx, key.to_string())? {
        return parse_labels_as_metric_name(&reply)
    }
    Ok(MetricName::default())
}

fn get_series_labels_ex(ctx: &RedisContext, key: String) -> RedisResult<Option<RedisValue>> {
    let res = call_redis_command(ctx, "TS.INFO", &[key])?;

    if let RedisValue::Array(mut arr) = res {
        let mut found = false;
        for (i, v) in arr.iter_mut().enumerate() {
            if i % 2 == 0 {
                let k = redis_value_as_str(&v)?;
                found = k == "labels";
            } else if found {
                // todo: send PR for RedisValue::default().
                // let res = std::mem::take(&mut v);
                let mut res = RedisValue::Bool(true);
                std::mem::swap(&mut res, v);
                return Ok(Some(res));
            }
        }
    }
    Ok(None)
}

pub fn get_series_info<'root>(ctx: &RedisContext, key: &str) -> RedisResult<Option<TimeSeriesInfo>> {
    let mut res = call_redis_command(ctx, "TS.INFO", &[key.to_string()])?;

    if let RedisValue::Array(ref mut arr) = res {
        // transform into TimeSeriesInfo

        let mut info = TimeSeriesInfo::default();
        let items = arr.as_mut_slice();
        let mut i = 0;
        while i < items.len() {
            let key = redis_value_as_str(&items[i])?;
            let val = &items[i + 1];
            match key.as_ref() {
                "chunk_type" => {
                    let chunk_type_string = redis_value_as_str(&val)?;
                    match Encoding::from_str(&chunk_type_string) {
                        Ok(encoding) => {
                            info.chunk_type = encoding;
                        }
                        Err(_) => {
                            // todo
                        }
                    }
                }
                "duplicate_policy" => {
                    let policy_string = redis_value_as_str(&val)?;
                    info.duplicate_policy = DuplicatePolicy::from(policy_string.as_ref());
                }
                "chunk_size" => {
                    info.chunk_size = redis_value_to_i64(&val)? as usize;
                }
                "total_samples" => {
                    info.total_samples = redis_value_to_i64(&val)? as usize;
                }
                "memory_usage" => {
                    info.memory_usage = redis_value_to_i64(&val)? as usize;
                }
                "first_timestamp" => {
                    info.first_timestamp = redis_value_to_i64(&val)? as u64;
                }
                "last_timestamp" => {
                    info.last_timestamp = redis_value_to_i64(&val)? as u64;
                }
                "chunk_count" => {
                    info.chunk_count = redis_value_to_i64(&val)? as usize;
                }
                "source_key" => {
                    info.source_key = redis_value_as_string(&val)?.to_string();
                }
                "retention" => {
                    info.retention = redis_value_to_i64(&val)? as u64;
                }
                "labels" => {
                    // todo: get rid of clone !!!
                    if let Ok(labels) = parse_labels(val.clone()) {
                        info.labels = labels;
                    }
                }
                _ => {
                    // todo
                }
            }
            i += 2;
        }

        return Ok(Some(info))
    }
    Ok(None)
}

pub(crate) fn parse_labels(reply: RedisValue) -> RedisResult<AHashMap<String, String>> {
    fn add_entry(result: &mut AHashMap<String, String>, key: &str, value: &RedisValue) {
        match value {
            RedisValue::StringBuffer(s) => {
                let value = String::from_utf8_lossy(s);
                result.insert(key.to_string(), value.to_string());
            }
            RedisValue::BulkString(s) => {
                result.insert(key.to_string(), s.to_string());
            }
            RedisValue::BulkRedisString(s) => {
                if let Ok(s) = s.try_as_str() {
                    result.insert(key.to_string(), s.to_string());
                } else {
                    result.insert(key.to_string(), s.to_string());
                }
            }
            _=> {}
        }
    }

    match reply {
        RedisValue::Map(map) => {
            let mut result = AHashMap::with_capacity(map.len());
            for (k, v) in map.iter() {
                let key = redis_value_key_as_str(k)?;
                add_entry(&mut result, &key, v);
            }
            return Ok(result)
        }
        RedisValue::Array(arr) => {
            let mut result = AHashMap::with_capacity(arr.len() / 2);
            let mut key: Cow<str> = "".into();
            for (i, v) in arr.iter().enumerate() {
                if i % 2 == 0 {
                    key = redis_value_as_str(v)?;
                } else {
                    add_entry(&mut result, &key, v);
                }
            }
            return Ok(result)
        }
        _=> {}
    }
    Ok(AHashMap::new())
}


pub(crate) fn parse_labels_as_metric_name(reply: &RedisValue) -> RedisResult<MetricName> {
    let mut result = MetricName::default(); // todo: use ahash

    fn add_tag(metric_name: &mut MetricName, key: &str, value: &RedisValue) -> RedisResult<()> {
        let value = redis_value_as_str(value)?;
        if key == METRIC_NAME_LABEL {
            metric_name.set_metric_group(&value);
        } else {
            // break the abstraction to avoid allocations
            let tag = Tag {
                key: key.to_string(),
                value: value.to_string(),
            };
            metric_name.tags.push(tag);
        }
        Ok(())
    }

    match reply {
        RedisValue::Map(map) => {
            for (k, v) in map.iter() {
                let key = redis_value_key_as_str(k)?;
                add_tag(&mut result, &key, v)?;
            }
        }
        RedisValue::Array(arr) => {
            let mut i = 0;
            while i < arr.len() {
                match (arr.get(i), arr.get(i + 1)) {
                    (Some(k), Some(v)) => {
                        let key = redis_value_as_str(k)?;
                        add_tag(&mut result, &key, v)?;
                    }
                    _ => break,
                }
                i += 2;
            }
        }
        _=> {}
    }

    result.sort_tags();

    Ok(result)
}

pub(crate) fn ts_range(ctx: &Context, key: &str, start: Timestamp, end: Timestamp) -> RedisResult<(Vec<Timestamp>, Vec<f64>)> {
    let start_str = start.to_string();
    let end_str = end.to_string();

    // https://redis.io/commands/ts.range/
    let args: [&str; 3] = [key, &start_str, &end_str];
    let reply = ctx.call("TS.RANGE", &args)?;

    if let RedisValue::Array(arr) = reply {
        let len = arr.len() / 2;
        let mut timestamps = Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);

        for (i, val) in arr.iter().enumerate() {
            if i % 2 == 0 {
                timestamps.push(redis_value_to_i64(val)?);
            } else {
                values.push(redis_value_to_f64(val)?);
            }
        }

        Ok((timestamps, values))
    } else {
        return Err(RedisError::Str("TSDB: unexpected reply type from TS.RANGE"));
    }
}

// todo: it should be an error to add to an existing series where the labels don't match
pub fn write_ts_to_redis(ctx: &RedisContext, key: &str, option: &TimeSeriesOptions, times: &[Timestamp], values: &[f64]) -> Result<(), RedisError> {
    if times.is_empty() {
        // skip if series already exists
        if !series_exists(ctx, key) {
            create_series(ctx, key, option)?;
        }
    }

    ts_multi_add(ctx, key, times, values)
}

// todo: it should be an error to add to an existing series where the labels don't match
pub fn write_result_to_redis(ctx: &RedisContext, key: &str, result: &QueryResult, option: &TimeSeriesOptions) -> Result<(), RedisError> {
    write_ts_to_redis(ctx, key, option, &result.timestamps, &result.values)
}

const MULTI_ADD_CHUNK_SIZE: usize = 1024;

pub(crate) fn ts_multi_add(ctx: &RedisContext, key: &str, timestamps: &[Timestamp], values: &[f64]) -> Result<(), RedisError> {
    ts_multi_add_ex(ctx, key, timestamps, values)
}

pub(crate) fn ts_multi_add_ex(ctx: &RedisContext, key: &str, timestamps: &[Timestamp], values: &[f64]) -> Result<(), RedisError> {
    // todo: use a buffer pool. Theres way too many allocations here
    let mut buf = Vec::with_capacity(3 * MULTI_ADD_CHUNK_SIZE.min(timestamps.len()));
    //  let slice = buf.as_mut_slice();
    for (i, (ts, val)) in timestamps.iter().zip(values.iter()).enumerate() {
        buf.push(key.to_string());
        buf.push(ts.to_string()); // todo: use buffers
        buf.push(val.to_string());
        if i % MULTI_ADD_CHUNK_SIZE == 0 {
            call_redis_command(ctx, "TS.MADD", buf.as_slice())?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        call_redis_command(ctx, "TS.MADD", buf.as_slice())?;
    }
    Ok(())
}

pub fn create_series(ctx: &RedisContext, key: &str, options: &TimeSeriesOptions) -> RedisResult {
    let mut capacity = 1usize;
    if options.chunk_size.is_some() {
        capacity += 2;
    }
    if options.encoding.is_some() {
        capacity += 2;
    }
    if options.retention.is_some() {
        capacity += 2;
    }
    if options.duplicate_policy.is_some() {
        capacity += 2;
    }
    if !options.labels.is_empty() {
        capacity += 2 * options.labels.len();
        if !options.metric_name.is_empty() {
            capacity += 2;
        }
    }

    let mut params: Vec<String> = Vec::with_capacity(capacity);
    params.push(key.to_string());

    if let Some(chunk_size) = options.chunk_size {
        params.push( "CHUNK_SIZE".to_string());
        params.push(chunk_size.to_string());
    }
    if let Some(encoding) = &options.encoding {
        params.push("ENCODING".to_string());
        params.push(encoding.to_string());
    }
    if let Some(retention) = options.retention {
        params.push("RETENTION".to_string());
        params.push(retention.as_millis().to_string());
    }
    if let Some(duplicate_policy) = options.duplicate_policy {
        params.push("ON_DUPLICATE".to_string());
        params.push(duplicate_policy.to_string());
    }
    if !options.labels.is_empty() {
        params.push("LABELS".to_string());
        for (k, v) in options.labels.iter() {
            params.push( k.clone());
            params.push( v.clone());
        }
        if !options.metric_name.is_empty() {
            params.push(METRIC_NAME_LABEL.to_string());
            params.push(options.metric_name.clone());
        }
    }

    call_redis_command(ctx, "TS.CREATE", params.as_slice())
}
