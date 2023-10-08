// Utility functions for the redis timeseries API

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use metricsql_engine::{MetricName, QueryResult, Tag, Timestamp};
use redis_module::{CallOptionResp, CallOptions, CallOptionsBuilder, CallReply, CallResult, Context, RedisError, RedisResult, RedisString};
use serde::{Deserialize, Serialize};
use crate::common::METRIC_NAME_LABEL;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub type Labels = HashMap<String, String>;  // todo use ahash

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
    pub labels: HashMap<String, String>,
    pub source_key: String,
    pub retention: u64,
}


pub fn series_exists(ctx: &RedisContext, key: &str) -> bool {
    ctx.call("TS.GET", &[key, "LATEST"]).is_ok()
}

pub fn get_series_labels(ctx: &RedisContext, key: &str) -> HashMap<String, String> {
    if let Ok(Some(reply)) = get_series_labels_ex(ctx, key) {
        return parse_labels(reply).unwrap_or_default();
    }
    HashMap::default()
}

pub(crate) fn get_series_labels_as_metric_name(ctx: &RedisContext, key: &str) -> RedisResult<MetricName> {
    if let Some(reply) = get_series_labels_ex(ctx, key)? {
        return parse_labels_as_metric_name(reply)
    }
    Ok(MetricName::default())
}

fn get_series_labels_ex(ctx: &RedisContext, key: &str) -> RedisResult<Option<CallReply>> {
    // prefer RESP3, but note that we may still get RESP2. IOW, we need to handle both
    // Map and Array replies.
    // See version support here:
    // https://docs.redis.com/latest/stack/release-notes/redistimeseries/
    let call_options: CallOptions = CallOptionsBuilder::default()
        .resp(CallOptionResp::Resp3)
        .build();

    let res: CallReply = ctx
        .call_ext::<_, CallResult>("TS.INFO", &call_options, &[key])
        .map_err(|e| -> RedisError { e.into() }).ok()?;

    if let CallReply::Map(map) = res {
        for (k, v) in map.iter() {
            if let CallReply::String(s) = k.unwrap_or_default() {
                if s.as_bytes() == b"labels" {
                    return Ok(Some(v?));
                }
            }
        }
    }
    Ok(None)
}

pub fn get_series_info<'root>(ctx: &RedisContext, key: &str) -> Option<TimeSeriesInfo> {
    let call_options: CallOptions = CallOptionsBuilder::default()
        .resp(CallOptionResp::Resp3)
        .build();

    let res: CallReply = ctx
        .call_ext::<_, CallResult>("TS.INFO", &call_options, &[key])
        .map_err(|e| -> RedisError { e.into() }).ok()?;

    if let CallReply::Map(map) = res {
        // transform into TimeSeriesInfo

        let mut info = TimeSeriesInfo::default();
        for (k, v) in map.iter() {
            let key = k.unwrap().to_string();
            let val = v.ok()?; // todo: map_err
            match key.as_str() {
                "chunk_type" => {
                    let chunk_type_string = call_reply_to_string(&val);
                    match Encoding::from_str(chunk_type_string.as_str()) {
                        Ok(encoding) => {
                            info.chunk_type = encoding;
                        }
                        Err(_) => {
                            // todo
                        }
                    }
                }
                "duplicate_policy" => {
                    let policy_string = call_reply_to_string(&val);
                    info.duplicate_policy = DuplicatePolicy::from(policy_string.as_str());
                }
                "chunk_size" => {
                    info.chunk_size = call_reply_to_i64(&val) as usize;
                }
                "total_samples" => {
                    info.total_samples = call_reply_to_i64(&val) as usize;
                }
                "memory_usage" => {
                    info.memory_usage = call_reply_to_i64(&val) as usize;
                }
                "first_timestamp" => {
                    info.first_timestamp = call_reply_to_i64(&val) as u64;
                }
                "last_timestamp" => {
                    info.last_timestamp = call_reply_to_timestamp(&val) as u64;
                }
                "chunk_count" => {
                    info.chunk_count = call_reply_to_i64(&val) as usize;
                }
                "source_key" => {
                    info.source_key = call_reply_to_string(&val);
                }
                "retention" => {
                    info.retention = call_reply_to_i64(&val) as u64;
                }
                "labels" => {
                    if let Ok(labels) = parse_labels(val) {
                        info.labels = labels;
                    }
                }
                _ => {
                    // todo
                }
            }

            return Some(info);
        }
    }
    None
}

pub(crate) fn parse_labels(reply: CallReply) -> RedisResult<HashMap<String, String>> {
    let mut result = HashMap::new(); // todo: use ahash
    match reply {
        CallReply::Map(map) => {
            // todo: return refs
            for (k, v) in map.iter() {
                // todo: map_err for k and v
                let key = k.unwrap();
                let val = v.unwrap();
                result.insert(
                    call_reply_to_string(&key),
                    call_reply_to_string(&val),
                );
            }
        }
        CallReply::Array(arr) => {
            let mut i = 0;
            while i < arr.len() {
                let key = arr.get(i).unwrap()?;
                let val = arr.get(i + 1).unwrap()?;
                result.insert(
                    call_reply_to_string(&key),
                    call_reply_to_string(&val),
                );
                i += 2;
            }
        }
        _=> {}
    }
    Ok(result)
}


pub(crate) fn parse_labels_as_metric_name(reply: CallReply) -> RedisResult<MetricName> {
    let mut result = MetricName::default(); // todo: use ahash

    fn add_tag(metric_name: &mut MetricName, key: String, value: String) {
        if key == METRIC_NAME_LABEL {
            metric_name.set_metric_name(value);
        } else {
            // break the abstraction to avoid allocations
            let tag = Tag {
                key,
                value,
            };
            metric_name.tags.push(tag);
        }
    }

    match reply {
        CallReply::Map(map) => {
            for (k, v) in map.iter() {
                let key = call_reply_to_string(&k?);
                let val = call_reply_to_string(&v?);
                add_tag(&mut result, key, val);
            }
        }
        CallReply::Array(arr) => {
            let mut i = 0;
            while i < arr.len() {
                let k = arr.get(i).unwrap()?;
                let v = arr.get(i + 1).unwrap()?;
                let key = call_reply_to_string(&k);
                let val = call_reply_to_string(&v);
                add_tag(&mut result, key, val);
                i += 2;
            }
        }
        _=> {}
    }

    result.sort_tags();

    Ok(result)
}

pub(crate) fn ts_range(ctx: &Context, key: &RedisString, start: Timestamp, end: Timestamp) -> RedisResult<(Vec<Timestamp>, Vec<f64>)> {
    let start_str = ctx.create_string(start.to_string());
    let end_str = ctx.create_string(end.to_string());

    // https://redis.io/commands/ts.range/
    let args: [&RedisString; 3] = [key, start_str, end_str];
    let reply = ctx.call("TS.RANGE", args)?;

    if let CallReply::Array(arr) = reply {
        let len = arr.len() / 2;
        let mut timestamps = Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);

        for (i, val) in arr.iter().enumerate() {
            match val {
                Ok(value) => {
                    if i % 2 == 0 {
                        timestamps.push(value.to_i64());
                    } else {
                        values.push(value.to_double());
                    }
                },
                Err(e) => {
                    return Err(e.into());
                }
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
    for (i, (ts, val)) in timestamps.iter().zip(values.iter()).enumerate() {
        buf.push(&ctx.create_string(key));
        buf.push(&ctx.create_string(ts.to_string())); // todo: use buffers
        buf.push(&ctx.create_string(val.to_string()));
        if i % MULTI_ADD_CHUNK_SIZE == 0 {
            ctx.call("TS.MADD", buf.as_slice())?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        ctx.call("TS.MADD", buf.as_slice())?;
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

    let mut buf: [String; 3] = ["".to_string(), "".to_string(), "".to_string()];
    let mut offset = 0;

    let mut params: Vec<&str> = Vec::with_capacity(capacity);
    params.push(key);
    if let Some(chunk_size) = options.chunk_size {
        params.push( "CHUNK_SIZE");
        buf[offset] = chunk_size.to_string();
        params.push(&buf[offset]);
        offset += 1;
    }
    if let Some(encoding) = &options.encoding {
        params.push("ENCODING");
        params.push(encoding.as_str());
    }
    if let Some(retention) = options.retention {
        params.push("RETENTION");
        buf[offset] = retention.as_millis().to_string();
        params.push(&buf[offset]);
        offset += 1;
    }
    if let Some(duplicate_policy) = options.duplicate_policy {
        params.push("ON_DUPLICATE");
        params.push(duplicate_policy.as_str());
    }
    if !options.labels.is_empty() {
        params.push("LABELS");
        for (k, v) in options.labels.iter() {
            params.push( k.as_str());
            params.push( v.as_str());
        }
        if !options.metric_name.is_empty() {
            params.push(METRIC_NAME_LABEL);
            params.push(&options.metric_name);
        }
    }

    ctx.call("TS.CREATE", params.as_slice())
}

// todo: utils
pub fn call_reply_to_i64(reply: &CallReply) -> i64 {
    match reply {
        CallReply::I64(i) => i.to_i64(),
        _ => panic!("unexpected reply type"),
    }
}

pub fn call_reply_to_f64(reply: &CallReply) -> f64 {
    match reply {
        CallReply::Double(d) => d.to_double(),
        CallReply::I64(i) => i.to_i64() as f64,
        _ => panic!("unexpected reply type"),
    }
}

pub fn call_reply_to_string(reply: &CallReply) -> String {
    match reply {
        CallReply::String(s) => s.to_string().unwrap_or_default(),
        CallReply::VerbatimString(s) => s.to_string(),
        _ => panic!("unexpected reply type"),
    }
}

pub fn call_reply_to_timestamp(reply: &CallReply) -> Timestamp {
    let val = call_reply_to_i64(reply);
    val
}