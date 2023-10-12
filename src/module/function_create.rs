use redis_module::{Context, NextArg, REDIS_OK, RedisError, RedisResult, RedisString};
use std::time::Duration;
use ahash::AHashMap;
use redis_module::key::RedisKeyWritable;
use crate::common::parse_duration;
use crate::globals::get_timeseries_index;
use crate::module::{DEFAULT_CHUNK_SIZE_BYTES, REDIS_PROMQL_TIMESERIES_TYPE};
use crate::ts::DuplicatePolicy;
use crate::ts::time_series::{Labels, TimeSeries};

pub struct TimeSeriesOptions {
    pub metric_name: String,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub dedupe_interval: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub labels: Labels,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: key.to_string(),
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            dedupe_interval: None,
            duplicate_policy: None,
            labels: Default::default(),
        }
    }

    pub fn chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = Some(chunk_size);
    }

    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn dedupe_interval(&mut self, dedupe_interval: Duration) {
        self.dedupe_interval = Some(dedupe_interval);
    }

    pub fn duplicate_policy(&mut self, duplicate_policy: DuplicatePolicy) {
        self.duplicate_policy = Some(duplicate_policy);
    }

    pub fn labels(&mut self, labels: Labels) {
        self.labels = labels;
    }
}

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_METRIC_NAME: &str = "METRIC_NAME";


pub fn create(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next().ok_or_else(|| RedisError::Str("Err missing key argument"))?;

    let mut options = TimeSeriesOptions::new(&key);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_duration(&next) {
                    options.retention(val);
                } else {
                    return Err(RedisError::Str("ERR invalid RETENTION value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_duration(&next) {
                    options.dedupe_interval(val);
                } else {
                    return Err(RedisError::Str("ERR invalid DEDUPE_INTERVAL value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                let next = args.next_str()?;
                if let Ok(policy) = DuplicatePolicy::try_from(next) {
                    options.duplicate_policy(policy);
                } else {
                    return Err(RedisError::Str("ERR invalid DUPLICATE_POLICY"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_METRIC_NAME) => {
                let next = args.next_str()?;
                options.metric_name = next.to_string();
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                let mut labels: AHashMap<String, String> = Default::default();
                while let Ok(name) = args.next_str() {
                    let value = args.next_str()?;
                    labels.insert(name.to_string(), value.to_string());
                }
                options.labels(labels);
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    let ts_index = get_timeseries_index();
    let mut ts = create_timeseries(options);
    ts_index.index_timeseries(&mut ts, key.to_string());

    let key = RedisKeyWritable::open(ctx.ctx, &key);
    key.set_value(&REDIS_PROMQL_TIMESERIES_TYPE, ts)?;

    REDIS_OK
}

pub(crate) fn create_timeseries(
    options: TimeSeriesOptions,
) -> TimeSeries {
    let mut ts = TimeSeries::new();
    ts.metric_name = options.metric_name;
    ts.chunk_size_bytes = options.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);
    ts.retention = options.retention.unwrap_or(Duration::from_millis(0u64));
    ts.dedupe_interval = options.dedupe_interval;
    ts.duplicate_policy = options.duplicate_policy;
    ts.labels = options.labels;
    ts
}