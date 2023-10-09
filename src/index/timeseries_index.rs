use metricsql_engine::MetricName;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use ahash::{AHashMap, AHashSet};
use lru_time_cache::LruCache;
use crate::common::regex_util::get_or_values;
use crate::module::{call_redis_command, get_series_labels_as_metric_name};

// todo: use https://github.com/Cognoscan/dynamic-lru-cache

pub type RedisContext = Context;

// todo: use a faster hash function (default is SipHasher)
pub type Labels = HashMap<String, String>;

static LABEL_CACHE_DURATION: Duration = Duration::from_secs(60);
static LABEL_CACHE_MAX_SIZE: usize = 100;

// todo: ahash for maps

/// Index for quick access to timeseries by label, label value or metric name.
pub struct TimeseriesIndex {
    id_to_group_key: RwLock<HashMap<u64, String>>,
    /// Map from timeseries id to timeseries key.
    id_to_key: RwLock<HashMap<u64, String>>,
    key_to_id: RwLock<HashMap<String, u64>>,
    timeseries_sequence: AtomicU64,
    group_sequence: AtomicU64,
    /// Cache of metric names. Stored in raw form since it's used in various contexts
    labels_cache: RwLock<LruCache<String, MetricName>>,
}

impl TimeseriesIndex {
    pub fn new() -> TimeseriesIndex {
        let labels_cache = RwLock::new(LruCache::with_expiry_duration_and_capacity(
            LABEL_CACHE_DURATION,
            LABEL_CACHE_MAX_SIZE,
        ));
        TimeseriesIndex {
            id_to_group_key: Default::default(),
            id_to_key: RwLock::new(HashMap::new()),
            key_to_id: Default::default(),
            timeseries_sequence: AtomicU64::new(0),
            group_sequence: AtomicU64::new(0),
            labels_cache
        }
    }

    pub fn clear(&self) {
        let mut id_to_key_map = self.id_to_key.write().unwrap();
        id_to_key_map.clear();

        let mut id_to_group_key = self.id_to_group_key.write().unwrap();
        id_to_group_key.clear();

        self.timeseries_sequence.store(0, Ordering::Relaxed);
        self.group_sequence.store(0, Ordering::Relaxed);
    }

    pub fn series_count(&self) -> usize {
        let id_to_key_map = self.id_to_key.read().unwrap();
        id_to_key_map.len()
    }

    pub(crate) fn next_id(&self) -> u64 {
        // we use Relaxed here since we only need uniqueness, not monotonicity
        self.timeseries_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn get_id_by_key(&self, key: &str) -> Option<u64> {
        let key_to_id = self.key_to_id.read().unwrap();
        key_to_id.get(key).copied()
    }

    fn get_or_create_id_by_key(&self, key: &str) -> u64 {
        let mut key_to_id = self.key_to_id.write().unwrap();
        if let Some(id) = key_to_id.get(key) {
            return *id;
        }
        let id = self.next_id();
        key_to_id.insert(key.to_string(), id);
        id
    }

    pub(crate) fn remove_labels_by_key(&self, key: &str) {
        self.labels_cache.write().unwrap().remove(key);
    }

    pub(crate) fn series_keys_by_matchers<'a>(
        &'a self,
        ctx: &'a Context,
        matchers: &Vec<Matchers>,
    ) -> RedisResult<AHashSet<RedisString>> {
        let mut keys: AHashSet<RedisString> = AHashSet::with_capacity(16); // todo: properly estimate
        for matcher in matchers {
            get_series_keys_by_matchers(ctx, matcher, &mut keys)?;
        }
        Ok(keys)
    }

    pub(crate) fn get_labels_by_key(&self, ctx: &RedisContext, key: &str) -> Option<&MetricName> {
        let mut labels_cache = self.labels_cache.read().unwrap();
        if let Some(metric_name) = labels_cache.get(key) {
            return Some(metric_name);
        }
        let rkey = ctx.create_string(key);
        if let Ok(m) = get_series_labels_as_metric_name(ctx, key) {
            let mut labels_cache = self.labels_cache.write().unwrap();
            labels_cache.insert(key.to_string(), m.clone());
            return labels_cache.get(key)
        }
        None
    }

    pub(crate) fn get_multi_labels_by_key<'a>(&self, ctx: &RedisContext, keys: AHashSet<RedisString>) -> AHashMap<RedisString, &MetricName> {
        let mut result = AHashMap::new();
        let mut labels_cache = self.labels_cache.write().unwrap();
        for key in keys {
            let key_str = key.to_string();
            if let Some(metric_name) = labels_cache.get(&key_str) {
                result.insert(key, metric_name);
            } else {
                if let Ok(m) = get_series_labels_as_metric_name(ctx, &key_str) {
                    labels_cache.insert(key.to_string(), m);
                    if let Some(m) = labels_cache.get(&key_str) {
                        result.insert(key, m);
                    }
                }
            }
        }
        result
    }
}

static LABEL_PARSING_ERROR: &str = "TSDB: failed parsing labels";

/// Attempt to convert a simple alternation regexes to a format that can be used with redis timeseries,
/// which does not support regexes for label filters.
/// see: https://redis.io/commands/ts.queryindex/
fn convert_regex(filter: &LabelFilter) -> RedisResult<String> {
    let op_str = if filter.op.is_negative() {
        "!="
    } else {
        "="
    };
    if filter.value.contains('|') {
        let alternates = get_or_values(&filter.value);
        if !alternates.is_empty() {
            let str = format!("{}{op_str}({})", filter.label, alternates.join("|"));
            return Ok(str);
        }
    } else if is_literal(&filter.value) {
        // see if we have a simple literal r.g. job~="packaging"
        let str = format!("{}{op_str}{}", filter.label, filter.value);
        return Ok(str);
    }
    Err(RedisError::Str(LABEL_PARSING_ERROR))
}

/// returns true if value has no regex meta-characters
fn is_literal(value: &str) -> bool {
    // use buffer here ?
    regex_syntax::escape(value) == value
}

pub fn convert_label_filter(filter: &LabelFilter) -> RedisResult<String> {
    use LabelFilterOp::*;

    match filter.op {
        Equal => {
            Ok(format!("{}={}", filter.label, filter.value))
        }
        NotEqual => {
            Ok(format!("{}!={}", filter.label, filter.value))
        }
        RegexEqual | RegexNotEqual => {
            convert_regex(filter)
        }
    }
}

pub(crate) fn matchers_to_query_args(
    matchers: &Matchers,
) -> RedisResult<Vec<String>> {
    matchers
        .iter()
        .map(|m| convert_label_filter(m))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn get_series_keys_by_matchers(
    ctx: &RedisContext,
    matchers: &Matchers,
    keys: &mut AHashSet<RedisString>
) -> RedisResult<()> {
    let args = matchers_to_query_args(matchers)?;
    let reply = call_redis_command(ctx, "TS.QUERYINDEX", args.as_slice())?;
    if let RedisValue::Array(mut values) = reply {
        for value in values.drain(..) {
            match value {
                RedisValue::BulkRedisString(s) => {
                    keys.insert(s);
                }
                RedisValue::BulkString(s) => {
                    let value = ctx.create_string(s);
                    keys.insert(value);
                }
                _ => {
                    return Err(RedisError::Str("TSDB: invalid TS.QUERYINDEX reply"));
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn get_series_keys_by_matchers_vec(
    ctx: &Context,
    matchers: &Vec<Matchers>,
) -> RedisResult<AHashSet<RedisString>> {
    let mut keys: AHashSet<RedisString> = AHashSet::with_capacity(16); // todo: properly estimate
    for matcher in matchers {
        get_series_keys_by_matchers(ctx, matcher, &mut keys)?;
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {}
