use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::index::{convert_label_filter, RedisContext};
use crate::module::function_metadata::parse_metadata_command_args;
use metricsql_parser::common::Matchers;
use redis_module::redisvalue::RedisValueKey;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::collections::HashMap;
use std::hash::{Hasher};
use ahash::{AHashMap, AHashSet};
use xxhash_rust::xxh3;
use crate::module::call_redis_command;

struct HandlerContext {
    result: Vec<RedisValue>,
    uniqs: AHashSet<String>,
    uniques: AHashSet<u64>,
    hasher: xxh3::Xxh3,
}

impl HandlerContext {
    pub fn new() -> Self {
        Self {
            result: Vec::new(),
            uniqs: Default::default(),
            uniques: AHashSet::new(),
            hasher: xxh3::Xxh3::with_seed(0),
        }
    }

    fn insert_unique(&mut self, key: &str) -> bool {
        self.hasher.reset();
        self.hasher.write(key.as_bytes());
        let hash = self.hasher.digest();
        self.uniques.insert(hash)
    }
} 

pub(crate) type MetaResultHandler =
    fn(&mut HandlerContext, &str, &str, &str) -> RedisResult;

pub(crate) fn collect_series_metadata(
    context: &mut HandlerContext,
    series: &str,
    label: &str,
    value: &str,
) -> RedisResult<()> {
    todo!()
}

pub(crate) fn collect_label_names_metadata(
    context: &mut HandlerContext,
    _series: &str,
    label: &str,
    _value: &str,
) -> RedisResult<()> {
    if context.insert_unique(label) {
        context.result.push(label.into());
    }
    Ok(())
}

pub(crate) fn collect_label_values_metadata(
    context: &mut HandlerContext,
    _series: &str,
    _label: &str,
    value: &str,
) -> RedisResult<()> {
    if context.insert_unique(value) {
        context.result.push(value.into());
    }
    Ok(())
}

fn handle_series_metadata(
    ctx: &RedisContext,
    args: Vec<RedisString>,
    needs_label: bool,
    collector: MetaResultHandler,
) -> RedisResult {
    let label_args = parse_metadata_command_args(args, true, needs_label)?;
    let mut handler_context = HandlerContext::new();

    for matchers in label_args.matchers.iter() {
        get_labels_from_matchers(
            ctx,
            matchers,
            label_args.start,
            label_args.end,
            &mut handler_context,
            collector,
        )?;
    }

    let map: HashMap<RedisValueKey, RedisValue> = [
        (
            RedisValueKey::String("status".into()),
            RedisValue::SimpleStringStatic("success"),
        ),
       // (RedisValueKey::String("data".into()), data.into()),
    ]
    .into_iter()
    .collect();

    Ok(RedisValue::Map(map))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub(crate) fn series(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, false, collect_series_metadata)
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub(crate) fn label_names(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, false, collect_label_names_metadata)
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn label_values(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, true, collect_label_values_metadata)
}

fn get_labels_from_matchers(
    ctx: &RedisContext,
    matchers: &Matchers,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    handler_context: &mut HandlerContext,
    handler: MetaResultHandler,
) -> RedisResult<Vec<RedisValue>> {
    let capacity = 5 + matchers.len();  //  start end WITHLABELS LATEST FILTER ....
    // todo: use pool
    let mut args: Vec<String> = Vec::with_capacity(capacity);
    let (start, end) = normalize_range_timestamps(start, end);

    args.push(start.to_string());
    args.push(end.to_string());

    // let mut args = matchers_to_query_args(ctx, matchers)?;
    args.push("WITHLABELS".to_string());
    args.push("LATEST".to_string());
    args.push("FILTER".to_string());

    for matcher in matchers.iter() {
        let filter = convert_label_filter(matcher)?;
        args.push(filter);
    }

    // https://redis.io/commands/ts.mrange/
    // search for the section "Filter query by value"
    let reply = call_redis_command(ctx, "TS.MRANGE", args.as_slice())?;
    if let RedisValue::Array(arr) = reply {
        let mut labels_hash: AHashMap<&str, &str> = AHashMap::new();
        for val in arr.iter() {
            if let RedisValue::Array(arr) = val {
                if arr.len() < 2 {
                    // ???
                    ctx.log_warning("TSDB: invalid TS.MRANGE reply");
                    continue;
                }
                match (&arr[0], &arr[1]) {
                    (RedisValue::BulkString(key), RedisValue::Array(labels)) => {
                        let key = key.as_str();
                        labels_hash.insert("__meta:key__", key);
                        for i in (0..labels.len()).step_by(2) {
                            // should be 2 bulk string items
                            match (&labels[i], &labels[i + 1]) {
                                (RedisValue::BulkString(key), RedisValue::BulkString(value)) => {
                                    let label = key.as_str();
                                    let value = value.as_str();
                                    handler(handler_context, key, label, value)?;
                                }
                                _ => break,
                            }
                        }
                    }
                    _ => {
                        ctx.log_warning("TSDB: invalid TS.MRANGE reply");
                    }
                }
            }
        }
    }
    Err(RedisError::Str("TSDB: invalid TS.MRANGE reply"))
}

pub(crate) fn normalize_range_timestamps(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> (TimestampRangeValue, TimestampRangeValue) {
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (
            TimestampRangeValue::Value(start),
            TimestampRangeValue::Latest,
        ),
        (None, Some(end)) => (TimestampRangeValue::Earliest, end.into()),
        (None, None) => (TimestampRangeValue::Earliest, TimestampRangeValue::Latest),
    }
}
