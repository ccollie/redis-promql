use crate::common::types::{Timestamp, TimestampRangeValue};
use metricsql_parser::common::Matchers;
use redis_module::redisvalue::RedisValueKey;
use redis_module::{Context as RedisContext, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use std::collections::HashMap;
use std::hash::{Hasher};
use ahash::{AHashMap, AHashSet};
use xxhash_rust::xxh3;
use crate::common::parse_series_selector;
use crate::module::{call_redis_command, convert_label_filter, normalize_range_args, parse_timestamp_arg};
use crate::module::result::META_KEY_LABEL;

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub fn prom_series(ctx: &RedisContext, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, false, collect_series_metadata)
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub(crate) fn prom_label_names(ctx: &RedisContext, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, false, collect_label_names_metadata)
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn prom_label_values(ctx: &RedisContext, args: Vec<RedisString>) -> RedisResult {
    handle_series_metadata(ctx, args, true, collect_label_values_metadata)
}

struct MetadataFunctionArgs {
    label_name: Option<String>,
    start: Timestamp,
    end: Timestamp,
    matchers: Vec<Matchers>,
}


struct HandlerContext<'a> {
    key: &'a str,
    result: Vec<RedisValue>,
    uniques: AHashSet<u64>,
    hasher: xxh3::Xxh3,
}

impl<'a> HandlerContext<'a> {
    pub fn new() -> Self {
        Self {
            key: "",
            result: Vec::new(),
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

type MetaResultHandler =
    fn(&mut HandlerContext, &str, &str, &str);

fn collect_series_metadata(
    context: &mut HandlerContext,
    series: &str,
    label: &str,
    value: &str,
) {
    todo!()
}

fn collect_label_names_metadata(
    context: &mut HandlerContext,
    _series: &str,
    label: &str,
    _value: &str,
) {
    if context.insert_unique(label) {
        context.result.push(label.into());
    }
}

fn collect_label_values_metadata(
    context: &mut HandlerContext,
    _series: &str,
    _label: &str,
    value: &str,
) {
    if context.insert_unique(value) {
        context.result.push(value.into());
    }
}

fn handle_series_metadata(
    ctx: &RedisContext,
    args: Vec<RedisString>,
    needs_label: bool,
    collector: MetaResultHandler,
) -> RedisResult {
    let label_args = parse_metadata_command_args(ctx, args, true, needs_label)?;
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


static CMD_ARG_START: &str = "START";
static CMD_ARG_END: &str = "END";
static CMD_ARG_MATCH: &str = "MATCH";
static CMD_ARG_LABEL: &str = "LABEL";

fn parse_metadata_command_args(ctx: &RedisContext, args: Vec<RedisString>, require_matchers: bool, need_label: bool) -> RedisResult<MetadataFunctionArgs> {
    let mut args = args.into_iter().skip(1);
    let mut label_name = None;
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;

    if need_label {
        label_name = Some(args.next_str()?.to_string());
    }

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(ctx, &next, "START")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(ctx, &next, "END")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MATCH) => {
                while let Ok(matcher) = args.next_str() {
                    if let Ok(selector) = parse_series_selector(&matcher) {
                        matchers.push(selector);
                    } else {
                        return Err(RedisError::Str("ERR invalid MATCH series selector"));
                    }
                }
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    let (start, end) = normalize_range_args(start_value, end_value)?;

    if require_matchers && matchers.is_empty() {
        return Err(RedisError::Str("ERR at least 1 MATCH series selector required"));
    }

    if need_label && label_name.is_none() {
        return Err(RedisError::Str("ERR missing label name"));
    }

    Ok(MetadataFunctionArgs {
        label_name,
        start,
        end,
        matchers,
    })
}

fn get_labels_from_matchers(
    ctx: &RedisContext,
    matchers: &Matchers,
    start: Timestamp,
    end: Timestamp,
    handler_context: &mut HandlerContext,
    handler: MetaResultHandler,
) -> RedisResult<Vec<RedisValue>> {
    let capacity = 5 + matchers.len();  //  start end WITHLABELS LATEST FILTER ....
    // todo: use pool
    let mut args: Vec<String> = Vec::with_capacity(capacity);

    args.push(start.to_string());
    args.push(end.to_string());

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
                        labels_hash.insert(META_KEY_LABEL, key);
                        for i in (0..labels.len()).step_by(2) {
                            // should be 2 bulk string items
                            match (&labels[i], &labels[i + 1]) {
                                (RedisValue::BulkString(key), RedisValue::BulkString(value)) => {
                                    let label = key.as_str();
                                    let value = value.as_str();
                                    handler(handler_context, key, label, value);
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
