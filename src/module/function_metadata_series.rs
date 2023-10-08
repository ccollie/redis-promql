use std::collections::{HashMap, HashSet};
use metricsql_parser::common::Matchers;
use redis_module::{CallReply, CallResult, Context, RedisResult, RedisString, RedisValue};
use redis_module::redisvalue::RedisValueKey;
use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::globals::get_timeseries_index;
use crate::index::{get_series_keys_by_matchers_vec, matchers_to_query_args, RedisContext};
use crate::module::function_metadata::parse_metadata_command_args;
use crate::module::result::metric_name_to_redis_value;

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub(crate) fn series(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let label_args = parse_metadata_command_args(args, true)?;
    let index = get_timeseries_index();

    let data = if label_args.start.is_none() && label_args.end.is_none() {
        // use quick path. query index by labels
        let series_keys = get_series_keys_by_matchers_vec(ctx, &label_args.matchers)?;
        let res = index.get_multi_labels_by_key(ctx, series_keys);

        let mut values = vec![];
        for (key, labels) in res {
            let key_str = key.try_as_str()?;
            let metric = metric_name_to_redis_value(labels, Some(key_str));
            values.push(metric);
        }
        values
    } else {
        let mut filter_args = matchers_to_query_args(ctx, &label_args.matchers);

        let values: RedisValue = series
            .into_iter()
            .map(get_ts_metric_selector)
            .collect::<Vec<_>>()
            .into();

        vec![]
    };

    let map: HashMap<RedisValueKey, RedisValue> = [
        (RedisValueKey::String("status".into()), RedisValue::SimpleStringStatic("success")),
        (RedisValueKey::String("data".into()), data.into())
    ].into_iter().collect();

    Ok(RedisValue::Map(map))
}

fn get_labels_from_matchers(ctx: &RedisContext,
                            matchers: &Matchers,
                            start: Option<Timestamp>,
                            end: Option<Timestamp>) -> RedisResult<()> {
    let mut args = matchers_to_query_args(ctx, matchers)?;
    args.push("WITHLABELS".into());
    args.push("LATEST".into());

    let (start, end) = normalize_range_timestamps(start, end);

    args.insert(0, start.to_redis_string(ctx));
    args.insert(1, end.to_redis_string(ctx));

    // https://redis.io/commands/ts.mrange/
    // search for the section "Filter query by value"
    let reply = ctx.call("TS.MRANGE", args.as_slice())?;
    if let CallReply::Array(arr) = reply {
        for (i, val) in arr.iter().enumerate() {
            if let RedisValue::Array(arr) = val {
                let key = arr[0].try_as_str()?;

                if i % 2 == 0 {
                    result.timestamps.push(value.to_i64());
                } else {
                    result.values.push(value.to_double());
                }
            }
        }
    }
    Ok(result)
}

fn process_mrange_item(item: &CallReply) {
    // should be sn array of arrays

    // first element is the key
    match item {
        Ok(value) => {
            if i % 2 == 0 {
                result.timestamps.push(value.to_i64());
            } else {
                result.values.push(value.to_double());
            }
        },
        Err(e) => {
            return Err(e.into());
        }
    }
}

pub(crate) fn normalize_range_timestamps(start: Option<Timestamp>, end: Option<Timestamp>) -> (TimestampRangeValue, TimestampRangeValue) {
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (TimestampRangeValue::Value(start), TimestampRangeValue::Latest),
        (None, Some(end)) => (TimestampRangeValue::Earliest, end.into()),
        (None, None) => (TimestampRangeValue::Earliest, TimestampRangeValue::Latest),
    }
}