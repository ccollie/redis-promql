use ahash::AHashSet;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use redis_module::{Context as RedisContext, RedisError, RedisResult, RedisValue};
use crate::common::regex_util::get_or_values;
use crate::module::call_redis_command;

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

pub(crate) fn convert_label_filter(filter: &LabelFilter) -> RedisResult<String> {
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
    keys: &mut AHashSet<String>
) -> RedisResult<()> {
    let args = matchers_to_query_args(matchers)?;
    let reply = call_redis_command(ctx, "TS.QUERYINDEX", args.as_slice())?;
    if let RedisValue::Array(mut values) = reply {
        for value in values.drain(..) {
            match value {
                RedisValue::BulkRedisString(s) => {
                    keys.insert(s.to_string_lossy());
                }
                RedisValue::BulkString(s) => {
                    keys.insert(s);
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
    ctx: &RedisContext,
    matchers: &Vec<Matchers>,
) -> RedisResult<AHashSet<String>> {
    let mut keys: AHashSet<String> = AHashSet::with_capacity(16); // todo: properly estimate
    for matcher in matchers {
        get_series_keys_by_matchers(ctx, matcher, &mut keys)?;
    }
    Ok(keys)
}