use std::time::Duration;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::aggregators::Aggregator;
use crate::module::parse_timestamp_arg;
use crate::module::result::sample_to_result;
use crate::ts::{get_timeseries_mut};

static CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
static CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
static CMD_ARG_ALIGN: &str = "ALIGN";
static CMD_ARG_COUNT: &str = "COUNT";
static CMD_ARG_AGGREGATION: &str = "AGGREGATION";
static CMD_ARG_MIN_VALUE: &str = "MIN_VALUE";
static CMD_ARG_MAX_VALUE: &str = "MAX_VALUE";
static CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";

pub struct AggregationOptions {
    pub(crate) aggregation: Aggregator,
    pub(crate) time_bucket: Duration,
    pub(crate) align_start_time: bool,
    pub(crate) align_end_time: bool,
    pub(crate) align: Option<Filter>,
}
pub struct RangeOptions {
    pub(crate) count: Option<usize>,
    pub(crate) aggregation: Option<AggregationOptions>,
    pub(crate) min_value: Option<f64>,
    pub(crate) max_value: Option<f64>,
    pub(crate) align_start_time: bool,
    pub(crate) align_end_time: bool,
    pub(crate) align: Option<Filter>,
}

pub fn range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    // Safety: passing 'true' as the third parameter ensures the key exists
    let series = get_timeseries_mut(ctx, &key, true)?.unwrap();

    let from = parse_timestamp_arg(ctx, args.next_str()?, "startTimestamp")?;
    let to = parse_timestamp_arg(ctx, args.next_str()?, "endTimestamp")?;

    args.done()?;

    let start = from.to_timestamp();
    let end = to.to_timestamp();

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    let samples = series.get_range(start, end)
        .map_err(|e| {
            // todo: log error
            ctx.log_warning(format!("ERR fetching range {:?}", e).as_str());
            RedisError::Str("ERR fetching range")
        })?;

    let result = samples.iter().map(|s| sample_to_result(s.timestamp, s.value)).collect();
    Ok(RedisValue::Array(result))
}

fn parse_value_filter_arg(ctx: &Context, args: &Vec<RedisString>) -> RedisResult<(Option<f64>, Option<f64>)> {
    let mut args = arg.splitn(2, '=');
    let index = args.find(|s| s.eq_ignore_ascii_case(CMD_ARG_VALUE))?;
    let key = args.next().ok_or_else(|| RedisError::Str("ERR invalid filter"))?;
    let value = args.next().ok_or_else(|| RedisError::Str("ERR invalid filter"))?;
    let value = parse_number(value).map_err(|_| RedisError::Str("ERR invalid filter"))?;
    Ok(Filter::new(key, value))
}