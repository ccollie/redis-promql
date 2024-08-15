use crate::module::{parse_timestamp_arg, with_timeseries_mut};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

pub fn del_range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    with_timeseries_mut(ctx, &key, |series| {
        let from = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
        let to = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

        args.done()?;

        let start = from.to_timestamp();
        let end = to.to_timestamp();

        if start > end {
            return Err(RedisError::Str("ERR invalid range"));
        }

        let sample_count = series.remove_range(start, end)?;

        Ok(RedisValue::from(sample_count))
    })
}
