use crate::module::with_timeseries_mut;
use redis_module::{Context, NextArg, RedisResult, RedisString, RedisValue};

pub(crate) fn get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    with_timeseries_mut(ctx, &key, |series| {
        args.done()?;

        let result = if series.is_empty() {
            vec![]
        } else {
            vec![RedisValue::from(series.last_timestamp), RedisValue::from(series.last_value)]
        };

        Ok(RedisValue::Array(result))
    })
}
