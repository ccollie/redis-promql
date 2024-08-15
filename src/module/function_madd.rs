use crate::arg_parse::parse_timestamp;
use crate::module::with_timeseries_mut;
use crate::storage::Timestamp;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

pub(crate) fn madd(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let arg_count = args.len() - 1;
    let mut args = args.into_iter().skip(1);

    if arg_count < 3 {
        return Err(RedisError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(RedisError::Str("ERR TSDB: wrong number of arguments for 'TS.MADD' command"));
    }

    let sample_count = arg_count / 3;

    let mut values: Vec<RedisValue> = Vec::with_capacity(sample_count);
    let mut inputs: Vec<(RedisString, Timestamp, f64)> = Vec::with_capacity(sample_count);

    while let Some(key) = args.next() {
        let timestamp = parse_timestamp(args.next_str()?)?;
        let value = args.next_f64()?;
        inputs.push((key, timestamp, value));
    }

    for (key, timestamp, value) in inputs {
        let value = with_timeseries_mut(ctx, &key, |series| {
            if series.add(timestamp, value, None).is_ok() {
                Ok(RedisValue::from(timestamp))
            } else {
                // todo !!!!!
                Ok(RedisValue::SimpleString("ERR".to_string()))
            }
        });
        match value {
            Ok(value) => values.push(value),
            Err(err) => values.push(
                RedisValue::SimpleString(format!("ERR TSDB: {}", err.to_string())),
            ),
        }
    }

    Ok(RedisValue::Array(values))

}
