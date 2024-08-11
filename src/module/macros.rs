
macro_rules! get_series {
    ($ctx:expr, $key:expr, $must_exist:expr) => {
        let redis_key = $ctx.open_key($key);
        let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
        if $must_exist && result.is_none() {
            return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
        }
        Ok(result)
    };
}

macro_rules! get_series_mut {
    ($ctx:expr, $key:expr) => {
        let redis_key = $ctx.open_key_writeable($key);
        let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
        if result.is_none() {
            return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
        }
        Ok(result)
    };
}
