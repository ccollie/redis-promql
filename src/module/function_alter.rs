use redis_module::{Context, NotifyEvent, REDIS_OK, RedisResult, RedisString};
use crate::globals::get_timeseries_index;
use crate::module::commands::parse_create_options;
use crate::module::get_timeseries_mut;
use crate::storage::Label;

pub fn alter(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let (parsed_key, options) = parse_create_options(args)?;

    let mut series = get_timeseries_mut(ctx, &parsed_key, true)?.unwrap();

    if let Some(retention) = options.retention {
        series.retention = retention;
    }

    series.dedupe_interval = options.dedupe_interval;

    if let Some(duplicate_policy) = options.duplicate_policy {
        series.duplicate_policy = duplicate_policy;
    }

    if let Some(chunk_size) = options.chunk_size {
        series.chunk_size_bytes = chunk_size;
    }

    let mut labels_changed = false;
    if let Some(labels) = options.labels {
        for (k,v) in labels.iter() {
            series.labels.push( Label{
                name: k.to_string(),
                value: v.to_string(),
            });
            labels_changed = true;
        }
    }

    if labels_changed {
        let ts_index = get_timeseries_index(ctx);
        ts_index.reindex_timeseries(&mut series, &parsed_key);
    }

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.ALTER", &parsed_key);

    REDIS_OK
}
