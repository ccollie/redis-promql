use redis_module::{Context, NotifyEvent};

fn notify_callback(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    match event {
        "del" | "set" | "expired" | "evict" | "evicted" | "trimmed" => {
            // unlink also notifies with del with freeseries called before
            // RemoveIndexedMetric(key);
        }
        "restore" => {
            // RestoreKey(ctx, key);
        }
        "rename_from" => {
            // RenameSeriesFrom(ctx, key);
        }
        "rename_to" => {
            // RenameSeriesTo(ctx, key);
        }
        "expire" => {
            // ExpireSeries(ctx, key);
        }
        "loaded" => {
            // Will be called in replicaof or on load rdb on load time
            // IndexMetricFromName(ctx, key);
        }
        "ts.create" => {
            // IndexMetricFromName(ctx, key);
        }
        "ts.alter" => {
            // IndexMetricFromName(ctx, key);
        }
        _ => {
            // ctx.log_warning(&format!("Unknown event: {}", event));
        }
    }
}


fn on_event(ctx: &Context, event_type: NotifyEvent, event: &str, key: &[u8]) {
    if key == b"num_sets" {
        // break infinite look
        return;
    }
    let msg = format!(
        "Received event: {:?} on key: {} via event: {}",
        event_type,
        std::str::from_utf8(key).unwrap(),
        event
    );
    ctx.log_notice(msg.as_str());
    let _ = ctx.add_post_notification_job(|ctx| {
        // it is not safe to write inside the notification callback itself.
        // So we perform the write on a post job notificaiton.
        if let Err(e) = ctx.call("incr", &["num_sets"]) {
            ctx.log_warning(&format!("Error on incr command, {}.", e));
        }
    });
}

//  REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
//  REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED |
//  REDISMODULE_NOTIFY_TRIMMED,