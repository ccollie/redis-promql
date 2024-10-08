#![feature(lazy_cell)]
extern crate async_trait;
extern crate croaring;
extern crate get_size;
extern crate smallvec;
extern crate valkey_module_macros;

use valkey_module::server_events::{FlushSubevent, LoadingSubevent};
use valkey_module::{valkey_module, Context as ValkeyContext, NotifyEvent, ValkeyString};
use valkey_module_macros::{config_changed_event_handler, flush_event_handler, loading_event_handler};

mod aggregators;
mod common;
mod config;
mod error;
mod globals;
mod index;
mod module;
mod provider;
mod storage;

#[cfg(test)]
mod tests;
mod gorilla;

use crate::globals::{clear_timeseries_index, with_timeseries_index};
use crate::index::reset_timeseries_id_after_load;
use crate::storage::time_series::TimeSeries;
use module::*;

pub const VKMETRICS_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "VKMetrics";
pub const MODULE_TYPE: &str = "vkmetrics";

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &ValkeyContext, _changed_configs: &[&str]) {
    ctx.log_notice("config changed")
}

#[flush_event_handler]
fn flushed_event_handler(_ctx: &ValkeyContext, flush_event: FlushSubevent) {
    if let FlushSubevent::Ended = flush_event {
        clear_timeseries_index();
    }
}

#[loading_event_handler]
fn loading_event_handler(_ctx: &ValkeyContext, values: LoadingSubevent) {
    match values {
        LoadingSubevent::ReplStarted |
        LoadingSubevent::AofStarted => {
            // TODO!: limit to current db
            clear_timeseries_index();
        }
        LoadingSubevent::Ended => {
            reset_timeseries_id_after_load();
        }
        _ => {}
    }
}

fn remove_key_from_index(ctx: &ValkeyContext, key: &[u8]) {
    with_timeseries_index(ctx, |ts_index| {
        let key: ValkeyString = ctx.create_string(key);
        ts_index.remove_series_by_key(ctx, &key);
    });
}

fn index_timeseries_by_key(ctx: &ValkeyContext, key: &[u8]) {
    with_timeseries_index(ctx, |ts_index| {
        let _key: ValkeyString = ctx.create_string(key);
        let redis_key = ctx.open_key_writable(&_key);
        let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE);
        if let Ok(Some(series)) = series {
            if ts_index.is_series_indexed(series.id) {
                // todo: log warning
                ts_index.remove_series_by_key(ctx, &_key);
                return;
            }
            ts_index.index_time_series(series, key);
        }
    });
}

fn on_event(ctx: &ValkeyContext, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    // todo: AddPostNotificationJob(ctx, event, key);
    match event {
        "del" | "set" | "expired" | "evict" | "evicted" | "expire" | "trimmed" => {
            remove_key_from_index(ctx, key);
        }
        "loaded" => {
            index_timeseries_by_key(ctx, key);
        }
        "rename_from" => {
            // RenameSeriesFrom(ctx, key);
        }
        "storage.alter" => remove_key_from_index(ctx, key),
        _ => {
            // ctx.log_warning(&format!("Unknown event: {}", event));
        }
    }
}

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        valkey_module::alloc::ValkeyAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

valkey_module! {
    name: MODULE_NAME,
    version: VKMETRICS_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VKM_SERIES_TYPE],
    commands: [
        ["VKM.CREATE-SERIES", commands::create, "write deny-oom", 1, 1, 1],
        ["VKM.ALTER-SERIES", commands::alter, "write deny-oom", 1, 1, 1],
        ["VKM.ADD", commands::add, "write deny-oom", 1, 1, 1],
        ["VKM.GET", commands::get, "write deny-oom", 1, 1, 1],
        ["VKM.SERIES-INFO", commands::info, "write deny-oom", 1, 1, 1],
        ["VKM.MADD", commands::madd, "write deny-oom", 1, 1, 1],
        ["VKM.DELETE-KEY_RANGE", commands::delete_key_range, "write deny-oom", 1, 1, 1],
        ["VKM.DELETE-RANGE", commands::delete_range, "write deny-oom", 1, 1, 1],
        ["VKM.DELETE-SERIES", commands::delete_series, "write deny-oom", 1, 1, 1],
        ["VKM.QUERY", commands::query, "write deny-oom", 1, 1, 1],
        ["VKM.QUERY-RANGE", commands::query_range, "write deny-oom", 1, 1, 1],
        ["VKM.RANGE", commands::range, "write deny-oom", 1, 1, 1],
        ["VKM.SERIES", commands::series, "write deny-oom", 1, 1, 1],
        ["VKM.TOP-QUERIES", commands::top_queries, "write deny-oom", 1, 1, 1],
        ["VKM.ACTIVE-QUERIES", commands::active_queries, "write deny-oom", 1, 1, 1],
        ["VKM.CARDINALITY", commands::cardinality, "write deny-oom", 1, 1, 1],
        ["VKM.LABEL-NAMES", commands::label_names, "write deny-oom", 1, 1, 1],
        ["VKM.LABEL-VALUES", commands::label_values, "write deny-oom", 1, 1, 1],
        ["VKM.STATS", commands::stats, "write deny-oom", 1, 1, 1],
        ["VKM.RESET-ROLLUP-CACHE", commands::reset_rollup_cache, "write deny-oom", 1, 1, 1],
    ],
     event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED @TRIMMED: on_event]
    ],
}
