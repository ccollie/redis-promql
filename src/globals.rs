use crate::index::TimeSeriesIndex;
use crate::provider::TsdbDataProvider;
use ahash::HashMapExt;
use metricsql_runtime::prelude::Context as QueryContext;
use papaya::HashMap;
use redis_module::{raw, Context, RedisModule_GetSelectedDb};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::AtomicU64;

pub type TimeSeriesIndexMap = HashMap<u32, TimeSeriesIndex>;

static TIMESERIES_INDEX: OnceLock<TimeSeriesIndexMap> = OnceLock::new();
static QUERY_CONTEXT: OnceLock<QueryContext> = OnceLock::new();

pub fn get_query_context() -> &'static QueryContext {
    QUERY_CONTEXT.get_or_init(create_query_context)
}

fn create_query_context() -> QueryContext {
    // todo: read from config
    let provider = Arc::new(TsdbDataProvider{});
    let ctx = QueryContext::new();
    ctx.with_metric_storage(provider)
}

pub(crate) fn set_query_context(ctx: QueryContext) {
    match QUERY_CONTEXT.set(ctx) {
        Ok(_) => {}
        Err(_) => {
            // how to do this in redis context ?
            panic!("set query context failed");
        }
    }
}

pub unsafe fn get_current_db(ctx: *mut raw::RedisModuleCtx) -> u32 {
    let db = RedisModule_GetSelectedDb.unwrap()(ctx);
    db as u32
}

// todo: in on_load, we need to set this to the last id + 1
static TIMESERIES_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn next_timeseries_id() -> u64 {
    TIMESERIES_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub fn get_timeseries_index(ctx: &Context) -> &'static mut TimeSeriesIndex {
    let mut map = TIMESERIES_INDEX.get_or_init(|| TimeSeriesIndexMap::new());
    let db = unsafe { get_current_db(ctx.ctx) };
    let inner = map.pin();
    if let Some(index) = inner.get(&db) {
        index
    } else {
        *inner.get_or_insert(db, TimeSeriesIndex::new(), 1)
    }
}