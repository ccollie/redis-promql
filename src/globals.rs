use std::sync::{Arc, OnceLock};
use metricsql_engine::prelude::{Context as QueryContext};
use crate::index::TimeSeriesIndex;
use crate::provider::TsdbDataProvider;

static TIMESERIES_INDEX: OnceLock<TimeSeriesIndex> = OnceLock::new();
static QUERY_CONTEXT: OnceLock<QueryContext> = OnceLock::new();

pub fn get_query_context() -> &'static QueryContext {
    QUERY_CONTEXT.get_or_init(create_query_context)
}

fn create_query_context() -> QueryContext {
    // todo: read from config
    let provider = Arc::new(TsdbDataProvider{});
    let ctx = QueryContext::new();
    ctx.with_provider(provider)
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


pub fn get_timeseries_index() -> &'static TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_init(|| TimeSeriesIndex::new())
}