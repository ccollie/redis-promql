use crate::module::{get_series_keys_by_matchers_vec, ts_range};
use metricsql_engine::provider::MetricDataProvider;
use metricsql_engine::{
    Deadline, QueryResult, QueryResults, RuntimeResult, SearchQuery,
};
use redis_module::Context;
use crate::globals::get_timeseries_index;
use crate::index::TimeseriesIndex;

pub struct TsdbDataProvider {}

impl TsdbDataProvider {
    fn get_series_data(
        &self,
        ctx: &Context,
        index: &TimeseriesIndex,
        search_query: &SearchQuery,
    ) -> Vec<QueryResult> {
        let series_keys = get_series_keys_by_matchers_vec(ctx, &search_query.matchers).unwrap();
        let labels_map = index.get_multi_labels_by_key(ctx, series_keys);
        let series = labels_map.into_iter().map(|(key, labels)| {
            // todo: return Result
            let (timestamps, values) = ts_range(ctx, &key, search_query.start, search_query.end).unwrap();
            QueryResult::new(labels.as_ref().clone(), timestamps, values)
        }).collect::<Vec<_>>();

        series
    }
}

impl MetricDataProvider for TsdbDataProvider {
    fn search(&self, sq: &SearchQuery, _deadline: &Deadline) -> RuntimeResult<QueryResults> {
        // see: https://github.com/RedisLabsModules/redismodule-rs/blob/master/examples/call.rs#L144
        let ctx_guard = redis_module::MODULE_CONTEXT.lock();
        let index = get_timeseries_index();

        let data = self.get_series_data(&ctx_guard, index, sq);
        let result = QueryResults::new(data);
        Ok(result)
    }
}