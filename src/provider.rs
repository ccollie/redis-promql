use crate::module::{ts_range};
use metricsql_engine::provider::MetricDataProvider;
use metricsql_engine::{
    Deadline, QueryResult, QueryResults, RuntimeResult, SearchQuery,
};
use redis_module::Context;
use crate::globals::get_timeseries_index;
use crate::index::{get_series_keys_by_matchers_vec, TimeseriesIndex};

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
            QueryResult::new(labels.clone(), timestamps, values)
        }).collect::<Vec<_>>();

        series
    }
}

impl MetricDataProvider for TsdbDataProvider {
    fn search(&self, sq: &SearchQuery, _deadline: &Deadline) -> RuntimeResult<QueryResults> {
        todo!()
        // let ctx = get_redis_context();
        // let index = get_timeseries_index();
        //
        // let data = self.get_series_data(ctx, index, sq);
        // let result = QueryResults::new(data);
        // Ok(result)
    }
}