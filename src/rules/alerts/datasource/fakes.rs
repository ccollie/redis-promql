use std::sync::Mutex;
use crate::common::types::Timestamp;
use crate::rules::alerts::{AlertsError, AlertsResult, Querier, QueryResult};
use super::{Metric};

struct QuerierInner {
    metrics: Vec<Metric>,
    ts_key: String,
    err: Option<AlertsError>,
}

pub struct FakeQuerier {
    inner: Mutex<QuerierInner>
}

impl FakeQuerier {
    pub fn new(metrics: Vec<Metric>, ts_key: String, err: Option<AlertsError>) -> Self {
        let inner = QuerierInner {
            metrics,
            ts_key,
            err,
        };
        Self { inner: Mutex::new(inner) }
    }

    pub fn add_metrics(&mut self, metrics: Vec<Metric>) {
        let mut inner = self.inner.lock().unwrap();
        inner.metrics.extend(metrics);
    }

    pub fn set_err(&mut self, err: Option<AlertsError>) {
        let inner = self.inner.lock().unwrap();
        inner.err = err;
    }

    pub fn reset(&mut self) {
        let inner = self.inner.lock().unwrap();
        inner.metrics.clear();
        inner.err = None;
    }
}

impl Querier for FakeQuerier {
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<QueryResult> {
        let inner = self.inner.lock().unwrap();
        if let Some(err) = &inner.err {
            return Err(err.clone());
        }
        let mut result = QueryResult::default();
        todo!()
    }

    fn query_range(&self, query: &str, from: Timestamp, to: Timestamp) -> AlertsResult<QueryResult> {
        todo!()
    }
}
