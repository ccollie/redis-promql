use metricsql_engine::MetricName;
use metricsql_parser::prelude::Matchers;
use redis_module::{Context, RedisResult};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use ahash::{AHashMap, AHashSet};
use dynamic_lru_cache::DynamicCache;
use crate::module::{get_series_keys_by_matchers, get_series_labels_as_metric_name};

pub type RedisContext = Context;

// todo: make configurable
static LABEL_CACHE_MAX_SIZE: usize = 100;

/// Index for quick access to timeseries by label, label value or metric name.
pub struct TimeseriesIndex {
    group_sequence: AtomicU64,
    /// Cache of metric names. Stored in raw form since it's used in various contexts
    labels_cache: DynamicCache<String, MetricName>,
}

impl TimeseriesIndex {
    pub fn new() -> TimeseriesIndex {
        let labels_cache: DynamicCache<String, MetricName> = DynamicCache::new(LABEL_CACHE_MAX_SIZE);
        TimeseriesIndex {
            group_sequence: AtomicU64::new(0),
            labels_cache
        }
    }

    pub fn clear(&self) {
        self.group_sequence.store(0, Ordering::Relaxed);
    }

    pub(crate) fn remove_labels_by_key(&self, key: &String) {
        self.labels_cache.pop(key);
    }

    pub(crate) fn series_keys_by_matchers<'a>(
        &'a self,
        ctx: &'a Context,
        matchers: &Vec<Matchers>,
    ) -> RedisResult<AHashSet<String>> {
        let mut keys: AHashSet<String> = AHashSet::with_capacity(16); // todo: properly estimate
        for matcher in matchers {
            get_series_keys_by_matchers(ctx, matcher, &mut keys)?;
        }
        Ok(keys)
    }

    pub(crate) fn get_labels_by_key(&self, ctx: &RedisContext, key: &String) -> Option<Arc<MetricName>> {
        if let Some(metric_name) = self.labels_cache.get(key) {
            return Some(metric_name);
        }
        if let Ok(m) = get_series_labels_as_metric_name(ctx, key) {
            self.labels_cache.insert(key, m.clone());
            return self.labels_cache.get(key)
        }
        None
    }

    pub(crate) fn get_multi_labels_by_key<'a>(&self, ctx: &RedisContext, keys: AHashSet<String>) -> AHashMap<String, Arc<MetricName>> {
        let mut result = AHashMap::new();
        for key in keys {
            if let Some(metric_name) = self.labels_cache.get(&key) {
                // todo: do we need to clone ????
                result.insert(key, metric_name);
            } else {
                if let Ok(m) = get_series_labels_as_metric_name(ctx, &key) {
                    self.labels_cache.insert(&key, m);
                    if let Some(m) = self.labels_cache.get(&key) {
                        result.insert(key, m);
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {}
