use crate::common::types::Timestamp;
use crate::module::REDIS_PROMQL_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use crate::storage::Label;
use ahash::AHashSet;
use metricsql_common::hash::IntMap;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use metricsql_runtime::METRIC_NAME_LABEL;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use redis_module::{Context, RedisError, RedisString};
use roaring::{MultiOps, RoaringTreemap};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

pub type RedisContext = Context;


/// A type mapping a label name to a bitmap of ids of timeseries having that label.
/// Note that we use `BtreeMap` specifically for it's `range()` method, so a regular HashMap won't work.
pub type LabelsBitmap = BTreeMap<String, RoaringTreemap>;

/// Index for quick access to timeseries by label, label value or metric name.
/// TODO: do we need to have one per db ?
#[derive(Default)]
pub(crate) struct TimeSeriesIndex {
    /// Map from timeseries id to timeseries key.
    id_to_key: IntMap<u64, RedisString>,
    /// Map from label name to set of timeseries ids.
    label_to_ts: LabelsBitmap,
    /// Map from label name + label value to set of timeseries ids.
    label_kv_to_ts: LabelsBitmap,
    series_sequence: AtomicU64,
}

impl TimeSeriesIndex {
    pub fn new() -> TimeSeriesIndex {
        TimeSeriesIndex {
            id_to_key: Default::default(),
            label_to_ts: Default::default(),
            label_kv_to_ts: Default::default(),
            series_sequence: AtomicU64::new(1),
        }
    }

    pub fn clear(&mut self) {
        self.id_to_key.clear();
        self.label_to_ts.clear();
        self.label_kv_to_ts.clear();

        // we use Relaxed here since we only need uniqueness, not monotonicity
        self.series_sequence.store(0, Ordering::Relaxed);
    }

    pub fn label_count(&self) -> usize {
        self.label_to_ts.len()
    }
    pub fn series_count(&self) -> usize {
        self.id_to_key.len()
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.series_sequence.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn index_time_series(&mut self, ts: &TimeSeries, key: &RedisString) {
        debug_assert!(ts.id != 0);

        self.id_to_key.insert(ts.id, key.clone());

        if !ts.metric_name.is_empty() {
            index_series_by_label_internal(
                &mut self.label_to_ts,
                &mut self.label_kv_to_ts,
                ts.id,
                METRIC_NAME_LABEL,
                &ts.metric_name,
            );
        }

        for Label { name, value } in ts.labels.iter() {
            index_series_by_label_internal(
                &mut self.label_to_ts,
                &mut self.label_kv_to_ts,
                ts.id,
                &name,
                &value,
            );
        }
    }

    pub fn reindex_timeseries(&mut self, ts: &TimeSeries, key: &RedisString) {
        // todo: may cause race ?
        self.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        self.index_time_series(ts, key);
    }

    pub(crate) fn remove_series(&mut self, ts: &TimeSeries) {
        self.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        self.id_to_key.remove(&ts.id);
    }

    fn remove_series_by_id(&mut self, id: u64, metric_name: &str, labels: &[Label]) {
        self.id_to_key.remove(&id);
        let should_delete = !metric_name.is_empty() || !labels.is_empty();
        if !should_delete {
            return;
        }

        let mut to_delete = Vec::with_capacity(labels.len() + 1);
        // todo: borrow from label
        {
            if !metric_name.is_empty() {
                if let Some(ts_by_label) = self.label_to_ts.get_mut(METRIC_NAME_LABEL) {
                    ts_by_label.remove(id);
                    if ts_by_label.is_empty() {
                        to_delete.push(METRIC_NAME_LABEL.to_string());
                    }
                }
            }
            for Label { name, .. } in labels.iter() {
                if let Some(ts_by_label) = self.label_to_ts.get_mut(name) {
                    ts_by_label.remove(id);
                    if ts_by_label.is_empty() {
                        to_delete.push(name.to_string());
                    }
                }
            }
            if !to_delete.is_empty() {
                for key in &to_delete {
                    self.label_to_ts.remove(key);
                }
            }
            to_delete.clear();
        }
        {
            if !metric_name.is_empty() {
                let key = format!("{}={}", METRIC_NAME_LABEL, metric_name);
                if let Some(ts_by_label) = self.label_kv_to_ts.get_mut(&key) {
                    ts_by_label.remove(id);
                    if ts_by_label.is_empty() {
                        to_delete.push(key);
                    }
                }
            }
            for Label { name, value} in labels.iter() {
                let key = format!("{}={}", name, value);
                if let Some(ts_by_label_value) = self.label_kv_to_ts.get_mut(&key) {
                    ts_by_label_value.remove(id);
                    if ts_by_label_value.is_empty() {
                        to_delete.push(key);
                    }
                }
            }
            if !to_delete.is_empty() {
                for key in to_delete {
                    self.label_kv_to_ts.remove(&key);
                }
            }
        }
    }

    fn index_series_by_metric_name(&mut self, ts_id: u64, metric_name: &str) {
        self.index_series_by_label(ts_id, METRIC_NAME_LABEL, metric_name);
    }

    fn index_series_by_label(&mut self, ts_id: u64, label: &str, value: &str) {
        index_series_by_label_internal(&mut self.label_to_ts, &mut self.label_kv_to_ts, ts_id, label, value)
    }

    fn index_series_by_labels(&mut self, ts_id: u64, labels: &Vec<Label>) {
        for Label { name, value} in labels.iter() {
            self.index_series_by_label(ts_id, name, value)
        }
    }

    pub(crate) fn remove_series_by_key(&mut self, ctx: &Context, key: &RedisString) -> bool {
        // get series by key
        if let Some(ts) = get_timeseries(ctx, key) {
            self.remove_series(ts);
            return true;
        }
        false
    }

    pub fn get_series_by_id(
        &self,
        ctx: &Context,
        id: u64,
    ) -> Result<Option<&mut TimeSeries>, RedisError> {
        get_series_by_id(ctx, &self.id_to_key, id)
    }

    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> RoaringTreemap {
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(ts_ids) = self.label_kv_to_ts.get(&key) {
            return ts_ids.clone();
        }
        RoaringTreemap::new()
    }

    pub(crate) fn get_series_by_metric_name(
        &self,
        ctx: &Context,
        metric: &str,
        res: &mut Vec<&TimeSeries>,
    ) {
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(ts_ids) = self.label_kv_to_ts.get(&key) {
            let iter = ts_ids
                .iter()
                .map(|id| get_series_by_id(ctx, &self.id_to_key, id));

            res.extend(iter);
        }
    }

    pub fn rename_series(&mut self, ctx: &Context, new_key: &RedisString) -> bool {
        // get ts by new key
        if let Some(series) = get_timeseries(ctx, new_key) {
            let id = series.id;
            self.id_to_key.insert(id, new_key.clone());
            return true;
        }
        false
    }

    /// Return a bitmap of series ids that have the given label and pass the filter `predicate`.
    pub(crate) fn get_label_value_bitmap(
        &self,
        label: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> RoaringTreemap {
        get_label_value_bitmap(&self.label_kv_to_ts, label, predicate)
    }

    /// Returns a list of all values for the given label
    pub(crate) fn get_label_values(
        &self,
        label: &str,
    ) -> AHashSet<String> {
        let prefix = format!("{label}=");
        let suffix = format!("{label}={}", char::MAX);
        self.label_kv_to_ts
            .range(prefix..suffix)
            .flat_map(|(key, _)| {
                if let Some((_, value)) = key.split_once('=') {
                    Some(value.to_string())
                } else {
                    None
                }
            }).into_iter().collect()
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_by_matchers(
        &self,
        ctx: &Context,
        matchers: &[Matchers],
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<&TimeSeries> {
        // todo: if we get a None from get_series_by_id, we should log an error
        // and remove the id from the index
        matchers
            .par_iter()
            .map(|filter| find_ids_by_matchers(&self.label_kv_to_ts, filter))
            .collect::<Vec<_>>()
            .intersection()
            .into_iter()
            .filter_map(|id| get_series_by_id(ctx, &self.id_to_key, id))
            .filter(|ts| ts.overlaps(start, end))
            .collect()
    }

    /// Returns a list of all label names used by series matching `matchers` and having samples in
    /// the range [`start`, `end`]
    pub(crate) fn labels_by_matchers<'a>(
        &'a self,
        ctx: &'a Context,
        matchers: &[Matchers],
        start: Timestamp,
        end: Timestamp,
    ) -> BTreeSet<&'a String> {
        let series = self.series_by_matchers(ctx, matchers, start, end);
        let mut result: BTreeSet<&'a String> = BTreeSet::new();

        for ts in series {
            result.extend(ts.labels.iter().map(|f| &f.name));
        }

        result
    }

    pub(crate) fn find_ids_by_matchers(&self, matchers: &Matchers) -> RoaringTreemap {
        find_ids_by_matchers(&self.label_kv_to_ts, matchers)
    }
}

fn get_label_value_bitmap(
    label_kv_to_ts: &LabelsBitmap,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) -> RoaringTreemap {
    let prefix = format!("{label}=");
    let suffix = format!("{label}=\u{10ffff}");
    label_kv_to_ts
        .range(prefix..suffix)
        .flat_map(|(key, map)| {
            if let Some((_, value)) = key.split_once('=') {
                return if predicate(&value) {
                    Some(map)
                } else {
                    None
                };
            }
            None
        })
        .collect::<Vec<_>>()
        .union()
}

fn index_series_by_label_internal(
    label_to_ts: &mut LabelsBitmap,
    label_kv_to_ts: &mut LabelsBitmap,
    ts_id: u64,
    label: &str,
    value: &str,
) {
    let ts_by_label = label_to_ts
        .entry(label.to_owned())
        .or_insert_with(|| RoaringTreemap::new());

    ts_by_label.insert(ts_id);

    let ts_by_label_value = label_kv_to_ts
        .entry(format!("{}={}", label, value))
        .or_insert_with(|| RoaringTreemap::new());

    ts_by_label_value.insert(ts_id);
}

#[inline]
fn get_series_by_id<'a>(
    ctx: &'a Context,
    id_to_key: &IntMap<u64, RedisString>,
    id: u64,
) -> Option<&'a TimeSeries> {
    if let Some(key) = id_to_key.get(&id) {
        return get_timeseries(ctx, key);
    }
    None
}

fn get_multi_series_by_id<'a>(
    ctx: &'a Context,
    id_to_key: &IntMap<u64, RedisString>,
    ids: &[u64],
) -> Result<Vec<Option<&'a TimeSeries>>, RedisError> {
    ids.iter()
        .map(|id| get_series_by_id(ctx, id_to_key, *id))
        .collect()
}

fn find_ids_by_label_filter(
    label_kv_to_ts: &LabelsBitmap,
    filter: &LabelFilter,
) -> RoaringTreemap {
    use LabelFilterOp::*;

    match filter.op {
        Equal => {
            unreachable!("Equal should be handled by find_ids_by_matchers")
        }
        NotEqual => {
            let predicate = |value: &str| value != filter.value;
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
        RegexEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| regex.is_match(value);
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
        RegexNotEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| !regex.is_match(value);
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
    }
}

const MAX_EQUAL_MAP_SIZE: usize = 5;

fn find_ids_by_multiple_filters<'a>(
    label_kv_to_ts: &'a LabelsBitmap,
    filters: &Vec<LabelFilter>,
    bitmaps: &mut Vec<RoaringTreemap>,
    key_buf: &mut String,
) {
    //bitmaps.clear();

    let mut equal_bitmap: RoaringTreemap = RoaringTreemap::new();
    let mut has_equal = false;
    for filter in filters.iter() {
        // perform a more efficient lookup for label=value
        if filter.op == LabelFilterOp::Equal {
            // according to https://github.com/rust-lang/rust/blob/1.47.0/library/alloc/src/string.rs#L2414-L2427
            // write! will not return an Err, so the unwrap is safe
            write!(key_buf, "{}={}", filter.label, filter.value).unwrap();
            if let Some(map) = label_kv_to_ts.get(&key_buf.to_string()) {
                equal_bitmap &= map;
                has_equal = true;
            }
            key_buf.clear();
        } else {
            let map = find_ids_by_label_filter(label_kv_to_ts, filter);
            bitmaps.push(map);
        }
    }
    if has_equal {
        bitmaps.push(equal_bitmap);
    }
}

fn find_ids_by_matchers(
    label_kv_to_ts: &LabelsBitmap,
    matchers: &Matchers,
) -> RoaringTreemap {
    let mut bitmaps: Vec<RoaringTreemap> = Vec::new();
    let mut key_buf = String::with_capacity(64);

    let mut or_bitmap: Option<RoaringTreemap> = None;
    {
        if !matchers.or_matchers.is_empty() {
            for filter in matchers.or_matchers.iter() {
                find_ids_by_multiple_filters(label_kv_to_ts, filter, &mut bitmaps, &mut key_buf);
            }
            or_bitmap = Some(bitmaps.union());
        }
    }

    if !matchers.matchers.is_empty() {
        find_ids_by_multiple_filters(label_kv_to_ts, &matchers.matchers, &mut bitmaps, &mut key_buf);
        if let Some(or_bitmap) = or_bitmap {
            bitmaps.push(or_bitmap);
        }
    }

    bitmaps.intersection()
}

fn get_timeseries<'a>(ctx: &'a Context, key: &RedisString) -> Option<&'a TimeSeries> {
    let redis_key = ctx.open_key(key);
    match redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE) {
        Ok(res) => res,
        Err(e) => {
            let msg = format!("Failed to get timeseries: {:?}", e);
            ctx.log_warning(msg.as_str());
            None
        },
    }
}

#[cfg(test)]
mod tests {}
