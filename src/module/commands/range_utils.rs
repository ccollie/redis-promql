use super::aggregator::AggrIterator;
use crate::common::types::{Sample, Timestamp};
use crate::module::types::{AggregationOptions, RangeOptions, ValueFilter};
use crate::storage::time_series::TimeSeries;
use ahash::AHashMap;
use valkey_module::ValkeyValue;

pub struct GroupMeta<'a> {
    pub groups: AHashMap<String, Vec<&'a TimeSeries>>,
}

impl<'a> GroupMeta<'a> {
    pub fn new() -> Self {
        Self {
            groups: Default::default(),
        }
    }

    pub fn add_series(&mut self, series: &'a TimeSeries, label_name: &str) {
        if let Some(value) = series.label_value(label_name) {
            match self.groups.get_mut(value) {
                Some(group) => {
                    group.push(series)
                }
                None => {
                    let mut group = Vec::with_capacity(8);
                    group.push(series);
                    self.groups.insert(value.to_string(), group);
                }
            }
        }
    }
}

// todo: move elsewhere, better name
pub(super) fn get_range_internal(
    series: &TimeSeries,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    timestamp_filter: &Option<Vec<Timestamp>>,
    value_filter: &Option<ValueFilter>
) -> Vec<Sample> {
    let mut samples = if let Some(timestamps) = timestamp_filter {
        series.samples_by_timestamps(timestamps)
            .unwrap_or_else(|e| {
                // todo: properly handle error and log
                vec![]
            })
            .into_iter()
            .filter(|sample| sample.timestamp >= start_timestamp && sample.timestamp <= end_timestamp)
            .collect()
    } else {
        series.range_iter(start_timestamp, end_timestamp).collect::<Vec<_>>()
    };

    if let Some(filter) = value_filter {
        samples.retain(|s| s.value >= filter.min && s.value <= filter.max);
    };

    samples
}

pub(crate) fn get_range(series: &TimeSeries, args: &RangeOptions, check_retention: bool) -> Vec<Sample> {
    let (start_timestamp, end_timestamp) = args.date_range.get_series_range(series, check_retention);
    let mut range = get_range_internal(
                                   series,
                                   start_timestamp,
                                   end_timestamp,
                                   &args.timestamp_filter,
                                   &args.value_filter);

    if let Some(aggr_options) = &args.aggregation {
        let mut aggr_iterator = get_series_aggregator(series, args, aggr_options, check_retention);
        aggr_iterator.calculate(range.into_iter())
    } else {
        if let Some(count) = args.count {
            range.truncate(count);
        }
        range
    }
    // group by
}

fn get_series_aggregator(
    series: &TimeSeries,
    args: &RangeOptions,
    aggr_options: &AggregationOptions,
    check_retention: bool
) -> AggrIterator {
    let (start_timestamp, end_timestamp) = args.date_range.get_series_range(series, check_retention);
    let aligned_timestamp= aggr_options.alignment.get_aligned_timestamp(start_timestamp, end_timestamp);

    AggrIterator::new(aggr_options, aligned_timestamp, args.count)
}

pub fn group_series_by_label_value<'a>(series: &'a [&TimeSeries], label: &str) -> AHashMap<String, Vec<&'a TimeSeries>> {
    let mut grouped: AHashMap<String, Vec<&TimeSeries>> = AHashMap::new();

    for ts in series.iter() {
        if let Some(label_value) = ts.label_value(label) {
            if let Some(list) = grouped.get_mut(label_value) {
                list.push(ts);
            } else {
                grouped.insert(label_value.to_string(), vec![ts]);
            }
        }
    }

    grouped
}

pub(crate) fn sample_to_value(sample: Sample) -> ValkeyValue {
    let row = vec![ValkeyValue::from(sample.timestamp), ValkeyValue::from(sample.value)];
    ValkeyValue::from(row)
}