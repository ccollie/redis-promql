use crate::aggregators::AggOp;
use crate::common::types::{Sample, Timestamp};
use crate::iter::aggregator::AggrIterator;
use crate::module::types::{AggregationOptions, RangeGroupingOptions, RangeOptions, ValueFilter};
use crate::storage::time_series::TimeSeries;
use valkey_module::ValkeyValue;

pub(super) struct RangeMeta {
    pub id: u64,
    pub(crate) source_key: String,
    pub(crate) start_ts: Timestamp,
    pub(crate) end_ts: Timestamp,
    pub(crate) labels: Vec<ValkeyValue>,
    pub(crate) sample_iter: Box<dyn Iterator::<Item=Sample>>
}

pub struct GroupedSeries {
    pub label_value: String,
    pub series: Vec<RangeMeta>,
    pub labels: Vec<ValkeyValue>,
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

pub(super) fn get_sample_iterator<'a>(
    series: &'a TimeSeries,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    timestamp_filter: &Option<Vec<Timestamp>>,
    value_filter: &Option<ValueFilter>
) -> Box<dyn Iterator<Item=Sample> + 'a> {

    let (min, max, has_value_filter) = if let Some(filter) = value_filter {
        let ValueFilter { min, max } = *filter;
        (min, max, true)
    } else {
        (f64::MIN, f64::MAX, false)
    };

    if let Some(timestamps) = timestamp_filter {
        let iter = series.samples_by_timestamps(timestamps)
            .unwrap_or_else(|e| {
                // todo: properly handle error and log
                vec![]
            })
            .into_iter()
            .filter(move |sample| sample.timestamp >= start_timestamp && sample.timestamp <= end_timestamp);

        if has_value_filter {
            let iter = iter.filter(move |sample| {
                sample.value >= min && sample.value <= max
            });
            Box::new(iter)
        } else {
            Box::new(iter)
        }

    } else {
        let iter = series.range_iter(start_timestamp, end_timestamp);

        if has_value_filter {
            let iter = iter.filter(move |sample| sample.value >= min && sample.value <= max);
            Box::new(iter)
        } else {
            Box::new(iter)
        }

    }

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

pub(crate) fn aggregate_samples(
    iter: impl Iterator<Item=Sample>,
    start_ts: Timestamp,
    end_ts: Timestamp,
    aggr_options: &AggregationOptions,
    count: Option<usize>
) -> Vec<Sample> {
    let aligned_timestamp= aggr_options.alignment.get_aligned_timestamp(start_ts, end_ts);
    let mut aggr = AggrIterator::new(aggr_options, aligned_timestamp, count);
    aggr.calculate(iter)
}

pub fn get_range_series_labels(options: &RangeOptions, series: &TimeSeries) -> Vec<ValkeyValue>  {

    if !options.with_labels && options.selected_labels.is_empty() {
        return vec![]
    }

    let mut dest = Vec::new();

    fn create_label(name: &str, value: &str) -> ValkeyValue {
        ValkeyValue::Array(vec![
            ValkeyValue::SimpleString(name.to_string()),
            ValkeyValue::SimpleString(value.to_string())
        ])
    }

    if !options.selected_labels.is_empty() {
        for name in options.selected_labels.iter() {
            if let Some(value) = series.label_value(&name) {
                dest.push(create_label(&name, &value));
            }
        }
    } else {
        for label in series.labels.iter().by_ref() {
            dest.push(create_label(&label.name, &label.value));
        }
    }

    dest
}

pub(super) fn group_samples_internal(samples: impl Iterator<Item=Sample>, option: &RangeGroupingOptions) -> Vec<Sample> {
    let mut iter = samples;
    let mut aggregator = option.aggregator.clone();
    let mut current = if let Some(current) = iter.next() {
        aggregator.update(current.value);
        current
    } else {
        return vec![];
    };

    let mut result = vec![];

    while let Some(next) = iter.next() {
        if next.timestamp == current.timestamp {
            aggregator.update(next.value);
        } else {
            let value = aggregator.finalize();
            result.push(Sample { timestamp: current.timestamp, value });
            aggregator.update(next.value);
            current = next;
        }
    }

    // Finalize last
    let value = aggregator.finalize();
    result.push(Sample { timestamp: current.timestamp, value });

    result
}
