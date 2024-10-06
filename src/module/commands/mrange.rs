use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Sample, Timestamp};
use crate::globals::with_timeseries_index;
use crate::iter::MultiSeriesSampleIter;
use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::{aggregate_samples, get_sample_iterator, get_series_labels, group_samples_internal};
use crate::module::result::sample_to_value;
use crate::module::types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::{SeriesSampleIterator, TimeSeries};
use ahash::AHashMap;
use rayon::iter::IntoParallelRefIterator;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

struct SeriesMeta<'a> {
    index: usize,
    series: &'a TimeSeries,
    source_key: ValkeyString,
    start_ts: Timestamp,
    end_ts: Timestamp,
}

// TODO: rayon as appropriate

pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries_index(ctx, move |index| {
        let matchers = std::mem::take(&mut options.series_selector);
        let keys = index.series_keys_by_matchers(ctx, &[matchers]);

        // needed to keep valkey keys alive below
        let db_keys = keys
            .iter()
            .map(|key| ctx.open_key(key))
            .collect::<Vec<_>>();

        let mut metas = Vec::with_capacity(keys.len());

        let mut idx: usize = 0;
        for (db_key, key) in db_keys.iter().zip(keys.into_iter()) {
            if let Some(ts) = db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                let meta = create_range_meta(key, idx, &options, ts);
                idx += 1;
                metas.push(meta);
            } else {
                // todo error
            }
        }

        let result_rows = process_command(metas, &options);
        let result = result_rows.into_iter()
            .map(result_row_to_value)
            .collect::<Vec<_>>();

        Ok(ValkeyValue::from(result))
    })
}

struct ResultRow {
    key: ValkeyValue,
    labels: Vec<ValkeyValue>,
    samples: Vec<Sample>,
}

// calculate the max and min timestamps
fn calculate_timestamp_range<'a>(metas: &'a Vec<SeriesMeta<'a>>) -> (Timestamp, Timestamp) {
    let mut start_ts = i64::MAX;
    let mut end_ts = i64::MIN;
    for meta in metas {
        start_ts = start_ts.min(meta.start_ts);
        end_ts = end_ts.max(meta.end_ts);
    }
    (start_ts, end_ts)
}

fn process_command(metas: Vec<SeriesMeta>, options: &RangeOptions) -> Vec<ResultRow> {
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, options, groupings, aggr_options)
        }
        (Some(groupings), None) => {
            // group raw samples
            handle_grouping(metas, options, groupings)
        }
        (None, Some(aggr_options)) => {
            handle_aggregation(metas, options, aggr_options)
        }
        (None, None) => {
            handle_raw(metas, options)
        }
    }
}

fn handle_aggregation_and_grouping<'a>(metas: Vec<SeriesMeta<'a>>,
                                       options: &RangeOptions,
                                       groupings: &RangeGroupingOptions,
                                       aggregations: &AggregationOptions) -> Vec<ResultRow> {
    let grouped_series = group_series_by_label(metas, groupings);

    grouped_series.into_iter()
        .map(|group| {
            let group_key = format!("{}={}", groupings.group_label, group.label_value);
            let key = ValkeyValue::from(group_key);
            let aggregator = aggregations.aggregator.clone();
            let aggregates = aggregate_grouped_samples(&group, &options, aggregator);
            let samples = group_samples_internal(aggregates.into_iter(), groupings);
            ResultRow {
                key,
                labels: group.labels,
                samples
            }
        }).collect::<Vec<_>>()
}

fn handle_grouping<'a>(metas: Vec<SeriesMeta<'a>>, options: &RangeOptions, grouping: &RangeGroupingOptions) -> Vec<ResultRow> {
    // group raw samples
    let grouped_series = group_series_by_label(metas, grouping);
    grouped_series.into_iter()
        .map(|group| {
            // todo: we need to account for the fact that valkey strings are binary safe,
            // we should probably restrict labels to utf-8 on construction
            let group_key = format!("{}={}", grouping.group_label, group.label_value);
            let key = ValkeyValue::from(group_key);
            let samples = get_grouped_raw_samples(&group.series, &options, &grouping);
            ResultRow {
                key,
                labels: group.labels,
                samples
            }
        }).collect::<Vec<_>>()
}

fn handle_aggregation<'a>(metas: Vec<SeriesMeta<'a>>, options: &RangeOptions, aggregation: &AggregationOptions) -> Vec<ResultRow> {
    let (start_ts, end_ts) = calculate_timestamp_range(&metas);
    let data = get_raw_sample_aggregates(&metas, start_ts, end_ts, &options, aggregation);
    data.into_iter().zip(metas.into_iter())
        .map(|(samples, meta) | {
            let labels = get_series_labels(
                meta.series,
                options.with_labels,
                &options.selected_labels
            );
            ResultRow {
                key: ValkeyValue::from(meta.source_key),
                labels,
                samples
            }
        }).collect::<Vec<_>>()
}

fn handle_raw(metas: Vec<SeriesMeta>, options: &RangeOptions) -> Vec<ResultRow> {
    let mut iterators = get_sample_iterators(&metas, &options);
    // todo: maybe rayon
    iterators.iter_mut().zip(metas.into_iter())
        .map(|(iter, meta)| {
            let labels = get_series_labels(
                meta.series,
                options.with_labels,
                &options.selected_labels
            );
            let samples = iter.collect::<Vec<Sample>>();
            ResultRow {
                key: ValkeyValue::from(meta.source_key),
                labels,
                samples
            }
        })
        .collect::<Vec<_>>()
}

fn result_row_to_value(row: ResultRow) -> ValkeyValue {
    let samples: Vec<_> = row.samples.into_iter().map(sample_to_value).collect();
    ValkeyValue::Array(vec![row.key, ValkeyValue::from(row.labels), ValkeyValue::from(samples)])
}

fn get_grouped_raw_samples<'a>(series: &Vec<SeriesMeta<'a>>,
                               options: &RangeOptions,
                               grouping_options: &RangeGroupingOptions) -> Vec<Sample> {
    let iterators = get_sample_iterators(series, options);
    let multi_iter = MultiSeriesSampleIter::new(iterators);
    group_samples_internal(multi_iter, grouping_options)
}


fn aggregate_grouped_samples(group: &GroupedSeries, options: &RangeOptions, aggr: Aggregator) -> Vec<Sample> {
    let mut aggregator = aggr;

    fn update(aggr: &mut Aggregator, sample: &Sample) -> bool {
        if sample.value.is_nan() {
            false
        } else {
            aggr.update(sample.value);
            true
        }
    }

    let mut count = options.count.unwrap_or(usize::MAX);
    let iterators = get_sample_iterators(&group.series, options);
    let mut iter = MultiSeriesSampleIter::new(iterators);

    let mut res: Vec<Sample> = Vec::with_capacity(iter.size_hint().0);

    let mut sample: Sample = Sample::default();
    let mut is_nan = false;

    if let Some(first) = iter.next() {
        sample = first;
        if !update(&mut aggregator, &sample) {
            is_nan = true
        }
    } else {
        return res;
    }

    for next_sample in iter {
        if sample.timestamp == next_sample.timestamp {
            if !update(&mut aggregator, &next_sample) {
                is_nan = true;
            }
        } else {
            is_nan = false;
            res.push(Sample {
                timestamp: sample.timestamp,
                value: if is_nan { f64::NAN } else { aggregator.finalize() },
            });

            count -= 1;
            if count == 0 {
                break;
            }

            aggregator.reset();
            sample = next_sample;
        }
    }

    res
}

fn get_series_iterator<'a>(meta: &SeriesMeta<'a>, options: &'a RangeOptions) -> SeriesSampleIterator<'a> {
    get_sample_iterator(meta.series,
                        meta.start_ts,
                        meta.end_ts,
                        &options.timestamp_filter,
                        &options.value_filter)
}

fn create_range_meta<'a>(source_key: ValkeyString, index: usize, options: &'a RangeOptions, series: &'a TimeSeries) -> SeriesMeta<'a> {
    let (start_ts, end_ts) = options.date_range.get_series_range(series, false);
    SeriesMeta {
        index,
        series,
        source_key,
        start_ts,
        end_ts,
    }
}

fn get_sample_iterators<'a>(series: &Vec<SeriesMeta<'a>>, range_options: &'a RangeOptions) -> Vec<SeriesSampleIterator<'a>> {
    series.iter().map(|meta| get_series_iterator(meta, range_options)).collect::<Vec<_>>()
}

fn get_raw_sample_aggregates<'a>(series: &Vec<SeriesMeta<'a>>,
                                 start_ts: Timestamp,
                                 end_ts: Timestamp,
                                 range_options: &RangeOptions,
                                 aggregation_options: &AggregationOptions) -> Vec<Vec<Sample>> {
    // todo: rayon
    series.iter().map(|meta| {
        get_series_sample_aggregates(meta, start_ts, end_ts, range_options, aggregation_options)
    }).collect::<Vec<_>>()
}

fn get_series_sample_aggregates<'a>(series: &SeriesMeta<'a>,
                                 start_ts: Timestamp,
                                 end_ts: Timestamp,
                                 range_options: &RangeOptions,
                                 aggregation_options: &AggregationOptions) -> Vec<Sample> {
    let iter = get_series_iterator(series, range_options);
    aggregate_samples(iter, start_ts, end_ts, aggregation_options, range_options.count)
}

const REDUCER_KEY: &str = "__reducer__";
const SOURCE_KEY: &str = "__source__";

pub struct GroupedSeries<'a> {
    pub label_value: String,
    pub series: Vec<SeriesMeta<'a>>,
    pub labels: Vec<ValkeyValue>,
}

fn group_series_by_label<'a>(
    metas: Vec<SeriesMeta<'a>>,
    grouping: &RangeGroupingOptions) -> Vec<GroupedSeries<'a>> {
    let mut grouped: AHashMap<String, Vec<SeriesMeta>> = AHashMap::new();
    let label = &grouping.group_label;

    for meta in metas.into_iter() {
        if let Some(label_value) = meta.series.label_value(label) {
            if let Some(list) = grouped.get_mut(label_value) {
                list.push(meta);
            } else {
                grouped.insert(label_value.to_string(), vec![meta]);
            }
        }
    }

    let reducer = grouping.aggregator.name();

    grouped.into_iter().map(|(label_value, series)| {
        let capacity = series.iter().map(|x| x.source_key.len()).sum::<usize>() + series.len();
        let mut sources: String = String::with_capacity(capacity);
        for (i, meta) in series.iter().enumerate() {
            sources.push_str(&meta.source_key.to_string());
            if i < series.len() - 1 {
                sources.push(',');
            }
        }
        let labels: Vec<ValkeyValue> = vec![
            ValkeyValue::Array(vec![ValkeyValue::from(label), ValkeyValue::from(label_value.clone())]),
            ValkeyValue::Array(vec![ValkeyValue::from(REDUCER_KEY), ValkeyValue::from(reducer)]),
            ValkeyValue::Array(vec![ValkeyValue::from(SOURCE_KEY), ValkeyValue::from(sources)]),
        ];
        GroupedSeries {
            label_value,
            series,
            labels,
        }
    }).collect()
}

