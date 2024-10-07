use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Matchers, Sample, TaggedSample, Timestamp};
use crate::globals::with_timeseries_index;
use crate::module::arg_parse::*;
use crate::module::commands::range_utils::get_series_labels;
use crate::module::result::sample_to_value;
use crate::module::types::TimestampRange;
use crate::module::{get_series_iterator, get_timeseries};
use crate::storage::time_series::TimeSeries;
use ahash::HashMapExt;
use metricsql_common::hash::IntMap;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const REDUCER_KEY: &str = "__reducer__";
const SOURCE_KEY: &str = "__source__";

struct CollateOptions {
    date_range: TimestampRange,
    matchers: Vec<Matchers>,
    with_labels: bool,
    selected_labels: Vec<String>,
    count: Option<usize>,
    aggregator: Option<Aggregator>,
}

/// VM.COLLATE fromTimestamp toTimestamp FILTER filter...
/// [COUNT count]
/// [WITHLABELS]
/// [SELECTED_LABELS label...]
/// [AGGREGATION aggregator]
pub(crate) fn collate(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_collate_options(&mut args)?;
    handle_collate(ctx, options)
}

struct SeriesMeta {
    id: u64,
    key: ValkeyString,
    labels: Vec<ValkeyValue>,
}

type PerTimestampData = BTreeMap<Timestamp, IntMap<u64, f64>>;
type SampleWithId = TaggedSample<u64>;

fn handle_collate(ctx: &Context, options: CollateOptions) -> ValkeyResult {

    with_timeseries_index(ctx, move |index| {
        let keys = index.series_keys_by_matchers(ctx, &options.matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("VM: ERR no series found"));
        }

        let mut metas: Vec<SeriesMeta> = Vec::with_capacity(keys.len());
        let mut all_samples: Vec<SampleWithId> = Vec::with_capacity(keys.len() * 10);

        for key in keys {
            if let Some(series) = get_timeseries(ctx, &key, false)? {
                let iter = get_series_iterator(series, options.date_range, &None, &None);
                let samples = iter.collect::<Vec<_>>();
                let meta = get_series_meta(key, series, &options);
                metas.push(meta);
                all_samples.extend(samples);
            }
        }

        metas.sort_by(|x, y| x.key.cmp(&y.key));

        all_samples.sort_by(|x, y| {
            let cmp = x.sample.timestamp.cmp(&y.sample.timestamp);
            if cmp == Ordering::Equal {
                x.tag.cmp(&y.tag)
            } else {
                cmp
            }
        });

        let count = options.count.unwrap_or(usize::MAX);
        let series_data = collate_data(all_samples);

        let value = if let Some(aggr) = options.aggregator {
            let mut aggregator = aggr;
            get_aggregation_output(metas, series_data, &mut aggregator, count)
        } else {
            get_base_output(metas, series_data, count)
        };

        Ok(value)
    })
}

fn get_base_output(metas: Vec<SeriesMeta>, data: PerTimestampData, count: usize) -> ValkeyValue {
    let mut count = count;

    let mut series_data_map: IntMap<u64, Vec<ValkeyValue>> = IntMap::with_capacity(metas.len());

    for (ts, series_data) in data.into_iter() {
        for meta in metas.iter() {
            let ts_value = ValkeyValue::from(ts);
            let series_value = if let Some(val) = series_data.get(&meta.id) {
                ValkeyValue::from(*val)
            } else {
                ValkeyValue::Null
            };
            series_data_map
                .entry(meta.id)
                .or_default()
                .push(ValkeyValue::Array(vec![ts_value, series_value]));
        }
        count -= 1;
        if count == 0 {
            break;
        }
    }

    let mut result: BTreeMap<ValkeyValueKey, ValkeyValue> = BTreeMap::new();
    for meta in metas.into_iter() {
        if let Some(values) = series_data_map.remove(&meta.id) {
            // values is samples ordered by timestamp
            result.insert(
                ValkeyValueKey::from(meta.key),
                ValkeyValue::Array(vec![ValkeyValue::from(meta.labels), ValkeyValue::from(values)])
            );
        }
    }

    ValkeyValue::from(result)
}

fn get_aggregation_output(metas: Vec<SeriesMeta>,
                          data: PerTimestampData,
                          aggregator: &mut Aggregator,
                          count: usize) -> ValkeyValue {
    let mut metas = metas;

    let capacity = metas.iter()
        .map(|meta| meta.key.len())
        .sum() + metas.len() - 1;

    let mut sources = String::with_capacity(capacity);

    for (i, meta) in metas.iter().enumerate() {
        sources.push_str(&meta.key.to_string());
        if i < metas.len() - 1 {
            sources.push(',');
        }
    }

    let labels: Vec<ValkeyValue> = vec![
        ValkeyValue::Array(vec![ValkeyValue::from(REDUCER_KEY), ValkeyValue::from(aggregator.name())]),
        ValkeyValue::Array(vec![ValkeyValue::from(SOURCE_KEY), ValkeyValue::from(sources)]),
    ];

    let samples = calculate_aggregates(data, aggregator, count);

    let values = samples.iter().map(sample_to_value);
    ValkeyValue::Array(vec![ValkeyValue::from(labels), ValkeyValue::from(values)])
}


fn parse_collate_options(args: &mut CommandArgIterator) -> ValkeyResult<CollateOptions> {

    let date_range = parse_timestamp_range(args)?;

    let mut options = CollateOptions {
        date_range,
        count: None,
        aggregator: None,
        with_labels: false,
        matchers: vec![],
        selected_labels: vec![],
    };

    fn is_command_keyword(arg: &str) -> bool {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
            arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => true,
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER) => true,
            arg if arg.eq_ignore_ascii_case(CMD_ARG_WITH_LABELS) => true,
            arg if arg.eq_ignore_ascii_case(CMD_ARG_SELECTED_LABELS) => true,
            _ => false,
        }
    }

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
            CMD_ARG_FILTER => {
                options.matchers = parse_series_selector_list(args, is_command_keyword)?;
            }
            CMD_ARG_AGGREGATION => {
                let agg_str = args.next_str()
                    .map_err(|_e| ValkeyError::Str("TSDB: Error parsing AGGREGATION"))?;
                let aggregator = Aggregator::try_from(agg_str)?;
                options.aggregator = Some(aggregator);
            }
            CMD_ARG_COUNT => {
                options.count = Some(parse_count(args)?);
            }
            CMD_ARG_WITH_LABELS => {
                options.with_labels = true;
            }
            CMD_ARG_SELECTED_LABELS => {
                options.selected_labels = parse_label_list(args, is_command_keyword)?;
            }
            _ => {}
        }
    }

    if options.matchers.is_empty() {
        return Err(ValkeyError::Str("ERR no FILTER given"));
    }

    Ok(options)
}


fn get_series_meta(key: ValkeyString, series: &TimeSeries, options: &CollateOptions) -> SeriesMeta {
    let is_aggregation = options.aggregator.is_some();

    let labels = if is_aggregation {
        get_series_labels(series, options.with_labels, &options.selected_labels)
    } else {
        vec![]
    };

    SeriesMeta {
        id: series.id,
        key,
        labels,
    }
}

fn collate_data(samples: Vec<SampleWithId>) -> PerTimestampData {

    let mut result: PerTimestampData = BTreeMap::new();

    for tagged in samples.iter() {
        let entry = result.entry(tagged.sample.timestamp).or_insert_with(IntMap::new);

        *entry.entry(tagged.tag).or_insert(tagged.sample.value);
    }

    result
}

fn calculate_aggregates(data: PerTimestampData, aggregator: &mut Aggregator, count: usize) -> Vec<Sample> {
    let mut result: Vec<Sample> = Vec::with_capacity(data.len());

    for (timestamp, sample_data) in data.iter() {
        for (_series_id, value) in sample_data.iter() {
            aggregator.update(*value);
        }
        let value = aggregator.finalize();
        aggregator.reset();
        result.push(Sample { timestamp: *timestamp, value: *value });
        if result.len() == count {
            break;
        }
    }

    result
}