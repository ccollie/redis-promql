use crate::globals::with_timeseries_index;
use crate::iter::MultiSeriesSampleIter;
use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::{get_range_series_labels, get_sample_iterator, GroupedSeries, RangeMeta};
use crate::module::types::{RangeGroupingOptions, RangeOptions};
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use ahash::{AHashMap};
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries_index(ctx, move |index| {
        let matchers = std::mem::take(&mut options.series_selector);
        let keys = index.series_keys_by_matchers(ctx, &[matchers]);

        let mut series = Vec::with_capacity(keys.len());
        let mut metas = Vec::with_capacity(keys.len());
        for key in keys.iter() {
            let db_key = ctx.open_key(key);
            if let Some(ts) = db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                let meta = create_range_meta(key, &options, ts)?;
                metas.push(meta);
                series.push(ts);
            } else {
                // todo error
            }
        }

        if let Some(opts) = options.grouping {
            let grouped_samples = group_series_by_label_value(series, metas, &opts);

        }

        Ok(ValkeyValue::from("OK"))
    })
}

fn create_range_meta(key: &ValkeyString, options: &RangeOptions, series: &TimeSeries) -> ValkeyResult<RangeMeta> {
    let (start_ts, end_ts) = options.date_range.get_series_range(series, true);

    let sample_iter = get_sample_iterator(series,
                                          start_ts,
                                          end_ts,
                                          &options.timestamp_filter,
                                          &options.value_filter);

    let labels= get_range_series_labels(options, series);
    Ok(RangeMeta {
        id: series.id,
        source_key: key.to_string(),
        start_ts,
        end_ts,
        labels,
        sample_iter
    })
}

const REDUCER_KEY: &str = "__reducer__";
const SOURCE_KEY: &str = "__source__";

fn group_series_by_label_value(
    series: Vec<&TimeSeries>,
    metas: Vec<RangeMeta>,
    grouping: &RangeGroupingOptions) -> Vec<GroupedSeries> {
    let mut grouped: AHashMap<String, Vec<RangeMeta>> = AHashMap::new();
    let label = &grouping.group_label;

    for (ts, meta) in series.into_iter().zip(metas.into_iter()) {
        if let Some(label_value) = ts.label_value(label) {
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

fn process_group(
    label: &str,
    group: GroupedSeries,
    metas: Vec<RangeMeta>,
    grouping: &RangeGroupingOptions,
    options: &RangeOptions) -> ValkeyResult {
    let reducer = grouping.aggregator.name();

    let mut iterators = Vec::with_capacity(metas.len());
    let mut source_keys = Vec::with_capacity(metas.len());

    for meta in metas.into_iter() {
        source_keys.push(meta.source_key.to_string());
        iterators.push(meta.sample_iter);
    }

    let multi_iter = MultiSeriesSampleIter::new(iterators);


    VALKEY_OK
}