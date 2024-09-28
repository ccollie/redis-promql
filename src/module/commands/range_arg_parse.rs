use std::iter::Skip;
use std::vec::IntoIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};
use crate::aggregators::Aggregator;
use crate::common::types::Timestamp;
use crate::module::arg_parse::{
    parse_duration_arg,
    parse_integer_arg,
    parse_number_with_unit,
    parse_series_selector,
    parse_timestamp
};
use crate::module::parse_timestamp_arg;
use crate::module::types::{
    AggregationOptions,
    BucketTimestamp,
    RangeAlignment,
    RangeGroupingOptions,
    RangeOptions
};

const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const CMD_ARG_WITH_LABELS: &str = "WITH_LABELS";
const CMD_ARG_SELECTED_LABELS: &str = "SELECTED_LABELS";
const CMD_ARG_GROUP_BY: &str = "GROUPBY";
const CMD_PARAM_REDUCER: &str = "REDUCE";
const CMD_ARG_FILTER: &str = "FILTER";
const MAX_TS_VALUES_FILTER: usize = 25;

pub fn parse_range_options(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<RangeOptions> {

    let start = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
    let end = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

    let mut options = RangeOptions {
        start,
        end,
        alignment: None,
        count: None,
        aggregation: None,
        filter: None,
        latest: false,
        with_labels: false,
        series_selector: Default::default(),
        selected_labels: Default::default(),
        grouping: None
    };

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
            CMD_ARG_ALIGN => {
                let next = args.next_str()?;
                options.alignment = Some(parse_alignment(next)?);
            }
            CMD_ARG_FILTER => {
                let filter = args.next_str()?;
                options.series_selector = parse_series_selector(filter)?;
            }
            CMD_ARG_FILTER_BY_VALUE => {
                let min = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter min parameter"))?;
                let max = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter max parameter"))?;
                options.set_value_range(min, max)?;
            }
            CMD_ARG_FILTER_BY_TS => {
                options.set_valid_timestamps(parse_timestamp_filter(args)?);
            }
            CMD_ARG_GROUP_BY => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CMD_ARG_AGGREGATION => {
                options.aggregation = Some(parse_aggregation_args(args)?);
            }
            CMD_ARG_COUNT => {
                let next = args.next_arg()?;
                let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
                    .map_err(|_| ValkeyError::Str("TSDB: COUNT must be a positive integer"))?;
                if count > usize::MAX as i64 {
                    return Err(ValkeyError::Str("TSDB: COUNT value is too large"));
                }
                options.count = Some(count as usize);
            }
            CMD_ARG_WITH_LABELS => {
                options.with_labels = true;
            }
            CMD_ARG_SELECTED_LABELS => {
                while let Ok(label) = args.next_str() {
                    if is_range_command_keyword(label) {
                        break;
                    }
                    if options.selected_labels.contains(label) {
                        return Err(ValkeyError::Str("TSDB: Duplicate label found"));
                    }
                    options.selected_labels.insert(label.to_string());
                }
            }
            _ => {}
        }
    }

    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("ERR no FILTER given"));
    }

    Ok(options)
}

fn parse_alignment(align: &str) -> ValkeyResult<RangeAlignment> {
    let alignment = match align {
        arg if arg.eq_ignore_ascii_case("start") => RangeAlignment::Start,
        arg if arg.eq_ignore_ascii_case("end") => RangeAlignment::End,
        arg if arg.len() == 1 => {
            let c = arg.chars().next().unwrap();
            match c {
                '-' => RangeAlignment::Start,
                '+' => RangeAlignment::End,
                _ => return Err(ValkeyError::Str("TSDB: unknown ALIGN parameter")),
            }
        }
        _ => {
            let timestamp = parse_timestamp(align)
                .map_err(|_| ValkeyError::Str("TSDB: unknown ALIGN parameter"))?;
            RangeAlignment::Timestamp(timestamp)
        }
    };
    Ok(alignment)
}

fn is_range_command_keyword(arg: &str) -> bool {
    match arg {
        arg if arg.eq_ignore_ascii_case(CMD_ARG_ALIGN) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_GROUP_BY) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_WITH_LABELS) => true,
        _ => false,
    }
}

fn parse_timestamp_filter(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<Vec<Timestamp>> {
    let mut values: Vec<Timestamp> = Vec::new();
    while let Ok(arg) = args.next_str() {
        if is_range_command_keyword(arg) {
            break;
        }
        if let Ok(timestamp) = parse_timestamp(arg) {
            values.push(timestamp);
        } else  {
            return Err(ValkeyError::Str("TSDB: cannot parse timestamp"));
        }
        if values.len() == MAX_TS_VALUES_FILTER {
            break
        }
    }
    if values.is_empty() {
        return Err(ValkeyError::Str("TSDB: FILTER_BY_TS one or more arguments are missing"));
    }
    values.sort();
    values.dedup();
    Ok(values)
}

fn parse_grouping_params(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;
    let token = args.next_str()
        .map_err(|_| ValkeyError::Str("TSDB: missing REDUCE"))?;
    if !token.eq_ignore_ascii_case(CMD_PARAM_REDUCER) {
        let msg = format!("TSDB: expected \"{CMD_PARAM_REDUCER}\", found \"{token}\"");
        return Err(ValkeyError::String(msg));
    }
    let agg_str = args.next_str()
        .map_err(|_e| ValkeyError::Str("TSDB: Error parsing grouping reducer"))?;

    let aggregator = Aggregator::try_from(agg_str)
        .map_err(|_| {
            let msg = format!("TSDB: invalid grouping aggregator \"{}\"", agg_str);
            ValkeyError::String(msg)
        })?;

    Ok(
        RangeGroupingOptions {
            group_label: label.to_string(),
            aggregator
        }
    )
}

fn parse_aggregation_args(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args.next_str()
        .map_err(|_e| ValkeyError::Str("TSDB: Error parsing AGGREGATION"))?;
    let aggregator = Aggregator::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregator,
        bucket_duration,
        timestamp_output: BucketTimestamp::Start,
        time_delta: 0,
        empty: false,
    };
    let mut arg_count: usize = 0;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EMPTY) => {
                aggr.empty = true;
                arg_count += 1;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            }
            _ => {
                return Err(ValkeyError::Str("TSDB: unknown AGGREGATION option"))
            }
        }
        if arg_count == 3 {
            break;
        }
    }

    Ok(aggr)
}
