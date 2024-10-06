use crate::common::types::Matchers;
use crate::module::arg_parse::*;
use crate::module::types::{AggregationOptions, TimestampRange};
use crate::module::{get_series_iterator, with_matched_series};
use crate::storage::time_series::SeriesSampleIterator;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};
use crate::module::commands::range_utils::get_series_labels;

struct UnionCommandOptions {
    date_range: TimestampRange,
    matchers: Vec<Matchers>,
    with_labels: bool,
    selected_labels: Vec<String>,
    count: Option<usize>,
    aggregation: Option<AggregationOptions>,
}

/// VM.UNION fromTimestamp toTimestamp FILTER filter...
/// [COUNT count]
/// [WITHLABELS]
/// [SELECTED_LABELS label...]
/// [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP bt] EMPTY]
pub(crate) fn union(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_union_options(&mut args)?;


    VALKEY_OK
}

struct SeriesMeta<'a> {
    key: ValkeyString,
    labels: Vec<ValkeyValue>,
    iter: SeriesSampleIterator<'a>
}

struct UnionState<'a> {
    date_range: TimestampRange,
    with_labels: bool,
    selected_labels: Vec<String>,
    aggregation: Option<AggregationOptions>,
    count: Option<usize>,
    series: Vec<SeriesMeta<'a>>
}

fn handle_union(ctx: &Context, options: UnionCommandOptions) -> ValkeyResult {

    let mut state = UnionState {
        date_range: options.date_range,
        with_labels: options.with_labels,
        selected_labels: options.selected_labels,
        aggregation: options.aggregation,
        count: options.count,
        series: Vec::new()
    };

    with_matched_series(ctx, &mut state, &options.matchers, |state, series, key| {
        let iter = get_series_iterator(series, state.date_range, &None, &None);

        let labels = if state.aggregation.is_none() {
            get_series_labels(series, state.with_labels, &state.selected_labels)
        } else {
            vec![]
        };

        let meta = SeriesMeta {
            key,
            labels,
            iter
        };

        //state.series.push(meta);
        Ok(())
    })?;

    VALKEY_OK
}


fn parse_union_options(args: &mut CommandArgIterator) -> ValkeyResult<UnionCommandOptions> {

    let date_range = parse_timestamp_range(args)?;

    let mut options = UnionCommandOptions {
        date_range,
        count: None,
        aggregation: None,
        with_labels: false,
        matchers: Default::default(),
        selected_labels: Default::default(),
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
                let filter = args.next_str()?;
                options.matchers = parse_series_selector_list(args, is_command_keyword)?;
            }
            CMD_ARG_AGGREGATION => {
                options.aggregation = Some(parse_aggregation_options(args)?);
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

