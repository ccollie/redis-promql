use crate::common::types::{Sample, SampleLike, Timestamp};
use crate::joins::asof::{merge_apply_asof, MergeAsOfMode, PotentiallyUnmatchedPair};
use crate::joins::TimeSeriesDataPoint;
use crate::module::arg_parse::*;
use crate::module::commands::range_utils::get_range_internal;
use crate::module::get_timeseries;
use crate::module::types::{TimestampRange, ValueFilter};
use crate::storage::time_series::TimeSeries;
use joinkit::{EitherOrBoth, Joinkit};
use metricsql_common::prelude::humanize_duration;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_TOLERANCE: &'static str = "TOLERANCE";
const CMD_ARG_RANGE: &'static str = "RANGE";
const CMD_ARG_FILTER_BY_VALUE: &'static str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &'static str = "FILTER_BY_TS";
const CMD_ARG_COUNT: &'static str = "COUNT";
const CMD_ARG_LEFT: &'static str = "LEFT";
const CMD_ARG_RIGHT: &'static str = "RIGHT";
const CMD_ARG_INNER: &'static str = "INNER";
const CMD_ARG_FULL: &'static str = "FULL";
const CMD_ARG_ASOF: &'static str = "ASOF";
const CMD_ARG_PRIOR: &'static str = "PRIOR";
const CMD_ARG_NEXT: &'static str = "NEXT";
const CMD_ARG_EXCLUSIVE: &'static str = "EXCLUSIVE";

const VALID_ARGS: [&str; 9] = [
    CMD_ARG_RANGE,
    CMD_ARG_FILTER_BY_VALUE,
    CMD_ARG_FILTER_BY_TS,
    CMD_ARG_COUNT,
    CMD_ARG_LEFT,
    CMD_ARG_RIGHT,
    CMD_ARG_INNER,
    CMD_ARG_FULL,
    CMD_ARG_ASOF,
];

#[derive(Debug, Default, Copy, Clone)]
pub enum JoinType {
    Left(bool),
    Right(bool),
    #[default]
    Inner,
    Full,
    AsOf(JoinAsOfDirection, Duration),
}

impl JoinType {
    pub fn is_asof(&self) -> bool {
        matches!(self, JoinType::AsOf(..))
    }

    pub fn can_be_exclusive(&self) -> bool {
        matches!(self, JoinType::Left(..) | JoinType::Right(..))
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Left(exclusive) => {
                write!(f, "LEFT OUTER JOIN")?;
                if *exclusive {
                    write!(f, " EXCLUSIVE")?;
                }
            }
            JoinType::Right(exclusive) => {
                write!(f, "RIGHT OUTER JOIN")?;
                if *exclusive {
                    write!(f, " EXCLUSIVE")?;
                }
            }
            JoinType::Inner => {
                write!(f, "INNER JOIN")?;
            }
            JoinType::Full => {
                write!(f, "FULL JOIN")?;
            }
            JoinType::AsOf(dir, tolerance) => {
                write!(f, "ASOF JOIN")?;
                match dir {
                    JoinAsOfDirection::Forward => write!(f, " FORWARD")?,
                    JoinAsOfDirection::Backward => write!(f, " BACKWARD")?,
                }
                if !tolerance.is_zero() {
                    write!(f, " TOLERANCE {}", humanize_duration(tolerance))?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum JoinAsOfDirection {
    #[default]
    Forward,
    Backward,
}

impl Display for JoinAsOfDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAsOfDirection::Forward => write!(f, "Forward"),
            JoinAsOfDirection::Backward => write!(f, "Backward"),
        }
    }
}

impl FromStr for JoinAsOfDirection {
    type Err = ValkeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("forward") => Ok(JoinAsOfDirection::Forward),
            s if s.eq_ignore_ascii_case("backward") => Ok(JoinAsOfDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

impl TryFrom<&str> for JoinAsOfDirection {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let direction = value.to_lowercase();
        match direction.as_str() {
            "forward" => Ok(JoinAsOfDirection::Forward),
            "backward" => Ok(JoinAsOfDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

#[derive(Debug, Default)]
pub struct JoinOptions {
    pub join_type: JoinType,
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
}

/// VKM.JOIN key1 key2 RANGE start end
/// [[INNER] | [FULL] | [LEFT [EXCLUSIVE]] | [RIGHT [EXCLUSIVE]] | [ASOF [PRIOR | NEXT] [TOLERANCE 2ms]]]
/// [FILTER_BY_TS ts...]
/// [FILTER_BY_VALUE min max]
/// [COUNT count]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;

    let mut options = JoinOptions {
        join_type: Default::default(),
        date_range: Default::default(),
        count: Default::default(),
        timestamp_filter: Default::default(),
        value_filter: Default::default(),
    };

    parse_join_args(&mut options, &mut args)?;

    let left_series = get_timeseries(ctx, &left_key)?;
    let right_series = get_timeseries(ctx, &right_key)?;

    Ok(process_join(left_series, right_series, &options))
}

fn parse_asof(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut direction = JoinAsOfDirection::Forward;

    // ASOF [PRIOR | NEXT] [TOLERANCE duration]
    if let Some(next) = advance_if_next_token_one_of(args, &[CMD_ARG_PRIOR, CMD_ARG_NEXT]) {
        if next == CMD_ARG_PRIOR {
            direction = JoinAsOfDirection::Backward;
        } else if next == CMD_ARG_NEXT {
            direction = JoinAsOfDirection::Forward;
        }
    }
    if advance_if_next_token(args, CMD_ARG_TOLERANCE) {
       let arg = args.next_str()?;
        tolerance = parse_duration(arg)?
    }

    Ok(JoinType::AsOf(direction, tolerance))
}

fn possibly_parse_exclusive(args: &mut CommandArgIterator) -> bool {
    advance_if_next_token(args, CMD_ARG_EXCLUSIVE)
}

fn parse_join_args(options: &mut JoinOptions, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut range_found: bool = false;
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("TSDB: join type already set"))
        } else {
            *is_set = true;
            Ok(())
        }
    }

    while let Ok(arg) = args.next_str() {
        let upper = arg.to_ascii_uppercase();
        match upper.as_str() {
            CMD_ARG_RANGE => {
                range_found = true;
                options.date_range = parse_timestamp_range(args)?;
            }
            CMD_ARG_FILTER_BY_VALUE => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CMD_ARG_FILTER_BY_TS => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, is_arg_valid)?);
            }
            CMD_ARG_COUNT => {
                options.count = Some(parse_count(args)?);
            }
            CMD_ARG_LEFT => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CMD_ARG_RIGHT => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CMD_ARG_INNER => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Inner;
            }
            CMD_ARG_FULL => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Full;
            }
            CMD_ARG_ASOF => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = parse_asof(args)?;
            }
            _ => return Err(ValkeyError::Str("invalid JOIN command argument")),
        }
    }
    if !range_found {
        return Err(ValkeyError::Str("ERR: missing RANGE"));
    }

    Ok(())
}

fn is_arg_valid(arg: &str) -> bool {
    VALID_ARGS.iter().any(|x| x.eq_ignore_ascii_case(arg))
}

type MergeRow = TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)>;

pub(crate) struct JoinResultRow {
    pub(crate) timestamp: Timestamp,
    pub(crate) left: Option<f64>,
    pub(crate) right: Option<f64>,
}

impl JoinResultRow {
    pub(crate) fn new(timestamp: Timestamp, left: Option<f64>, right: Option<f64>) -> Self {
        Self {
            timestamp,
            left,
            right,
        }
    }
}

fn process_join(left_series: &TimeSeries, right_series: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    // todo: rayon::join
    let left_samples = fetch_samples(left_series, options);
    let right_samples = fetch_samples(right_series, options);
    join_internal(&left_samples, &right_samples, options)
}

fn join_internal(left: &Vec<Sample>, right: &Vec<Sample>, options: &JoinOptions) -> ValkeyValue {

    let is_exclusive = match options.join_type {
        JoinType::Left(exclusive) | JoinType::Right(exclusive) => exclusive,
        _ => false
    };

    let mut state: Vec<ValkeyValue> = Vec::with_capacity(left.len().max(right.len()));

    fn acc(state: &mut Vec<ValkeyValue>, row: JoinResultRow) {
        let ts = ValkeyValue::from(row.timestamp);
        let left = if let Some(value) = row.left {
            ValkeyValue::Float(value)
        } else {
            ValkeyValue::Null
        };
        let right = if let Some(value) = row.right {
            ValkeyValue::Float(value)
        } else {
            ValkeyValue::Null
        };
        let value = ValkeyValue::Array(vec![ts, left, right]);
        state.push(value);
    }

    fn acc_exclusive(state: &mut Vec<ValkeyValue>, row: JoinResultRow) {
        let ts = ValkeyValue::from(row.timestamp);
        let value = row.left.unwrap_or(row.right.unwrap_or(f64::NAN));
        if !value.is_nan() {
            let val = vec![ts, ValkeyValue::Float(value)];
            state.push(ValkeyValue::Array(val))
        }
    }

    let mapper = if is_exclusive {
        acc_exclusive
    } else {
        acc
    };

    join_samples_internal(left, right, options, &mut state, mapper);

    ValkeyValue::Array(state)
}

pub(crate) fn join_samples_internal<F, STATE>(
    left: &Vec<Sample>,
    right: &Vec<Sample>,
    options: &JoinOptions,
    state: &mut STATE,
    f: F,
)
where F: Fn(&mut STATE, JoinResultRow)
{
    match options.join_type {
        JoinType::AsOf(..) => {
            join_asof_internal(left, right, options, state, f)
        }
        JoinType::Left(exclusive) => {
            if exclusive {
                join_left_exclusive(state, left, right, f)
            } else {
                join_left(state, left, right, f)
            }
        }
        JoinType::Right(exclusive) => if exclusive {
            join_right_exclusive(state, left, right, f)
        } else {
            join_right(state, left, right, f)
        }
        JoinType::Inner => join_inner(state, left, right, f),
        JoinType::Full => join_full(state, left, right, f),
    }
}

fn join_left<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    for row in left.into_iter()
        .merge_join_left_outer_by(right, |x, y| x.timestamp.cmp(&y.timestamp))
        .map(convert_join_item) {
        f(state, row);
    }
}

fn join_left_exclusive<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    for row in left.into_iter()
        .merge_join_left_excl_by(right, |x, y| x.timestamp.cmp(&y.timestamp))
        .map(|x| JoinResultRow::new(x.timestamp, Some(x.value), None)) {
        f(state, row);
    }
}

fn join_right<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    let left_iter = left.into_iter().map(|sample| (sample.timestamp, sample));
    let right_iter = right.into_iter().map(|sample| (sample.timestamp, sample));
    for row in left_iter
        .into_iter()
        .hash_join_right_outer(right_iter) {

        match row {
            EitherOrBoth::Left(_) => {
                // should not happen
            }
            EitherOrBoth::Right(r) => {
                for item in r.iter().map(|sample| {
                    JoinResultRow::new(sample.timestamp, None, Some(sample.value))
                }) {
                    f(state, item);
                }
            }
            EitherOrBoth::Both(left, right) => {
                let ts = left.timestamp;
                for item in right.iter()
                    .map(|sample| JoinResultRow::new(ts, Some(left.value), Some(sample.value))) {
                    f(state, item);
                }
            }
        }
    }
}

fn join_right_exclusive<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    let left_iter = left.into_iter().map(|sample| (sample.timestamp, sample));
    let right_iter = right.into_iter().map(|sample| (sample.timestamp, sample));
    for item in left_iter
        .into_iter()
        .hash_join_right_excl(right_iter)
        .flatten() {

        let row = JoinResultRow::new(item.timestamp, Some(item.value), None);
        f(state, row);
    }
}

fn join_inner<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    for row in left.into_iter()
        .merge_join_inner_by(right, |x, y| x.timestamp.cmp(&y.timestamp))
        .map(|(l, r)| JoinResultRow::new(l.timestamp, Some(l.value), Some(r.value))) {
        f(state, row);
    }
}

fn join_full<F, STATE>(state: &mut STATE, left: &Vec<Sample>, right: &Vec<Sample>, f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    for row in left.into_iter()
        .merge_join_full_outer_by(right, |x, y| x.timestamp.cmp(&y.timestamp))
        .map(convert_join_item) {
        f(state, row);
    }
}

fn convert_join_item(item: EitherOrBoth<&Sample, &Sample>) -> JoinResultRow {
    match item {
        EitherOrBoth::Both(l, r) => JoinResultRow::new(l.timestamp, Some(l.value), Some(r.value)),
        EitherOrBoth::Left(l) => JoinResultRow::new(l.timestamp, Some(l.value), None),
        EitherOrBoth::Right(r) => JoinResultRow::new(r.timestamp, None, Some(r.value)),
    }
}

fn join_asof(left: &Vec<Sample>, right: &Vec<Sample>, options: &JoinOptions) -> ValkeyValue {

    let mut state: Vec<ValkeyValue> = Vec::with_capacity(left.len().max(right.len()));

    fn mapper(state: &mut Vec<ValkeyValue>, row: JoinResultRow) {
        let value = join_row_to_value(&row);
        state.push(value);
    }

    join_asof_internal(&left, &right, options, &mut state, mapper);

    ValkeyValue::Array(state)
}

fn join_asof_internal<F, STATE>(left_samples: &Vec<Sample>,
                                right_samples: &Vec<Sample>,
                                options: &JoinOptions,
                                state: &mut STATE,
                                f: F)
where
    F: Fn(&mut STATE, JoinResultRow),
{
    let (tolerance, merge_mode) = match &options.join_type {
        JoinType::AsOf(direction, duration) => {
            let merge_mode = match direction {
                JoinAsOfDirection::Forward => MergeAsOfMode::RollFollowing,
                JoinAsOfDirection::Backward => MergeAsOfMode::RollPrior
            };
            (duration, merge_mode)
        }
        _ => panic!("asof JoinType expected")
    };

    for row in merge_apply_asof(&left_samples, &right_samples, tolerance, merge_mode)
        .iter() {
        let val = JoinResultRow::new(row.this.timestamp, Some(row.this.value), None);
        f(state, val);
    }
}

fn convert_unmatched_pair(value: &PotentiallyUnmatchedPair) -> TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)> {
    TimeSeriesDataPoint {
        timestamp: value.this.timestamp(),
        value: (
            value.this.value(),
            match value.other {
                Some(sample) => Some(sample.value),
                None => None
            }),
    }
}

fn fetch_samples(ts: &TimeSeries, options: &JoinOptions) -> Vec<Sample> {
    let mut samples = get_range_internal(ts, &options.date_range, true, &options.timestamp_filter, &options.value_filter);
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}

pub fn join_row_to_value(row: &JoinResultRow) -> ValkeyValue {
    let left_value = if let Some(val) = row.left {
        ValkeyValue::Float(val)
    } else {
        ValkeyValue::Null
    };
    let other_value = if let Some(val) = row.left {
        ValkeyValue::Float(val)
    } else {
        ValkeyValue::Null
    };
    let row = vec![
        ValkeyValue::from(row.timestamp),
        left_value,
        other_value,
    ];
    ValkeyValue::Array(row)
}

#[cfg(test)]
mod tests {
    use crate::common::types::{Sample, Timestamp};
    use crate::joins::asof::PotentiallyUnmatchedPair;
    use crate::joins::TimeSeriesDataPoint;
    use crate::module::commands::join::join_asof_internal;
    use crate::module::commands::{join_row_to_value, JoinAsOfDirection, JoinOptions, JoinResultRow, JoinType};
    use std::time::Duration;
    use valkey_module::ValkeyValue;

    fn create_samples(timestamps: &[Timestamp], values: &[f64]) -> Vec<Sample> {
        timestamps.iter()
            .zip(values.iter())
            .map(|(t, v)| Sample::new(*t, *v))
            .collect()
    }

    fn convert_unmatched_pair(value: &PotentiallyUnmatchedPair) -> TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)> {
        TimeSeriesDataPoint {
            timestamp: value.this.timestamp,
            value: (
                value.this.value,
                match value.other {
                    Some(sample) => Some(sample.value),
                    None => None
                }),
        }
    }

    fn create_options() -> JoinOptions {
        JoinOptions::default()
    }

    type DataPointVec = Vec<TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)>>;

    fn handle_join_asof(left: &Vec<Sample>, right: &Vec<Sample>, options: &JoinOptions) -> DataPointVec {
        let mut state: DataPointVec = DataPointVec::new();
        fn mapper(state: &mut DataPointVec, row: JoinResultRow) {
            let left_value = row.left.unwrap();
            let val = TimeSeriesDataPoint::new(row.timestamp, (left_value, None));
            state.push(val)
        }

        join_asof_internal(left, right, options, &mut state, mapper);

        state
    }

    #[test]
    fn test_merge_asof_lookingback(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];
        let ts_join = create_samples(&index2, &values2);

        let mut options = create_options();
        options.join_type = JoinType::AsOf(JoinAsOfDirection::Backward, Duration::from_millis(1));

        let custom = handle_join_asof(&ts, &ts_join, &options);

        options.join_type = JoinType::AsOf(JoinAsOfDirection::Backward, Duration::from_millis(2));
        let custom2 = handle_join_asof(&ts, &ts_join, &options);

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        assert_eq!(custom, expected);
        assert_eq!(custom2, expected1);
    }

    #[test]
    fn test_merge_asof_lookingforward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let mut options = create_options();

        options.join_type = JoinType::AsOf(JoinAsOfDirection::Forward, Duration::from_millis(1));
        let custom = handle_join_asof(&ts, &ts_join, &options);

        options.join_type = JoinType::AsOf(JoinAsOfDirection::Forward, Duration::from_millis(2));
        let custom2 = handle_join_asof(&ts, &ts_join, &options);

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        assert_eq!(custom, expected);
        assert_eq!(custom2, expected1);
    }

}