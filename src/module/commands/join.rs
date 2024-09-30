use crate::common::types::{Sample, SampleLike, Timestamp};
use crate::joins::asof::{merge_apply_asof, MergeAsOfMode, PotentiallyUnmatchedPair};
use crate::joins::TimeSeriesDataPoint;
use crate::module::arg_parse::{parse_count, parse_duration_arg, parse_timestamp_filter, parse_timestamp_range, parse_value_filter, CommandArgIterator};
use crate::module::commands::range_utils::get_range_internal;
use crate::module::get_timeseries;
use crate::module::types::{TimestampRange, ValueFilter};
use crate::storage::time_series::TimeSeries;
use joinkit::{EitherOrBoth, Joinkit};
use metricsql_common::prelude::humanize_duration;
use std::borrow::BorrowMut;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_TOLERANCE: &'static str = "TOLERANCE";
const CMD_ARG_RANGE: &'static str = "RANGE";
const CMD_ARG_FILTER_BY_VALUE: &'static str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &'static str = "FILTER_BY_TS";
const CMD_ARG_COUNT: &'static str = "COUNT";
const CMD_ARG_DIRECTION: &'static str = "DIRECTION";
const CMD_ARG_LEFT: &'static str = "LEFT";
const CMD_ARG_RIGHT: &'static str = "RIGHT";
const CMD_ARG_INNER: &'static str = "INNER";
const CMD_ARG_FULL: &'static str = "FULL";
const CMD_ARG_ASOF: &'static str = "ASOF";
const CMD_ARG_EXCLUSIVE: &'static str = "EXCLUSIVE";

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

/// VKM.JOIN [LEFT|RIGHT|INNER|ASOF] key1 key2 RANGE start end
/// [FILTER_BY_TS ts...]
/// [FILTER_BY_VALUE min max]
/// EXCLUSIVE
/// [COUNT count]
/// [DIRECTION <forward|backward>]
/// [TOLERANCE 2ms]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let join_type_str = args.next_string()?.to_ascii_uppercase();
    let join_type = match join_type_str.as_str() {
        CMD_ARG_LEFT => JoinType::Left(false),
        CMD_ARG_RIGHT => JoinType::Right(false),
        CMD_ARG_INNER => JoinType::Inner,
        CMD_ARG_FULL => JoinType::Full,
        CMD_ARG_ASOF => JoinType::AsOf(JoinAsOfDirection::Forward, Duration::default()),
        _ => return Err(ValkeyError::Str("invalid join type")),
    };

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;

    let mut options = JoinOptions {
        join_type,
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

fn parse_join_args(options: &mut JoinOptions, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut range_found: bool = false;

    while let Ok(arg) = args.next_str() {
        let upper = arg.to_ascii_uppercase();
        match upper.as_str() {
            CMD_ARG_TOLERANCE => {
                match options.join_type {
                    JoinType::AsOf(_, ref mut duration) => {
                        let d = args.next_arg()?;
                        *duration = parse_duration_arg(&d)?;
                    }
                    _ => {
                        return Err(ValkeyError::Str("ERR: join argument to `TOLERANCE` only valid for ASOF"));
                    }
                }
            }
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
            CMD_ARG_DIRECTION => {
                match options.join_type.borrow_mut() {
                    JoinType::AsOf(mut direction, _duration) => {
                        let candidate = args.next_str()?;
                        direction = candidate.try_into()?;
                    }
                    _ => {
                        return Err(ValkeyError::Str("ERR: join argument to `DIRECTION` only valid for ASOF"));
                    }
                }
            }
            CMD_ARG_EXCLUSIVE => {
                match options.join_type {
                    JoinType::Left(ref mut exclusive) => { *exclusive = true; }
                    JoinType::Right(ref mut exclusive) => { *exclusive = true; }
                    _ => {
                        return Err(ValkeyError::Str("ERR: join argument `EXCLUSIVE` current join type"));
                    }
                }
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
    match arg {
        arg if arg.eq_ignore_ascii_case(CMD_ARG_TOLERANCE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_RANGE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_DIRECTION) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_LEFT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_RIGHT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_INNER) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FULL) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_ASOF) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_EXCLUSIVE) => true,
        _ => false,
    }
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
    if options.join_type.is_asof() {
        return join_asof(left, right, options);
    }

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

    match options.join_type {
        JoinType::Left(exclusive) => {
            if exclusive {
                join_left_exclusive(&mut state, left, right, acc_exclusive)
            } else {
                join_left(&mut state, left, right, acc)
            }
        }
        JoinType::Right(exclusive) => if exclusive {
            join_right_exclusive(&mut state, left, right, acc_exclusive)
        } else {
            join_right(&mut state, left, right, acc)
        }
        JoinType::Inner => join_inner(&mut state, left, right, acc),
        JoinType::Full => join_full(&mut state, left, right, acc),
        _ => unreachable!("join type unexpected")
    }

    ValkeyValue::Array(state)
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
    let joined = join_asof_internal(&left, &right, options);

    let result = joined.into_iter().map(|x| {
        let TimeSeriesDataPoint { timestamp, value } = x;
        let other_value = if let Some(val) = value.1 {
            ValkeyValue::Float(val)
        } else {
            ValkeyValue::Null
        };
        let row = vec![
            ValkeyValue::from(timestamp),
            ValkeyValue::Float(value.0),
            other_value,
        ];
        ValkeyValue::Array(row)
    }).collect::<Vec<ValkeyValue>>();

    result.into()
}

fn join_asof_internal(left_samples: &Vec<Sample>, right_samples: &Vec<Sample>, options: &JoinOptions) -> Vec<MergeRow> {
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

    merge_apply_asof(&left_samples, &right_samples, tolerance, merge_mode)
        .iter()
        .map(convert_unmatched_pair)
        .collect()
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

#[cfg(test)]
mod tests {
    use crate::common::types::{Sample, Timestamp};
    use crate::joins::TimeSeriesDataPoint;
    use crate::module::commands::join::join_asof_internal;
    use crate::module::commands::{JoinAsOfDirection, JoinOptions, JoinType};
    use std::time::Duration;

    fn create_samples(timestamps: &[Timestamp], values: &[f64]) -> Vec<Sample> {
        timestamps.iter()
            .zip(values.iter())
            .map(|(t, v)| Sample::new(*t, *v))
            .collect()
    }

    fn create_options() -> JoinOptions {
        JoinOptions::default()
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

        let custom = join_asof_internal(&ts, &ts_join, &options);

        options.join_type = JoinType::AsOf(JoinAsOfDirection::Backward, Duration::from_millis(2));
        let custom2 = join_asof_internal(&ts, &ts_join, &options);

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
        let custom = join_asof_internal(&ts, &ts_join, &options);

        options.join_type = JoinType::AsOf(JoinAsOfDirection::Forward, Duration::from_millis(2));
        let custom2 = join_asof_internal(&ts, &ts_join, &options);


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