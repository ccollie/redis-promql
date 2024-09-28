use crate::common::types::Timestamp;
use crate::module::arg_parse::parse_timestamp;
use crate::storage::time_series::TimeSeries;
use crate::storage::MAX_TIMESTAMP;
use metricsql_parser::prelude::Matchers;
use metricsql_runtime::types::TimestampTrait;
use std::cmp::Ordering;
use std::fmt::Display;
use std::time::Duration;
use ahash::HashSet;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};
use crate::aggregators::Aggregator;
use crate::error::{TsdbError, TsdbResult};

#[derive(Clone, Default, Debug, PartialEq, Copy)]
pub enum TimestampRangeValue {
    Earliest,
    Latest,
    #[default]
    Now,
    Value(Timestamp),
}

impl TimestampRangeValue {
    pub fn to_timestamp(&self) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => 0,
            Latest => MAX_TIMESTAMP,
            Now => Timestamp::now(),
            Value(ts) => *ts,
        }
    }

    pub fn to_series_timestamp(&self, series: &TimeSeries) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => series.first_timestamp,
            Latest => series.last_timestamp,
            Now => Timestamp::now(),
            Value(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        match value {
            "-" => Ok(Earliest),
            "+" => Ok(Latest),
            "*" => Ok(Now),
            _ => {
                let ts =
                    parse_timestamp(value).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
                if ts < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid timestamp, must be a non-negative integer",
                    ));
                }
                Ok(TimestampRangeValue::Value(ts))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(Earliest),
                b'+' => return Ok(Latest),
                b'*' => return Ok(Now),
                _ => {}
            }
        }
        if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(ValkeyError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(TimestampRangeValue::Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            let ts =
                parse_timestamp(&date_str).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
            Ok(TimestampRangeValue::Value(ts))
        }
    }
}

impl From<Timestamp> for TimestampRangeValue {
    fn from(ts: Timestamp) -> Self {
        TimestampRangeValue::Value(ts)
    }
}

impl Display for TimestampRangeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimestampRangeValue::*;
        match self {
            Earliest => write!(f, "-"),
            Latest => write!(f, "+"),
            Value(ts) => write!(f, "{}", ts),
            Now => write!(f, "*"),
        }
    }
}

impl PartialOrd for TimestampRangeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use TimestampRangeValue::*;

        match (self, other) {
            (Now, Now) => Some(Ordering::Equal),
            (Earliest, Earliest) => Some(Ordering::Equal),
            (Latest, Latest) => Some(Ordering::Equal),
            (Value(a), Value(b)) => a.partial_cmp(b),
            (Now, Value(v)) => {
                let now = Timestamp::now();
                now.partial_cmp(v)
            }
            (Value(v), Now) => {
                let now = Timestamp::now();
                v.partial_cmp(&now)
            }
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}

// todo: better naming
pub struct TimestampRange {
    start: TimestampRangeValue,
    end: TimestampRangeValue,
}

impl TimestampRange {
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> ValkeyResult<Self> {
        if start > end {
            return Err(ValkeyError::Str("invalid timestamp range: start > end"));
        }
        Ok(TimestampRange { start, end })
    }

    pub fn start(&self) -> &TimestampRangeValue {
        &self.start
    }

    pub fn end(&self) -> &TimestampRangeValue {
        &self.end
    }
}

pub struct MetadataFunctionArgs {
    pub label_name: Option<String>,
    pub start: Timestamp,
    pub end: Timestamp,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

pub(crate) fn normalize_range_timestamps(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> (TimestampRangeValue, TimestampRangeValue) {
    use TimestampRangeValue::*;
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (
            TimestampRangeValue::Value(start),
            Latest,
        ),
        (None, Some(end)) => (Earliest, end.into()),
        (None, None) => (Earliest, Latest),
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> TsdbResult<Self> {
        if min > max {
            return Err(TsdbError::General("ERR invalid range".to_string()));
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct RangeFilter {
    pub value: Option<ValueFilter>,
    pub timestamps: Option<Vec<Timestamp>>,
}

impl RangeFilter {
    pub fn new(value: Option<ValueFilter>, timestamps: Option<Vec<Timestamp>>) -> Self {
        Self { value, timestamps }
    }

    pub fn filter(&self, timestamp: Timestamp, value: f64) -> bool {
        if let Some(value_filter) = &self.value {
            if value < value_filter.min || value > value_filter.max {
                return false;
            }
        }
        if let Some(timestamps) = &self.timestamps {
            if !timestamps.contains(&timestamp) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum RangeAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: crate::common::types::Timestamp, time_delta: i64) -> crate::common::types::Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts + time_delta / 2,
            Self::End => ts + time_delta,
        }
    }

}
impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub time_delta: i64,
    pub empty: bool
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub(crate) aggregator: Aggregator,
    pub(crate) group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub start: TimestampRangeValue,
    pub end: TimestampRangeValue,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub filter: Option<RangeFilter>,
    pub series_selector: Matchers,
    pub alignment: Option<RangeAlignment>,
    pub with_labels: bool,
    pub selected_labels: HashSet<String>,
    pub grouping: Option<RangeGroupingOptions>,
    pub latest: bool
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            start: TimestampRangeValue::Value(start),
            end: TimestampRangeValue::Value(end),
            ..Default::default()
        }
    }

    pub fn set_value_range(&mut self, start: f64, end: f64) -> TsdbResult<()> {
        let mut filter = self.filter.clone().unwrap_or_default();
        filter.value = Some(ValueFilter::new(start, end)?);
        self.filter = Some(filter);
        Ok(())
    }

    pub fn set_valid_timestamps(&mut self, timestamps: Vec<Timestamp>) {
        let mut filter = self.filter.clone().unwrap_or_default();
        filter.timestamps = Some(timestamps);
        self.filter = Some(filter);
    }

    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }

    pub fn get_value_filter(&self) -> Option<&ValueFilter> {
        if let Some(filter) = &self.filter {
            if let Some(value_filter) = &filter.value {
                return Some(value_filter)
            }
        }
        None
    }
}