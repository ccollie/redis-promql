use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use ahash::AHashMap;
use enquote::enquote;
use metricsql_engine::TimestampTrait;
use redis_module::{RedisError, RedisResult, RedisString};
use serde::{Deserialize, Serialize};
use crate::common::parse_timestamp;
use crate::index::RedisContext;

pub trait AToAny: 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AToAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}


pub type Timestamp = metricsql_engine::prelude::Timestamp;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut v = self.name.cmp(&other.name);
        if v == Ordering::Equal {
            v = self.value.cmp(&other.value);
        }
        Some(v)
    }
}

/// MetricLabels is collection of Label
pub struct MetricLabels(pub Vec<Label>);

impl MetricLabels {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<Label> {
        self.0.iter()
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }
}

fn quote(s: &str) -> String {
    if s.chars().count() < 2 {
        return s.to_string()
    }

    let quote = s.chars().next().unwrap();
    enquote(quote, s)
}

impl Display for MetricLabels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let estimated_size = self.0.iter().fold(0, |acc, l| acc + l.name.len() + l.value.len() + 2);
        let mut s = String::with_capacity(estimated_size);
        s.push('{');
        for (i, l) in self.0.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(&l.name);
            s.push('=');
            let quoted = quote(&l.value);
            s.push_str(&quoted);
            s.push(',');
        }
        write!(f, "{}", s)
    }
}


impl PartialEq<Self> for MetricLabels {
    fn eq(&self, other: &Self) -> bool {
        label_compare(&self.0, &other.0) == Ordering::Equal
    }
}

/// label_compare return negative if a is less than b, return 0 if they are the same
/// eg.
/// a=[Label{name: "a", value: "1"}],b=[Label{name: "b", value: "1"}], return -1
/// a=[Label{name: "a", value: "2"}],b=[Label{name: "a", value: "1"}], return 1
/// a=[Label{name: "a", value: "1"}],b=[Label{name: "a", value: "1"}], return 0
impl PartialOrd for MetricLabels {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(label_compare(&self.0, &other.0))
    }
}
fn label_compare(a: &[Label], b: &[Label]) -> Ordering {
    let mut l = a.len();
    if b.len() < l {
        l = b.len();
    }

    for (a_label, b_label) in a.iter().zip(b.iter()) {
        let mut v = a_label.partial_cmp(&b_label).map_or(Ordering::Equal, |o| o);
        if v != Ordering::Equal {
            return v;
        }
    }
    // if all labels so far were in common, the set with fewer labels comes first.
    let b_len = b.len();
    return a.len().cmp(&b_len)
}

impl From<HashMap<String, String>> for MetricLabels {
    fn from(m: HashMap<String, String>) -> Self {
        let mut labels = Vec::with_capacity(m.len());
        for (k, v) in m {
            labels.push(Label{
                name: k.to_string(),
                value: v.to_string()
            })
        }
        labels.sort();
        MetricLabels(labels)
    }
}

impl From<&AHashMap<String, String>> for MetricLabels {
    fn from(m: &AHashMap<String, String>) -> Self {
        let mut labels = Vec::with_capacity(m.len());
        for (k, v) in m {
            labels.push(Label{
                name: k.to_string(),
                value: v.to_string()
            })
        }
        labels.sort();
        MetricLabels(labels)
    }
}

impl From<AHashMap<String, String>> for MetricLabels {
    fn from(m: AHashMap<String, String>) -> Self {
        let mut labels = Vec::with_capacity(m.len());
        for (k, v) in m.into_iter() {
            labels.push(Label{
                name: k,
                value: v
            })
        }
        labels.sort();
        MetricLabels(labels)
    }
}

/// Represents a data point in time series.
#[derive(Debug, Deserialize, Serialize)]
pub struct Sample {
    /// Timestamp from epoch.
    pub(crate) timestamp: Timestamp,

    /// Value for this data point.
    pub(crate) value: f64,
}

impl Sample {
    /// Create a new DataPoint from given time and value.
    pub fn new(time: Timestamp, value: f64) -> Self {
        Sample { timestamp: time, value }
    }

    /// Get time.
    pub fn get_time(&self) -> i64 {
        self.timestamp
    }

    /// Get value.
    pub fn get_value(&self) -> f64 {
        self.value
    }
}

impl Clone for Sample {
    fn clone(&self) -> Sample {
        Sample {
            timestamp: self.get_time(),
            value: self.get_value(),
        }
    }
}

impl PartialEq for Sample {
    #[inline]
    fn eq(&self, other: &Sample) -> bool {
        // Two data points are equal if their times are equal, and their values are either equal or are NaN.

        if self.timestamp == other.timestamp {
            if self.value.is_nan() {
                return other.value.is_nan();
            } else {
                return self.value == other.value;
            }
        }
        false
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub const MAX_TIMESTAMP: i64 = 253402300799;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Copy)]
pub enum TimestampRangeValue {
    Earliest,
    Latest,
    Now,
    Value(Timestamp),
}

impl TimestampRangeValue {
    pub fn to_redis_string(&self, ctx: &RedisContext) -> RedisString {
        match self {
            TimestampRangeValue::Earliest => ctx.create_string("-"),
            TimestampRangeValue::Latest => ctx.create_string("+"),
            TimestampRangeValue::Now => ctx.create_string("*"),
            TimestampRangeValue::Value(ts) => {
                ctx.create_string(ts.to_string())
            },
        }
    }

    pub fn to_timestamp(&self) -> Timestamp {
        match self {
            TimestampRangeValue::Earliest => 0,
            TimestampRangeValue::Latest => MAX_TIMESTAMP,
            TimestampRangeValue::Now => Timestamp::now(),
            TimestampRangeValue::Value(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = RedisError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "-" => Ok(TimestampRangeValue::Earliest),
            "+" => Ok(TimestampRangeValue::Latest),
            "*" => Ok(TimestampRangeValue::Now),
            _ => {
                let ts = parse_timestamp(value).map_err(|_| RedisError::Str("invalid timestamp"))?;
                if ts < 0 {
                    return Err(RedisError::Str("TSDB: invalid timestamp, must be a non-negative integer"));
                }
                Ok(TimestampRangeValue::Value(ts))
            }
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
        match self {
            TimestampRangeValue::Earliest => write!(f, "-"),
            TimestampRangeValue::Latest => write!(f, "+"),
            TimestampRangeValue::Value(ts) => write!(f, "{}", ts),
            TimestampRangeValue::Now => write!(f, "*"),
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
            },
            (Value(v), Now) => {
                let now = Timestamp::now();
                v.partial_cmp(&now)
            },
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}


pub struct TimestampRange {
    start: TimestampRangeValue,
    end: TimestampRangeValue,
}

impl TimestampRange {
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> RedisResult<Self> {
        if start > end {
            return Err( RedisError::Str("invalid range"));
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

pub(crate) fn normalize_range_timestamps(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> (TimestampRangeValue, TimestampRangeValue) {
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (
            TimestampRangeValue::Value(start),
            TimestampRangeValue::Latest,
        ),
        (None, Some(end)) => (TimestampRangeValue::Earliest, end.into()),
        (None, None) => (TimestampRangeValue::Earliest, TimestampRangeValue::Latest),
    }
}
