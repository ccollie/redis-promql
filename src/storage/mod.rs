use get_size::GetSize;
use metricsql_common::prelude::Label;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::size_of;
use std::str::FromStr;
use std::time::Duration;
use ahash::HashSet;
use valkey_module::{ValkeyError, ValkeyString};

mod chunk;
mod pco_chunk;
mod constants;
mod merge;
mod slice;
pub mod time_series;
mod uncompressed_chunk;
pub(crate) mod utils;
pub(crate) mod series_data;
mod defrag;
mod types;
mod timestamps_filter_iterator;
mod gorilla_chunk;

use crate::aggregators::Aggregator;
use crate::common::types::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::module::arg_parse::TimestampRangeValue;
pub(super) use chunk::*;
pub(crate) use constants::*;
pub(crate) use defrag::*;
pub(crate) use slice::*;

pub const SAMPLE_SIZE: usize = size_of::<Sample>();



#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum Encoding {
    #[default]
    Compressed,
    Uncompressed,
}

impl Encoding {
    pub fn is_compressed(&self) -> bool {
        match self {
            Encoding::Compressed => true,
            Encoding::Uncompressed => false,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Compressed => "COMPRESSED",
            Encoding::Uncompressed => "UNCOMPRESSED",
        }
    }
}

impl Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Encoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("compressed") => Ok(Encoding::Compressed),
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(Encoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", s)),
        }
    }
}

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy)]
#[derive(GetSize)]
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    KeepFirst,
    /// overwrite the existing value with the new value
    KeepLast,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DuplicatePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::KeepFirst => "first",
            DuplicatePolicy::KeepLast => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            DuplicatePolicy::Block => 0,
            DuplicatePolicy::KeepFirst => 1,
            DuplicatePolicy::KeepLast => 2,
            DuplicatePolicy::Min => 4,
            DuplicatePolicy::Max => 8,
            DuplicatePolicy::Sum => 16,
        }
    }

    pub fn value_on_duplicate(self, ts: Timestamp, old: f64, new: f64) -> TsdbResult<f64> {
        use DuplicatePolicy::*;
        let has_nan = old.is_nan() || new.is_nan();
        if has_nan && self != Block {
            // take the valid sample regardless of policy
            let value = if new.is_nan() { old } else { new };
            return Ok(value);
        }
        Ok(match self {
            Block => {
                // todo: format ts as iso-8601 or rfc3339
                let msg = format!("{new} @ {ts}");
                return Err(TsdbError::DuplicateSample(msg));
            }
            KeepFirst => old,
            KeepLast => new,
            Min => old.min(new),
            Max => old.max(new),
            Sum => old + new,
        })
    }
}

impl FromStr for DuplicatePolicy {
    type Err = TsdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DuplicatePolicy::*;

        match s {
            s if s.eq_ignore_ascii_case("block") => Ok(Block),
            s if s.eq_ignore_ascii_case("first") => Ok(KeepFirst),
            s if s.eq_ignore_ascii_case("keepfirst") || s.eq_ignore_ascii_case("keep_first") => Ok(KeepFirst),
            s if s.eq_ignore_ascii_case("last") => Ok(KeepLast),
            s if s.eq_ignore_ascii_case("keeplast") || s.eq_ignore_ascii_case("keep_first") => Ok(KeepLast),
            s if s.eq_ignore_ascii_case("min") => Ok(Min),
            s if s.eq_ignore_ascii_case("max") => Ok(Max),
            s if s.eq_ignore_ascii_case("sum") => Ok(Sum),
            _ => Err(TsdbError::General(format!("invalid duplicate policy: {s}"))),
        }
    }
}
impl From<&str> for DuplicatePolicy {
    fn from(s: &str) -> Self {
        DuplicatePolicy::from_str(s).unwrap()
    }
}

impl TryFrom<u8> for DuplicatePolicy {
    type Error = TsdbError;

    fn try_from(n: u8) -> Result<Self, Self::Error> {
        match n {
            0 => Ok(DuplicatePolicy::Block),
            1 => Ok(DuplicatePolicy::KeepFirst),
            2 => Ok(DuplicatePolicy::KeepLast),
            4 => Ok(DuplicatePolicy::Min),
            8 => Ok(DuplicatePolicy::Max),
            16 => Ok(DuplicatePolicy::Sum),
            _ => Err(TsdbError::General(format!("invalid duplicate policy: {n}"))),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: Option<String>,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub dedupe_interval: Option<Duration>,
    pub labels: Vec<Label>,
    pub significant_digits: Option<u8>,
}

impl TimeSeriesOptions {
    pub fn encoding(&mut self, encoding: Encoding) {
        self.encoding = Some(encoding);
    }

    pub fn chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = Some(chunk_size);
    }

    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn duplicate_policy(&mut self, duplicate_policy: DuplicatePolicy) {
        self.duplicate_policy = Some(duplicate_policy);
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

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub start: TimestampRangeValue,
    pub end: TimestampRangeValue,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub filter: Option<RangeFilter>,
    pub alignment: Option<RangeAlignment>,
    pub with_labels: bool,
    pub selected_labels: HashSet<String>,
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

    pub fn set_valid_timestamps(&mut self, timestamps: Vec<crate::common::types::Timestamp>) {
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
#[cfg(test)]
mod tests {
    use crate::error::TsdbError;
    use crate::storage::DuplicatePolicy;
    use std::str::FromStr;

    #[test]
    fn test_duplicate_policy_parse() {
        assert!(matches!(DuplicatePolicy::from_str("block"), Ok(DuplicatePolicy::Block)));
        assert!(matches!(DuplicatePolicy::from_str("last"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("keepLast"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("first"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("KeEpFIRst"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("min"), Ok(DuplicatePolicy::Min)));
        assert!(matches!(DuplicatePolicy::from_str("max"), Ok(DuplicatePolicy::Max)));
        assert!(matches!(DuplicatePolicy::from_str("sum"), Ok(DuplicatePolicy::Sum)));
    }

    #[test]
    fn test_duplicate_policy_handle_duplicate() {
        let dp = DuplicatePolicy::Block;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert!(matches!(dp.value_on_duplicate(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let dp = DuplicatePolicy::KeepFirst;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::KeepLast;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Min;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::Max;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Sum;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old + new);
    }

    #[test]
    fn test_duplicate_policy_handle_nan() {
        use DuplicatePolicy::*;

        let dp = Block;
        let ts = 0;
        let old = 1.0;
        let new = f64::NAN;
        assert!(matches!(dp.value_on_duplicate(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let policies = [KeepFirst, KeepLast, Min, Max, Sum];
        for policy in policies {
            assert_eq!(policy.value_on_duplicate(ts, 10.0, f64::NAN).unwrap(), 10.0);
            assert_eq!(policy.value_on_duplicate(ts, f64::NAN, 8.0).unwrap(), 8.0);
        }
    }
}