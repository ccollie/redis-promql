use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use ahash::AHashMap;
use redis_module::RedisString;
use serde::{Deserialize, Serialize};

pub mod time_series;
mod dedup;
mod utils;
mod constants;
mod duplicate_policy;
mod encoding;
mod compressed_chunk;
mod uncompressed_chunk;
mod chunk;
mod merge;
mod slice;


pub(super) use chunk::*;
pub(crate) use constants::*;
pub(crate) use slice::*;


#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
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
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    First,
    /// overwrite the existing value with the new value
    Last,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicatePolicy::Block => write!(f, "block"),
            DuplicatePolicy::First => write!(f, "first"),
            DuplicatePolicy::Last => write!(f, "last"),
            DuplicatePolicy::Min => write!(f, "min"),
            DuplicatePolicy::Max => write!(f, "max"),
            DuplicatePolicy::Sum => write!(f, "sum"),
        }
    }
}

impl From<&str> for DuplicatePolicy {
    fn from(s: &str) -> Self {
        match s {
            s if s.eq_ignore_ascii_case("block") => DuplicatePolicy::Block,
            s if s.eq_ignore_ascii_case("first") => DuplicatePolicy::First,
            s if s.eq_ignore_ascii_case("last") => DuplicatePolicy::Last,
            s if s.eq_ignore_ascii_case("min") => DuplicatePolicy::Min,
            s if s.eq_ignore_ascii_case("max") => DuplicatePolicy::Max,
            s if s.eq_ignore_ascii_case("sum") => DuplicatePolicy::Sum,
            _ => panic!("invalid duplicate policy: {}", s),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DuplicateStatus {
    Ok,
    Err,
    Deduped,
}


#[derive(Debug, Default, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: Option<String>,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub dedupe_interval: Option<Duration>,
    pub labels: Option<AHashMap<String, String>>,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: Some(key.to_string()),
            encoding: None,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            duplicate_policy: None,
            dedupe_interval: None,
            labels: Default::default(),
        }
    }

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

    pub fn labels(&mut self, labels: AHashMap<String, String>) {
        self.labels = Some(labels);
    }
}
