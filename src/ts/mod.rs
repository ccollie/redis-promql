use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
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


pub(super) use chunk::*;
pub(crate) use constants::*;
pub use duplicate_policy::*;

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
        match s.to_ascii_lowercase().as_str() {
            "compressed" => Ok(Encoding::Compressed),
            "uncompressed" => Ok(Encoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: String,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub labels: HashMap<String, String>,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: key.to_string(),
            encoding: None,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            duplicate_policy: None,
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

    pub fn labels(&mut self, labels: HashMap<String, String>) {
        self.labels = labels;
    }
}
