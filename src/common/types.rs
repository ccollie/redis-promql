
pub type Timestamp = metricsql_runtime::prelude::Timestamp;
pub type PooledTimestampVec = metricsql_common::pool::PooledVecI64;
pub type PooledValuesVec = metricsql_common::pool::PooledVecF64;
pub type Sample = metricsql_runtime::types::Sample;
pub type Label = metricsql_runtime::types::Label;
pub type Matchers = metricsql_parser::prelude::Matchers;


pub trait SampleLike: Eq + PartialEq + PartialOrd + Ord {
    fn timestamp(&self) -> Timestamp;
    fn value(&self) -> f64;
}


impl SampleLike for Sample {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    fn value(&self) -> f64 {
        self.value
    }
}