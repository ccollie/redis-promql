//! # TimeSeries Data Element Representations
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// TimeSeriesDataPoint representation, consists of a timestamp and value
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TimeSeriesDataPoint<TDate: Hash + Clone + Eq + Ord, T> {
    pub timestamp: TDate,
    pub value: T,
}


impl<TDate: Hash + Clone + Eq + Ord, T> TimeSeriesDataPoint<TDate, T> {
    /// Generic new method
    pub fn new(timestamp: TDate, value: T) -> TimeSeriesDataPoint<TDate, T> {
        TimeSeriesDataPoint { timestamp, value }
    }
}
