use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Sample, Timestamp};
use crate::module::types::{AggregationOptions, BucketTimestamp};

#[derive(Debug)]
pub(crate) struct AggrIterator {
    pub(crate) aggregator: Aggregator,
    pub(crate) time_delta: i64,
    pub(crate) bucket_ts: BucketTimestamp,
    pub(crate) last_timestamp: Timestamp,
    pub(crate) aligned_timestamp: Timestamp,
    pub(crate) count: usize,
    pub(crate) empty: bool,
    bucket_right_ts: Timestamp,
}

impl Default for AggrIterator {
    fn default() -> AggrIterator {
        AggrIterator {
            aggregator: Aggregator::new("avg").unwrap(),
            time_delta: 0,
            bucket_ts: Default::default(),
            last_timestamp: 0,
            bucket_right_ts: 0,
            aligned_timestamp: 0,
            count: AggrIterator::DEFAULT_COUNT,
            empty: false,
        }
    }
}

impl AggrIterator {
    pub(crate) const DEFAULT_COUNT: usize = usize::MAX - 1;

    pub(crate) fn new(options: &AggregationOptions, aligned_timestamp: Timestamp, count: Option<usize>) -> Self {
        AggrIterator {
            aligned_timestamp,
            empty: options.empty,
            aggregator: options.aggregator.clone(),
            time_delta: options.time_delta,
            bucket_ts: options.timestamp_output,
            count: count.unwrap_or(AggrIterator::DEFAULT_COUNT),
            ..Default::default()
        }

    }

    fn normalize_last_timestamp(&mut self) -> Timestamp {
        self.last_timestamp = self.last_timestamp.max(0);
        self.last_timestamp
    }

    fn add_empty_buckets(&self,
                         samples: &mut Vec<Sample>,
                         first_bucket_ts: Timestamp,
                         end_bucket_ts: Timestamp,
                         max_count: usize) {
        let empty_bucket_count = self.calculate_empty_bucket_count(first_bucket_ts, end_bucket_ts, max_count);
        let value = self.aggregator.empty_value();
        for _ in 0..empty_bucket_count {
            samples.push(Sample { timestamp: end_bucket_ts, value });
        }
    }

    fn calculate_empty_bucket_count(&self, first_bucket_ts: Timestamp, end_bucket_ts: Timestamp, max_count: usize) -> usize {
        let time_delta = self.time_delta;
        let total_empty_buckets = (((end_bucket_ts - first_bucket_ts) / time_delta) + 1) as usize;
        let remaining_capacity = max_count - total_empty_buckets;
        total_empty_buckets.min(remaining_capacity)
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts.calculate(self.last_timestamp, self.time_delta)
    }

    fn finalize_current_bucket(&mut self) -> Sample {
        let value = self.aggregator.finalize();
        let timestamp = self.calculate_bucket_start();
        self.aggregator.reset();
        Sample { timestamp, value }
    }

    pub fn calculate(&mut self, iterator: impl Iterator<Item=Sample>) -> Vec<Sample> {
        self.bucket_right_ts = self.last_timestamp + self.time_delta;
        let mut buckets = Vec::new();
        let max_count = self.count;

        self.normalize_last_timestamp();

        for sample in iterator {
            if self.should_finalize_bucket(sample.timestamp) {
                buckets.push(self.finalize_current_bucket());
                if buckets.len() >= self.count {
                    return buckets;
                }
                self.update_bucket_timestamps(sample.timestamp);
                if self.empty {
                    self.finalize_empty_buckets(&mut buckets, max_count);
                }
            }
            self.aggregator.update(sample.value);
        }

        buckets.truncate(max_count);
        buckets
    }

    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_right_ts
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.last_timestamp = calc_bucket_start(timestamp, self.time_delta, self.aligned_timestamp);
        self.bucket_right_ts = self.last_timestamp + self.time_delta;
    }

    fn finalize_empty_buckets(&self, buckets: &mut Vec<Sample>, max_count: usize) {
        let first_bucket = self.bucket_right_ts;
        let last_bucket = self.last_timestamp - self.time_delta;
        if first_bucket < self.last_timestamp {
            self.add_empty_buckets(buckets, first_bucket, last_bucket, max_count);
        }
    }
}

#[inline]
fn calc_bucket_start(ts: Timestamp, bucket_duration: i64, timestamp_alignment: i64) -> Timestamp {
    let timestamp_diff = ts - timestamp_alignment;
    ts - ((timestamp_diff % bucket_duration + bucket_duration) % bucket_duration)
}
