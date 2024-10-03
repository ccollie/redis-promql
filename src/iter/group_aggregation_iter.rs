use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Sample, Timestamp};
use crate::iter::MultiSeriesSampleIter;

pub struct GroupAggregationIter {
    last_sample: Sample,
    bucket_count: usize,
    aggregator: Aggregator,
    inner_iter: MultiSeriesSampleIter,
}

impl GroupAggregationIter {
    pub fn new(series: Vec<Box<dyn Iterator<Item=Sample>>>, aggregator: Aggregator) -> Self {
        Self {
            aggregator,
            bucket_count: 0,
            last_sample: Sample { timestamp: i64::MIN, value: 0.0 },
            inner_iter: MultiSeriesSampleIter::new(series)
        }
    }

    fn finalize_bucket(&mut self, timestamp: Timestamp) -> Sample {
        let value = self.aggregator.finalize();
        self.aggregator.reset();
        self.bucket_count = 0;
        Sample { timestamp, value }
    }
}

impl Iterator for GroupAggregationIter {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.last_sample.timestamp == i64::MIN {
            // first run
            if let Some(sample) = self.inner_iter.next() {
                self.bucket_count = 1;
                self.aggregator.update(sample.value);
                self.last_sample = sample;
            } else {
                return None;
            }
        }

        let last_ts = self.last_sample.timestamp;
        while let Some(sample) = self.inner_iter.by_ref().next() {
            let value = sample.value;
            self.last_sample = sample;
            if sample.timestamp == last_ts {
                self.aggregator.update(value);
                self.bucket_count += 1;
            } else {
                break;
            }
        }

        // i dont this condition is necessary
        if self.bucket_count > 0 {
            let value = self.finalize_bucket(self.last_sample.timestamp);
            return Some(value);
        }

        None
    }
}