use super::{Chunk, ChunkCompression, TimeSeriesChunk};
use crate::common::types::{PooledTimestampVec, PooledValuesVec, Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::constants::{BLOCK_SIZE_FOR_TIME_SERIES, DEFAULT_CHUNK_SIZE_BYTES, SPLIT_FACTOR};
use crate::ts::uncompressed_chunk::UncompressedChunk;
use crate::ts::DuplicatePolicy;
use ahash::{AHashMap, AHashSet};
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::time::Duration;

pub type Labels = AHashMap<String, String>;

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TimeSeries {
    pub(crate) id: u64,

    pub metric_name: String,

    pub labels: Labels,

    pub retention: Duration,

    pub dedupe_interval: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,

    pub chunk_compression: ChunkCompression,
    pub chunk_size_bytes: usize,
    pub chunks: Vec<TimeSeriesChunk>,

    pub total_samples: u64,

    pub first_timestamp: Timestamp,

    pub last_timestamp: Timestamp,

    pub last_value: f64,
}

impl TimeSeries {
    /// Create a new empty time series.
    pub fn new() -> Self {
        TimeSeries {
            id: 0,
            metric_name: "".to_string(),
            labels: Default::default(),
            retention: Default::default(),
            duplicate_policy: None,
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            dedupe_interval: Default::default(),
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    pub fn add_sample(&mut self, time: Timestamp, value: f64) -> TsdbResult<()> {
        if self.chunks.is_empty() {
            // First insertion in this time series.
            self.chunks
                .push(TimeSeriesChunk::Uncompressed(UncompressedChunk::default()));
        }
        Ok(())
    }

    /// append the given time and value to the time series.
    pub fn append(&mut self, time: Timestamp, value: f64) -> TsdbResult<()> {
        let chunk_size = self.chunk_size_bytes;

        // Try to append the time+value to the last block.
        let ret_val = {
            let last_chunk = self.get_last_chunk();
            last_chunk.add_sample(&Sample {
                timestamp: time,
                value,
            })
        };

        if let Err(err) = ret_val {
            if err == TsdbError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES) {
                // The last block is full. So, compress it and append it time_series_block_compressed.
                let chunk_len = self.chunks.len();
                let chunk = self.chunks.last().unwrap();
                // The last block is full. So, compress it and append it time_series_block_compressed.
                // todo: see if previous block is not full, and if not simply append to it
                let last_chunk = if let TimeSeriesChunk::Uncompressed(uncompressed_chunk) = chunk {
                    let new_chunk = TimeSeriesChunk::new(
                        self.chunk_compression,
                        chunk_size,
                        &uncompressed_chunk.timestamps,
                        &uncompressed_chunk.values,
                    )?;
                    // insert before last elem
                    self.chunks.insert(chunk_len - 1, new_chunk);
                    // todo: clear and return last element
                    let res = self.get_last_chunk();
                    res.clear();
                    res
                } else {
                    // Create a new last block and append the time+value to it.
                    self.append_uncompressed_chunk();
                    self.get_last_chunk()
                };

                last_chunk.add_sample(&Sample {
                    timestamp: time,
                    value,
                })?
            } else {
                return Err(err);
            }
        }

        if time < self.first_timestamp {
            self.first_timestamp = time;
        }

        if time > self.last_timestamp {
            self.last_timestamp = time;
            self.last_value = value;
        }

        Ok(())
    }

    fn append_uncompressed_chunk(&mut self) {
        let new_chunk =
            TimeSeriesChunk::Uncompressed(UncompressedChunk::with_max_size(self.chunk_size_bytes));
        self.chunks.push(new_chunk);
    }

    #[inline]
    fn get_last_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_uncompressed_chunk();
        }
        self.chunks.last_mut().unwrap()
    }

    fn get_first_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_uncompressed_chunk();
        }
        self.chunks.first_mut().unwrap()
    }

    pub fn upsert_sample(
        &mut self,
        timestamp: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        fn handle_upsert(
            chunk: &mut TimeSeriesChunk,
            timestamp: Timestamp,
            value: f64,
            max_size: usize,
            dp_policy: DuplicatePolicy,
        ) -> TsdbResult<(usize, Option<TimeSeriesChunk>)> {
            let mut sample = Sample { timestamp, value };
            if chunk.size() as f64 > max_size as f64 * SPLIT_FACTOR {
                let mut new_chunk = chunk.split()?;
                let size = new_chunk.upsert_sample(&mut sample, dp_policy)?;
                Ok((size, Some(new_chunk)))
            } else {
                let size = chunk.upsert_sample(&mut sample, dp_policy)?;
                Ok((size, None))
            }
        }

        let dp_policy = dp_override.unwrap_or(
            self.duplicate_policy.unwrap_or_default(), //.unwrap_or(/* TSGlobalConfig.duplicatePolicy),
        );

        let block_count = self.chunks.len();
        let max_size = self.chunk_size_bytes;
        let last_chunk = self.get_last_chunk();
        let first_ts = last_chunk.first_timestamp();

        let (size, new_chunk) = if timestamp < first_ts && block_count > 1 {
            // Upsert in an older chunk
            let (pos, found) = get_chunk_index(&self.chunks, timestamp);
            if found {
                let chunk = self.chunks.get_mut(pos).unwrap();
                handle_upsert(chunk, timestamp, value, max_size, dp_policy)?
            } else {
                // if we get here we have a big hole. We're ordered ascending so wee can append to the
                // previous block
                let chunk = if pos > 0 {
                    self.chunks.get_mut(pos - 1).unwrap()
                } else {
                    // we're at the beginning of the series
                    self.get_first_chunk()
                };
                chunk.add_sample(&Sample { timestamp, value })?;
                (1, None)
            }
        } else {
            handle_upsert(last_chunk, timestamp, value, max_size, dp_policy)?
        };

        if let Some(new_chunk) = new_chunk {
            self.chunks.push(new_chunk);
            self.chunks.sort_by_key(|chunk| chunk.first_timestamp());
        }

        self.total_samples += size as u64;
        if timestamp == self.last_timestamp {
            self.last_value = value;
        }

        Ok(size)
    }

    /// Get the time series between give start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> TsdbResult<Vec<Sample>> {
        let mut result: BinaryHeap<Sample> = BinaryHeap::new();
        let mut timestamps = Vec::with_capacity(64);
        let mut values = Vec::with_capacity(64);

        self.select_raw(start_time, end_time, &mut timestamps, &mut values)?;
        for (ts, value) in timestamps.iter().zip(values.iter()) {
            result.push(Sample::new(*ts, *value));
        }

        Ok(result.into_sorted_vec())
    }

    pub fn select_raw(
        &self,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        let (index, _) = get_chunk_index(&self.chunks, start_time);
        let chunks = &self.chunks[index..];
        // Get overlapping data points from the compressed blocks.
        for chunk in chunks.iter() {
            let first = chunk.first_timestamp();
            if first > end_time {
                break;
            }
            chunk.get_range(first, end_time, timestamps, values)?;
        }

        Ok(())
    }

    pub fn iter<'a>(&'a self, start: Timestamp, end: Timestamp) -> impl Iterator<Item = Sample> + 'a{
        SampleIterator::new(self, start, end)
    }

    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.last_timestamp >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn is_older_than_retention(&self, timestamp: Timestamp) -> bool {
        let min_ts = self.get_min_timestamp();
        if timestamp < min_ts {
            return true;
        }
        false
    }

    pub fn trim(&mut self, start: Timestamp, end: Timestamp) -> TsdbResult<()> {
        if self.retention.is_zero() {
            return Ok(());
        }

        let min_timestamp = self.get_min_timestamp();
        let mut count = 0;
        let mut deleted_count = 0;

        for chunk in self
            .chunks
            .iter()
            .take_while(|&block| block.first_timestamp() < min_timestamp)
        {
            count += 1;
            deleted_count += chunk.num_samples();
        }

        if count > 0 {
            let _ = self.chunks.drain(0..count);
        }

        self.total_samples -= deleted_count as u64;

        Ok(())
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut deleted_samples = 0;
        // todo: tinyvec
        let mut indexes_to_delete = Vec::new();

        let (index, _) = get_chunk_index(&self.chunks, start_ts);
        let chunks = &mut self.chunks[index..];

        for (idx, chunk) in chunks.iter_mut().enumerate() {
            let chunk_first_ts = chunk.first_timestamp();
            let chunk_last_ts = chunk.last_timestamp();

            // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
            // larger than end_ts
            if chunk.is_empty() || chunk_first_ts > end_ts {
                // Having empty chunk means the series is empty
                break;
            }

            if chunk_last_ts < start_ts {
                continue;
            }

            let is_only_chunk =
                (chunk.num_samples() + deleted_samples) as u64 == self.total_samples;

            // Should we delete the entire chunk?
            // We assume at least one allocated chunk in the series
            if chunk.is_contained_by_range(start_ts, end_ts) && (!is_only_chunk) {
                deleted_samples += chunk.num_samples();
                indexes_to_delete.push(idx);
            } else {
                deleted_samples += chunk.remove_range(start_ts, end_ts)?;
            }
        }

        self.total_samples -= deleted_samples as u64;

        for idx in indexes_to_delete.iter().rev() {
            let _ = self.chunks.remove(*idx);
        }

        // Check if last timestamp deleted
        if end_ts >= self.last_timestamp && start_ts <= self.last_timestamp {
            match self.chunks.iter().last() {
                Some(chunk) => {
                    self.last_timestamp = chunk.last_timestamp();
                    self.last_value = chunk.last_value();
                }
                None => {
                    self.last_timestamp = 0;
                    self.last_value = f64::NAN;
                }
            }
        }

        Ok(deleted_samples)
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for chunk in self.chunks.iter() {
            size += chunk.size();
        }
        size
    }

    fn get_min_timestamp(&self) -> Timestamp {
        let retention_millis = self.retention.as_millis() as i64;
        if self.last_timestamp > retention_millis {
            self.last_timestamp - retention_millis
        } else {
            0
        }
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

fn get_chunk_index(chunks: &Vec<TimeSeriesChunk>, timestamp: Timestamp) -> (usize, bool) {
    return match chunks.binary_search_by(|probe| {
        if timestamp < probe.first_timestamp() {
            std::cmp::Ordering::Greater
        } else if timestamp > probe.last_timestamp() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    };
}

pub struct SampleIterator<'a> {
    series: &'a TimeSeries,
    timestamps: PooledTimestampVec,
    values: PooledValuesVec,
    chunk_index: usize,
    sample_index: usize,
    start: Timestamp,
    end: Timestamp,
    err: bool,
    first_iter: bool,
    filter: Option<Box<dyn Fn(&Sample) -> bool>>,
}

impl<'a> SampleIterator<'a> {
    fn new(series: &'a TimeSeries, start: Timestamp, end: Timestamp) -> Self {
        let (chunk_index, _) = get_chunk_index(&series.chunks, start);
        // estimate buffer size
        let size = if chunk_index < series.chunks.len() {
            series.chunks[chunk_index..]
                .iter()
                .take_while(|chunk| chunk.last_timestamp() >= end)
                .map(|chunk| chunk.num_samples())
                .min()
                .unwrap_or(4)
        } else {
            4
        };

        let timestamps = get_pooled_vec_i64(size);
        let values = get_pooled_vec_f64(size);

        Self {
            series,
            timestamps,
            values,
            chunk_index,
            sample_index: 0,
            start,
            end,
            err: false,
            first_iter: false,
            filter: None,
        }
    }

    fn with_filter<F>(series: &'a TimeSeries, start: Timestamp, end: Timestamp, filter: F) -> Self
    where
        F: Fn(&Sample) -> bool + 'static,
    {
        let mut iter = Self::new(series, start, end);
        iter.filter = Some(Box::new(filter));
        iter
    }

    fn next_chunk(&mut self) -> bool {
        if self.chunk_index >= self.series.chunks.len() {
            return false;
        }
        let chunk = &self.series.chunks[self.chunk_index];
        if chunk.first_timestamp() > self.end {
            return false;
        }
        self.chunk_index += 1;
        self.timestamps.clear();
        self.values.clear();
        if let Err(e) =
            chunk.get_range(self.start, self.end, &mut self.timestamps, &mut self.values)
        {
            self.err = true;
            return false;
        }
        self.sample_index = if self.first_iter {
            match self.timestamps.binary_search(&self.start) {
                Ok(idx) => idx,
                Err(idx) => idx,
            }
        } else {
            0
        };
        if let Some(filter) = &self.filter {
            let timestamps = &self.timestamps[self.sample_index..];
            let values = &self.values[self.sample_index..];

            // trim the timestamps by the filter function while retaining a list of thee
            // indices that were removed
            let mut indices_to_remove = AHashSet::new();
            for (idx, (ts, v)) in timestamps.iter().zip(values.iter()).enumerate() {
                let sample = Sample::new(*ts, *v);
                if !filter(&sample) {
                    indices_to_remove.insert(idx);
                }
            }

            let mut idx = 0;
            self.timestamps.retain(|ts| {
                let keep = indices_to_remove.contains(&idx);
                idx += 1;
                keep
            });

            idx = 0;
            self.values.retain(|ts| {
                let keep = indices_to_remove.contains(&idx);
                idx += 1;
                keep
            });

            if self.timestamps.is_empty() {
                // loop again
                return self.next_chunk();
            }
        }

        self.start = chunk.last_timestamp();
        self.first_iter = false;
        true
    }
}

// todo: implement next_chunk
impl<'a> Iterator for SampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sample_index >= self.timestamps.len() || self.first_iter {
            if !self.next_chunk() {
                return None;
            }
        }
        let timestamp = self.timestamps[self.sample_index];
        let value = self.values[self.sample_index];
        self.sample_index += 1;
        Some(Sample::new(timestamp, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts::constants::BLOCK_SIZE_FOR_TIME_SERIES;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        ts.append(100, 200.0).unwrap();

        assert_eq!(ts.get_last_chunk().len(), 1);
        let last_block_lock = ts.get_last_chunk();
        let time_series_data_points = &*last_block_lock
            .get_time_series_data_points()
            .read()
            .unwrap();
        let data_point = time_series_data_points.get(0).unwrap();
        assert_eq!(data_point.get_time(), 100);
        assert_eq!(data_point.get_value(), 200.0);
    }

    #[test]
    fn test_block_size_entries() {
        let mut ts = TimeSeries::new();
        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            ts.append(i as i64, i as f64).unwrap();
        }

        // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
        assert_eq!(ts.chunks.len(), 0);
        assert_eq!(ts.get_last_chunk().len(), BLOCK_SIZE_FOR_TIME_SERIES);

        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            let last_block = ts.get_last_chunk();
            let time_series_data_points =
                &*last_block.get_time_series_data_points().read().unwrap();
            let data_point = time_series_data_points.get(i).unwrap();
            assert_eq!(data_point.get_time(), i as u64);
            assert_eq!(data_point.get_value(), i as f64);
        }
    }
}
