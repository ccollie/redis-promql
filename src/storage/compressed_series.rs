use std::collections::BinaryHeap;

use get_size::GetSize;
use serde::{Deserialize, Serialize};

use crate::common::types::Timestamp;
use crate::error::TsdbResult;
use crate::module::commands::RangeAlignment::Default;
use crate::storage::{Chunk, DuplicatePolicy, Sample, SPLIT_FACTOR, TimeSeriesChunk};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RangeIndex {
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    pub count: usize,
    pub offset: usize,
}

impl RangeIndex {
    pub fn is_contained_by_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.start_ts >= start_ts && self.end_ts <= end_ts
    }
}
/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[derive(GetSize)]
pub struct CompressedSeriesSlice {
    pub chunk_size_bytes: usize,

    // meta
    pub page_size: Option<usize>,
    pub total_samples: usize,
    pub first_timestamp: Timestamp,
    pub last_timestamp: Timestamp,
    pub last_value: f64,
    /// index of time start of range in the compressed buffer
    pub segment_index: Vec<RangeIndex>,
    /// compressed slice data
    pub data: Vec<u8>,
}

impl CompressedSeriesSlice {
    pub fn new(page_size: Option<usize>, last_value: f64) -> Self {
        CompressedSeriesSlice {
            total_samples: 0,
            chunk_size_bytes,
            page_size,
            first_timestamp,
            last_timestamp,
            last_value,
            segment_index: Default::default(),
            data: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total_samples == 0
    }

    pub(crate) fn upsert_sample(
        &mut self,
        timestamp: Timestamp,
        value: f64,
    ) -> TsdbResult<usize> {

        let (size, new_chunk) = {
            let max_size = self.chunk_size_bytes;
            let (pos, _) = crate::storage::time_series::get_chunk_index(&self.chunks, timestamp);
            let chunk = self.chunks.get_mut(pos).unwrap();
            Self::handle_upsert(chunk, timestamp, value, max_size)?
        };

        if let Some(new_chunk) = new_chunk {
            self.trim()?;
            // todo: how to avoid this ? Since chunks are currently stored inline this can cause a lot of
            // moves
            let ts = new_chunk.first_timestamp();
            let insert_at = self.chunks.binary_search_by(|chunk| {
                chunk.first_timestamp().cmp(&ts)
            }).unwrap_or_else(|i| i);
            self.chunks.insert(insert_at, new_chunk);
        }

        self.total_samples += size;
        if timestamp == self.last_timestamp {
            self.last_value = value;
        }

        Ok(size)
    }

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

    /// Get the time series between given start and end time (both inclusive).
    /// todo: return a SeriesSlice or SeriesData so we don't realloc
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> TsdbResult<Vec<Sample>> {
        let mut result: BinaryHeap<Sample> = BinaryHeap::new();
        // todo: possibly used pooled vecs
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
        if self.is_empty() {
            return Ok(());
        }
        let (index, _) = get_segment_index(&self.segment_index, start_time);
        let chunks = &self.segment_index[index..];
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

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut deleted_samples = 0;
        // todo: tinyvec
        let mut indexes_to_delete = Vec::new();

        let (index, _) = get_segment_index(&self.segment_index, start_ts);
        let index_items = &mut self.segment_index[index..];

        // Todo: although many chunks may be deleted, only a max of 2 will be modified, so
        // we can try to merge it with the next chunk

        for (idx, index_entry) in index_items.iter_mut().enumerate() {
            let chunk_first_ts = index_entry.start_ts;

            // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
            // larger than end_ts
            if index_entry.is_empty() || chunk_first_ts > end_ts {
                // Having empty chunk means the series is empty
                break;
            }

            let is_only_chunk =
                (index_entry.num_samples() + deleted_samples) == self.total_samples;

            // Should we delete the entire chunk?
            // We assume at least one allocated chunk in the series
            if index_entry.is_contained_by_range(start_ts, end_ts) && (!is_only_chunk) {
                deleted_samples += index_entry.num_samples();
                indexes_to_delete.push(index + idx);
            } else {
                deleted_samples += index_entry.remove_range(start_ts, end_ts)?;
            }
        }

        self.total_samples -= deleted_samples;

        for idx in indexes_to_delete.iter().rev() {
            let _ = self.segment_index.remove(*idx);
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
}

/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_segment_index(chunks: &[RangeIndex], timestamp: Timestamp) -> (usize, bool) {
    use std::cmp::Ordering;
    let len = chunks.len();
    let first = chunks[0].start_ts;
    let last = chunks[len - 1].start_ts;
    if timestamp <= first {
        return (0, false);
    }
    if timestamp >= last {
        return (len - 1, false);
    }
    return match chunks.binary_search_by(|probe| {
        if timestamp < probe.first_timestamp() {
            Ordering::Greater
        } else if timestamp > probe.last_timestamp() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    };
}
