use super::{merge_by_capacity, validate_chunk_size, Chunk, ChunkCompression, ChunkSampleIterator, Sample, TimeSeriesChunk, TimeSeriesOptions};
use crate::common::decimal::{round_to_significant_digits, RoundDirection};
use crate::common::types::{Label, Timestamp};
use crate::common::METRIC_NAME_LABEL;
use crate::error::{TsdbError, TsdbResult};
use crate::module::types::ValueFilter;
use crate::storage::constants::{DEFAULT_CHUNK_SIZE_BYTES, SPLIT_FACTOR};
use crate::storage::uncompressed_chunk::UncompressedChunk;
use crate::storage::utils::format_prometheus_metric_name;
use crate::storage::DuplicatePolicy;
use get_size::GetSize;
use metricsql_common::hash::IntMap;
use smallvec::SmallVec;
use std::mem::size_of;
use std::time::Duration;
use valkey_module::error::GenericError;
use valkey_module::raw;

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed.
#[derive(Clone, Debug, PartialEq)]
#[derive(GetSize)]
pub struct TimeSeries {
    /// fixed internal id used in indexing
    pub id: u64,

    /// Name of the metric
    /// For example, given `http_requests_total{method="POST", status="500"}`
    /// the metric name is `http_requests_total`, and the labels are method="POST" and status="500"
    /// Metric names must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
    pub metric_name: String,
    pub labels: Vec<Label>,

    pub retention: Duration,
    pub dedupe_interval: Option<Duration>,
    pub duplicate_policy: DuplicatePolicy,
    pub chunk_compression: ChunkCompression,
    pub significant_digits: Option<u8>,
    pub chunk_size_bytes: usize,
    pub chunks: Vec<TimeSeriesChunk>,

    // meta
    pub total_samples: usize,
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
            labels: vec![],
            retention: Default::default(),
            duplicate_policy: DuplicatePolicy::KeepLast,
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            dedupe_interval: Default::default(),
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
            significant_digits: None
        }
    }

    pub fn with_options(options: TimeSeriesOptions) -> TsdbResult<Self> {
        let mut options = options;
        let mut res = Self::new();
        if let Some(chunk_size) = options.chunk_size {
            validate_chunk_size(chunk_size)?;
            res.chunk_size_bytes = chunk_size;
        }
        res.duplicate_policy = options.duplicate_policy.unwrap_or(DuplicatePolicy::KeepLast);
        // res.chunk_compression = options.encoding.unwrap_or(Encoding::Compressed);
        if let Some(metric_name) = options.metric_name {
            // todo: validate against regex
            res.metric_name = metric_name;
        }
        if let Some(retention) = options.retention {
            res.retention = retention;
        }
        if let Some(dedupe_interval) = options.dedupe_interval {
            res.dedupe_interval = Some(dedupe_interval);
        }

        // todo: make sure labels are sorted and dont contain __name__
        let label = options.labels
            .iter()
            .find(|x| x.name == METRIC_NAME_LABEL)
            .ok_or(TsdbError::InvalidMetric("NONE".to_string()))?; // better error

        options.metric_name = Some(label.value.clone());
        options.labels.retain(|x| x.name != METRIC_NAME_LABEL);

        res.labels = options.labels;
        Ok(res)
    }

    pub fn is_empty(&self) -> bool {
        self.total_samples == 0
    }

    /// Get the full metric name of the time series, including labels in Prometheus format.
    /// For example,
    ///
    /// `http_requests_total{method="POST", status="500"}`
    ///
    /// Note that for our internal purposes, we store the metric name and labels separately, and
    /// assume that the labels are sorted by name.
    pub fn prometheus_metric_name(&self) -> String {
        format_prometheus_metric_name(&self.metric_name, &self.labels)
    }

    pub fn label_value(&self, name: &str) -> Option<&String> {
        if name == METRIC_NAME_LABEL {
            return Some(&self.metric_name);
        }
        if let Some(label) = self.labels.iter().find(|x| x.name == name) {
            return Some(&label.value);
        }
        None
    }

    fn adjust_value(&mut self, value: f64) -> f64 {
        if let Some(significant_digits) = self.significant_digits {
            // todo: limit digits to a max, for ex.
            // https://stackoverflow.com/questions/65719216/why-does-rust-only-use-16-significant-digits-for-f64-equality-checks
            round_to_significant_digits(value, significant_digits as i32, RoundDirection::Up)
        } else {
            value
        }
    }

    pub fn add(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<()> {
        if self.is_older_than_retention(ts) {
            return Err(TsdbError::SampleTooOld);
        }

        if !self.is_empty() {
            let last_ts = self.last_timestamp;
            if let Some(dedup_interval) = self.dedupe_interval {
                let millis = dedup_interval.as_millis() as i64;
                if millis > 0 && (ts - last_ts) < millis {
                    // todo: use policy to derive a value to insert
                    let msg = "New sample encountered in less than dedupe interval";
                    return Err(TsdbError::DuplicateSample(msg.to_string()));
                }
            }

            if ts <= last_ts {
                let _ = self.upsert_sample(ts, value, dp_override);
                return Ok(());
            }
        }

        self.add_sample(ts, value)
    }

    pub(super) fn add_sample(&mut self, time: Timestamp, value: f64) -> TsdbResult<()> {
        let value = self.adjust_value(value);
        let sample = Sample {
            timestamp: time,
            value,
        };

        let was_empty = self.is_empty();
        let chunk = self.get_last_chunk();
        match chunk.add_sample(&sample) {
            Err(TsdbError::CapacityFull(_)) => {
                self.add_chunk_with_sample(&sample)?;
            },
            Err(e) => return Err(e),
            _ => {},
        }
        if was_empty {
            self.first_timestamp = time;
        }

        self.last_value = value;
        self.last_timestamp = time;
        self.total_samples += 1;
        Ok(())
    }

    #[inline]
    fn append(&mut self, sample: &Sample) -> TsdbResult<bool> {
        let chunk = self.get_last_chunk();
        match chunk.add_sample(sample) {
            Err(TsdbError::CapacityFull(_)) => Ok(false),
            Err(e) => Err(e),
            _ => Ok(true),
        }
    }

    /// Add a new chunk and compact the current chunk if necessary.
    fn add_chunk_with_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        let chunks_len = self.chunks.len();

        // The last block is full. So, compress it and append it time_series_block_compressed.
        let chunk_size = self.chunk_size_bytes;
        let compression = self.chunk_compression;
        let min_timestamp = self.get_min_timestamp();
        let duplicate_policy = self.duplicate_policy;

        // arrrgh! rust treats vecs as a single unit wrt borrowing, but the following iterator trick
        // seems to work
        let mut iter = self.chunks.iter_mut().rev();
        let last_chunk = iter.next().unwrap();

        // check if previous block has capacity, and if so merge into it
        if let Some(prev_chunk) = iter.next() {
            if let Some(deleted_count) = merge_by_capacity(
                prev_chunk,
                last_chunk,
                min_timestamp,
                duplicate_policy,
            )? {
                self.total_samples -= deleted_count;
                last_chunk.add_sample(sample)?;
                return Ok(());
            }
        }

        // if the last chunk is uncompressed, create a new compressed block and move samples to it
        if let TimeSeriesChunk::Uncompressed(uncompressed_chunk) = last_chunk {
            let new_chunk = TimeSeriesChunk::new(
                compression,
                chunk_size,
                &uncompressed_chunk.timestamps,
                &uncompressed_chunk.values,
            )?;

            // clear last chunk for reuse
            last_chunk.clear();

            // insert new chunk before last block
            self.chunks.insert(chunks_len - 1, new_chunk);
            // res
        } else {
            let mut new_chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::with_max_size(
                self.chunk_size_bytes,
            ));
            new_chunk.add_sample(sample)?;
            self.chunks.push(new_chunk);

            return Ok(());
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
        let dp_policy = dp_override.unwrap_or(
            self.duplicate_policy
        );

        let value = self.adjust_value(value);

        let (size, new_chunk) = {
            let max_size = self.chunk_size_bytes;
            let (pos, _) = get_chunk_index(&self.chunks, timestamp);
            let chunk = self.chunks.get_mut(pos).unwrap();
            Self::handle_upsert(chunk, timestamp, value, max_size, dp_policy)?
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

    pub fn chunks_range_indexes(&self, start_timestamp: Timestamp, end_timestamp: Timestamp) -> (usize, usize) {
        let (first_index, _) = get_chunk_index(&self.chunks, start_timestamp);
        let (mut last_index, last_found) = get_chunk_index(&self.chunks, end_timestamp);
        if last_index == self.chunks.len() && !last_found   {
            last_index = self.chunks.len() - 1;
        }
        (first_index, last_index)
    }

    /// Get the time series between given start and end time (both inclusive).
    /// todo: return a SeriesSlice or SeriesData so we don't realloc
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Sample> {
        self.range_iter(start_time, end_time).collect()
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

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() || timestamps.is_empty() {
            Ok(vec![])
        } else {
            let mut samples = Vec::with_capacity(timestamps.len());

            // partition timestamps by chunk
            let mut map: IntMap<usize, SmallVec<Timestamp, 6>> = Default::default();
            for ts in timestamps.iter() {
                let (index, _found) = get_chunk_index(&self.chunks, *ts);
                if index >= self.chunks.len() {
                    continue;
                }
                map.entry(index).or_default().push(*ts);
            }

            for (index, _ts) in map.iter() {
                let chunk = &self.chunks[*index];
                let sub_samples = chunk.samples_by_timestamps(timestamps)?;
                samples.extend(sub_samples);
            }

            samples.sort_by(|x, y| x.timestamp.cmp(&y.timestamp));

            Ok(samples)
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        SeriesSampleIterator::new(self, self.first_timestamp, self.last_timestamp, &None, &None)
    }

    pub fn range_iter(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> impl Iterator<Item = Sample> + '_ {
        SeriesSampleIterator::new(self, start, end, &None, &None)
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

    pub fn trim(&mut self) -> TsdbResult<()> {
        if self.retention.is_zero() || self.is_empty() {
            return Ok(());
        }

        let min_timestamp = self.get_min_timestamp();
        let mut count = 0;
        let mut deleted_count = 0;

        for chunk in self
            .chunks
            .iter()
            .take_while(|&block| block.last_timestamp() <= min_timestamp)
        {
            count += 1;
            deleted_count += chunk.num_samples();
        }

        if count > 0 {
            let _ = self.chunks.drain(0..count);
            self.total_samples -= deleted_count;
        }

        // now deal with partials (a chunk with only some expired items). There should be at most 1
        if let Some(chunk) = self.chunks.first_mut() {
            if chunk.first_timestamp() > min_timestamp {
                let deleted = chunk.remove_range(0, min_timestamp)?;
                self.total_samples -= deleted;
            }
        }

        Ok(())
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut deleted_samples = 0;
        // todo: tinyvec
        let mut indexes_to_delete = Vec::new();

        let (index, _) = get_chunk_index(&self.chunks, start_ts);
        let chunks = &mut self.chunks[index..];

        // Todo: although many chunks may be deleted, only a max of 2 will be modified, so
        // we can try to merge it with the next chunk

        for (idx, chunk) in chunks.iter_mut().enumerate() {
            let chunk_first_ts = chunk.first_timestamp();

            // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
            // larger than end_ts
            if chunk.is_empty() || chunk_first_ts > end_ts {
                // Having empty chunk means the series is empty
                break;
            }

            let is_only_chunk =
                (chunk.num_samples() + deleted_samples) == self.total_samples;

            // Should we delete the entire chunk?
            // We assume at least one allocated chunk in the series
            if chunk.is_contained_by_range(start_ts, end_ts) && (!is_only_chunk) {
                deleted_samples += chunk.num_samples();
                indexes_to_delete.push(index + idx);
            } else {
                deleted_samples += chunk.remove_range(start_ts, end_ts)?;
            }
        }

        self.total_samples -= deleted_samples;

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

    pub fn data_size(&self) -> usize {
        self.chunks.iter().map(|x| x.size()).sum()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() +
            self.get_heap_size()
    }

    pub(crate) fn get_min_timestamp(&self) -> Timestamp {
        if self.retention.is_zero() {
            return 0;
        }
        let retention_millis = self.retention.as_millis() as i64;
        (self.last_timestamp - retention_millis).min(0)
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.id);
        raw::save_string(rdb, &self.metric_name);
        raw::save_unsigned(rdb, self.labels.len() as u64);
        for label in self.labels.iter() {
            raw::save_string(rdb, &label.name);
            raw::save_string(rdb, &label.value);
        }
        raw::save_unsigned(rdb, self.retention.as_secs());
        // todo: how to mark as optional ???
        if let Some(interval) = self.dedupe_interval {
            raw::save_unsigned(rdb, interval.as_secs());
        } else {
            raw::save_unsigned(rdb, 0);
        }
        raw::save_unsigned(rdb, self.dedupe_interval.map(|x| x.as_secs()).unwrap_or(0));
        raw::save_unsigned(rdb, self.duplicate_policy.as_u8() as u64);
        raw::save_unsigned(rdb, self.chunk_compression as u64);
        raw::save_unsigned(rdb, self.significant_digits.unwrap_or(255) as u64);
        raw::save_unsigned(rdb, self.chunk_size_bytes as u64);
        raw::save_unsigned(rdb, self.chunks.len() as u64);
        for chunk in self.chunks.iter() {
            chunk.rdb_save(rdb);
        }
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: i32) -> *mut std::ffi::c_void {
        if let Ok(series) = Self::load_internal(rdb, _encver) {
            Box::into_raw(Box::new(series)) as *mut std::ffi::c_void
        } else {
            std::ptr::null_mut()
        }
    }

     fn load_internal(rdb: *mut raw::RedisModuleIO, _encver: i32) -> Result<Self, valkey_module::error::Error> {
        let id = raw::load_unsigned(rdb)?;
        let metric_name = raw::load_string(rdb)?.into();
        let labels_len = raw::load_unsigned(rdb)? as usize;
        let mut labels = Vec::with_capacity(labels_len);
        for _ in 0..labels_len {
            let name = raw::load_string(rdb)?;
            let value = raw::load_string(rdb)?;
            labels.push(Label { name: name.into(), value: value.into() });
        }
        let retention = Duration::from_secs(raw::load_unsigned(rdb)?);
        let dedupe_interval = if let Ok(interval) = raw::load_unsigned(rdb) {
            if interval == 0 {
                None
            } else {
                Some(Duration::from_secs(interval))
            }
        } else {
            None
        };
        let duplicate_policy = DuplicatePolicy::try_from(raw::load_unsigned(rdb)? as u8
        ).map_err(|_| valkey_module::error::Error::Generic(
            GenericError::new("Invalid duplicate policy")
        ))?;

        let chunk_compression = ChunkCompression::try_from(
            raw::load_unsigned(rdb)? as u8
        ).map_err(|_| valkey_module::error::Error::Generic(
            GenericError::new("Invalid chunk compression")
        ))?;

        let significant_digits = raw::load_unsigned(rdb)? as u8;
        let chunk_size_bytes = raw::load_unsigned(rdb)? as usize;
        let chunks_len = raw::load_unsigned(rdb)? as usize;
        let mut chunks = Vec::with_capacity(chunks_len);
        let mut last_value = f64::NAN;
        let mut total_samples: usize = 0;
        let mut first_timestamp = 0;
        let mut last_timestamp = 0;

        for _ in 0..chunks_len {
            let chunk = TimeSeriesChunk::rdb_load(rdb)?;
            last_value = chunk.last_value();
            total_samples += chunk.num_samples();
            if first_timestamp == 0 {
                first_timestamp = chunk.first_timestamp();
            }
            last_timestamp = last_timestamp.max(chunk.last_timestamp());
            chunks.push(chunk);
        }

        let ts = TimeSeries {
            id,
            metric_name,
            labels,
            retention,
            dedupe_interval,
            duplicate_policy,
            chunk_compression,
            significant_digits: if significant_digits == 255 { None } else { Some(significant_digits) },
            chunk_size_bytes,
            chunks,
            total_samples,
            first_timestamp,
            last_timestamp,
            last_value,
        };

        // ts.update_meta();
         // add to index
        Ok(ts)
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            id: 0,
            metric_name: "".to_string(),
            labels: vec![],
            retention: Default::default(),
            duplicate_policy: DuplicatePolicy::KeepLast,
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            dedupe_interval: Default::default(),
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
            significant_digits: None
        }
    }
}

/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_chunk_index(chunks: &[TimeSeriesChunk], timestamp: Timestamp) -> (usize, bool) {
    let len = chunks.len();
    let first = chunks[0].first_timestamp();
    let last = chunks[len - 1].last_timestamp();
    if timestamp <= first {
        return (0, false);
    }
    if timestamp > last {
        return (len, false);
    }
    match chunks.binary_search_by(|probe| {
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
    }
}

pub type BoxedSampleIterator<'a> = Box<dyn Iterator<Item = Sample> + 'a>;

pub(crate) struct SeriesSampleIterator<'a> {
    series: &'a TimeSeries,
    curr_iter: Option<ChunkSampleIterator<'a>>,
    chunk_index: usize,
    value_filter: &'a Option<ValueFilter>,
    ts_filter: &'a Option<Vec<Timestamp>>, // box instead
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
}

impl<'a> SeriesSampleIterator<'a> {
    pub(crate) fn new(series: &'a TimeSeries,
                      start: Timestamp,
                      end: Timestamp,
                      value_filter: &'a Option<ValueFilter>,
                      ts_filter: &'a Option<Vec<Timestamp>>, // box instead
    ) -> Self {
        let (chunk_index, _) = get_chunk_index(&series.chunks, start);

        let mut result = Self {
            series,
            curr_iter: None,
            chunk_index,
            start,
            end,
            value_filter,
            ts_filter,
        };

        result.curr_iter= result.get_iter(start, end);
        result
    }

    fn get_iter(&mut self, start: Timestamp, end: Timestamp) -> Option<ChunkSampleIterator<'a>> {
        if let Some(chunk) = self.series.chunks.get(self.chunk_index) {
            self.chunk_index += 1;
            if chunk.first_timestamp() <= end {
                let new_iter = ChunkSampleIterator::new(chunk, start, end, &self.value_filter, self.ts_filter);
                self.start = chunk.last_timestamp();

                return Some(new_iter)
            }
        }
        None
    }

}

// todo: implement next_chunk
impl<'a> Iterator for SeriesSampleIterator<'a> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut curr_iter) = self.curr_iter {
            match curr_iter.next() {
                Some(sample) => {
                    Some(sample)
                }
                None => {
                    if let Some(mut iter)= self.get_iter(self.start, self.end){
                        let result = iter.next();
                        self.curr_iter = Some(iter);
                        result
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::generators::{generate_series_data, GeneratorOptions};

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None).unwrap();

        assert_eq!(ts.get_last_chunk().num_samples(), 1);
        let last_block = ts.get_last_chunk();
        let samples = last_block.get_samples(0, 1000).unwrap();

        let data_point = samples.get(0).unwrap();
        assert_eq!(data_point.timestamp, 100);
        assert_eq!(data_point.value, 200.0);
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 100);
    }

    #[test]
    fn test_1000_entries() {
        let mut ts = TimeSeries::new();
        let mut options = GeneratorOptions::default();
        options.samples = 1000;
        let data = generate_series_data(&options).unwrap();
        for sample in data.iter() {
            ts.add(sample.timestamp, sample.value, None).unwrap();
        }

        assert_eq!(ts.total_samples, 1000);
        assert_eq!(ts.first_timestamp, data.first_timestamp());
        assert_eq!(ts.last_timestamp, data.last_timestamp());

        let mut i: usize = 0;
        for sample in ts.iter() {
            assert_eq!(sample.timestamp, data.timestamps[i]);
            assert_eq!(sample.value, data.values[i]);
            i += 1;
        }
    }

    const BLOCK_SIZE_FOR_TIME_SERIES: usize = 1000;

    #[test]
    fn test_block_size_entries() {
        let mut ts = TimeSeries::new();
        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            ts.add(i as i64, i as f64, None).unwrap();
        }

        // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
        assert_eq!(ts.chunks.len(), 2);
        assert_eq!(
            ts.get_last_chunk().num_samples(),
            BLOCK_SIZE_FOR_TIME_SERIES
        );

        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            let last_block = ts.get_last_chunk();
            let samples = last_block.get_samples(0, 7000).unwrap();
            let data_point = samples.get(i).unwrap();
            assert_eq!(data_point.timestamp, i as i64);
            assert_eq!(data_point.value, i as f64);
        }
    }

    #[test]
    fn test_last_chunk_overflow() {
        todo!();

    }
}
