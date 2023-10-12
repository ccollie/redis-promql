use super::compress::compress_series;
use super::CompressedPageIterator;
use crate::ts::chunks::uncompressed::UncompressedChunk;
use crate::ts::chunks::{DataPage, TimesSeriesBlock};
use crate::ts::utils::trim_data;
use crate::ts::{handle_duplicate_sample, DuplicatePolicy, DuplicateStatus, Sample, Timestamp};
use serde::{Deserialize, Serialize};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::chunks::compressed::common::{CompressedData, PageMetadata};


#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileCompressedChunk {
    pub compressed: CompressedData
}

impl QuantileCompressedChunk {
    pub fn new(timestamps: &[i64], values: &[f64]) -> TsdbResult<Self> {
        // todo: calculate init buffer size
        // todo: ensure timestamps.len() == values.len()
        let reserved_size = estimate_compressed_size(timestamps.len());
        let chunk = create_chunk(timestamps, values, reserved_size)?;
        Ok(QuantileCompressedChunk {
            compressed: chunk,
        })
    }

    pub fn len(&self) -> usize {
        self.compressed.count
    }
    pub fn is_empty(&self) -> bool {
        self.compressed.count == 0
    }
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.compressed.overlaps(start, end)
    }
    pub fn get_timestamp_range(&self) -> Option<(i64, i64)> {
        self.compressed.get_timestamp_range()
    }

    pub fn buf(&self) -> &[u8] {
        &self.compressed.data
    }

    pub fn pages(&self) -> &[PageMetadata] {
        &self.compressed.pages
    }

    /// Get the data points in the specified range (both range_start_time and range_end_time inclusive).
    pub fn get_samples_in_range(&self, start_time: i64, end_time: i64) -> TsdbResult<Vec<Sample>> {
        let mut result = Vec::new();

        for DataPage { timestamps, values } in self.range_iter(start_time, end_time)? {
            for (time, val) in timestamps.iter().zip(values.iter()) {
                result.push(Sample::new(*time, *val));
            }
        }

        Ok(result)
    }
}

impl TimesSeriesBlock for QuantileCompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.compressed.first_timestamp()
    }
    fn last_timestamp(&self) -> Timestamp {
        self.compressed.last_timestamp()
    }
    fn last_value(&self) -> f64 {
        self.compressed.last_value()
    }
    fn num_samples(&self) -> usize {
        self.compressed.count
    }
    fn size(&self) -> usize {
        self.compressed.size()
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        // todo: better capacity calculation.
        // Idea:: iterated over pages and calculate total size
        // / avg(t-max - tmin)
        let mut ts_buffer: Vec<i64> = Vec::with_capacity(1024);
        let mut value_buffer: Vec<f64> = Vec::with_capacity(1024);

        let max_ts = self.last_timestamp();

        for DataPage { timestamps, values } in self.range_iter(0, max_ts)? {
            let first_ts = timestamps[0];
            let last_ts = timestamps[timestamps.len() - 1];
            let non_overlapping = first_ts >= start_ts && last_ts <= end_ts;
            // fast path
            let (stamps_, values_) = if non_overlapping {
                (timestamps, values)
            } else {
                // slow path
                trim_data(timestamps, values, start_ts, end_ts)
            };
            ts_buffer.extend_from_slice(stamps_);
            value_buffer.extend_from_slice(values_);
        }
        //

        let deleted_count = self.num_samples() - ts_buffer.len();
        if deleted_count == 0 {
            return Ok(0);
        }

        // todo: calculate initial capacity
        let compressed = create_chunk( &ts_buffer, &value_buffer, 1024)?;
        self.compressed = compressed;

        Ok(deleted_count)
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let num_samples = self.num_samples();

        let mut duplicate_found = false;

        // this compressed method does not do streaming compressed, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        let mut acc_timestamps = Vec::with_capacity(num_samples + 1);
        let mut acc_values = Vec::with_capacity(num_samples + 1);

        for page in self.range_iter(0, i64::MAX)? {
            let timestamps = page.timestamps;
            let values = page.values;

            if page.contains_timestamp(ts) {
                let dupe_pos = page.timestamps.iter().position(|&t| t == ts);
                if let Some(pos) = dupe_pos {
                    let old_sample = Sample {
                        timestamp: page.timestamps[pos],
                        value: page.values[pos],
                    };
                    let cr = handle_duplicate_sample(dp_policy, old_sample, sample);
                    if cr != DuplicateStatus::Ok {
                        return Err(TsdbError::DuplicateSample(sample.timestamp.to_string()));
                    }
                    duplicate_found = true;
                    // value changed. write it to the accumulator
                    acc_values.extend_from_slice(&values[0..pos]);
                    acc_values.push(sample.value);
                    acc_values.extend_from_slice(&values[pos + 1..]);
                    acc_timestamps.extend(timestamps);
                    continue;
                }
            }
            acc_timestamps.extend(timestamps);
            acc_values.extend(values);
        }

        let mut size = acc_timestamps.len();
        if duplicate_found {
            size -= 1;
        }
        // todo: specify config options
        self.compressed = create_chunk(&acc_timestamps, &acc_values, 1024)?;

        Ok(size)
    }

    fn range_iter(
        &self,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<Box<dyn Iterator<Item = DataPage> + '_ >> {
        let iter = CompressedPageIterator::new(self, start_ts, end_ts)?;
        Ok(Box::new(iter))
    }

    fn split(&mut self) -> TsdbResult<Self> {
        let mid = self.num_samples() / 2;

        // this compressed method does not do streaming compressed, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        let mut acc_timestamps = vec![];
        let mut acc_values = vec![];

        let reserve_size = mid;

        let mut left: Option<Self> = None;

        for page in self.range_iter(0, i64::MAX)? {
            let timestamps = page.timestamps;
            let values = page.values;

            // slight optimization. if the first iteration gets us at least half of the data points,
            // we can skip one round of allocations
            if left.is_none() && timestamps.len() >= mid {
                let (left_ts, right_ts) = timestamps.split_at(mid);
                let (left_values, right_values) = values.split_at(mid);
                acc_timestamps.extend_from_slice(right_ts);
                acc_values.extend_from_slice(right_values);
                let compressed = create_chunk(left_ts, left_values, reserve_size)?;
                left = Some(QuantileCompressedChunk { compressed } );
                continue;
            }

            acc_timestamps.extend(timestamps);
            acc_values.extend(values);
        }

        // todo: specify config options
        let chunk = create_chunk(&acc_timestamps, &acc_values, reserve_size)?;
        Ok(
            QuantileCompressedChunk {
                compressed: chunk,
            }
        )
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        todo!()
    }
}

impl Eq for QuantileCompressedChunk {}

impl TryFrom<&UncompressedChunk> for QuantileCompressedChunk {
    type Error = TsdbError;

    /// Compress the given time series block.
    fn try_from(time_series_block: &UncompressedChunk) -> Result<Self, Self::Error> {
        if time_series_block.is_empty() {
            return Err(TsdbError::EmptyTimeSeriesBlock());
        }

        let chunk = QuantileCompressedChunk::new(
            &time_series_block.timestamps[0..],
            &time_series_block.values,
        )?;

        Ok(chunk)
    }
}

fn estimate_compressed_size(count: usize) -> usize {
    (count * 16) / 6
}

fn create_chunk(
    timestamps: &[i64],
    values: &[f64],
    reserve_size: usize,
) -> TsdbResult<CompressedData> {
    let mut data = Vec::with_capacity(reserve_size);
    let pages = compress_series(&mut data, timestamps, values)?;
    data.shrink_to_fit();
    let last_value = values[values.len() - 1];
    Ok(CompressedData {
        data,
        pages,
        count: timestamps.len(),
        last_value,
    })
}