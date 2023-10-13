use std::io::Write;
use q_compress::wrapped::Decompressor;
use crate::error::TsdbResult;
use crate::ts::chunks::compressed::common::PageMetadata;
use crate::ts::chunks::DataPage;
use super::{QuantileCompressedChunk};
use super::compress::{map_compression_err, read_metadata};
use crate::ts::chunks::compressed::common::serialize::read_usize;
use crate::ts::utils::trim_data;

pub struct DecompressorContext<'a> {
    data: &'a [u8],
    t_decompressor: Decompressor<i64>,
    v_decompressor: Decompressor<f64>,
}

impl<'a> DecompressorContext<'a> {
    fn new(data: &'a [u8]) -> TsdbResult<Self> {
        let mut t_decompressor = Decompressor::<i64>::default();

        // read timestamp metadata
        let compressed = read_metadata(&mut t_decompressor, data)?;
        let mut v_decompressor = Decompressor::<f64>::default();

        let _ = read_metadata(&mut v_decompressor, compressed)?;

        Ok(Self {
            data,
            t_decompressor,
            v_decompressor,
        })
    }

    fn get_page_data(
        &mut self,
        offset: usize,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<&[u8]> {
        let buf = &self.data[offset..];
        // we need to filter and append this data
        let (mut compressed, mut size) = read_usize(buf, "timestamp data size")?;

        let n = size;
        self.t_decompressor.write_all(&compressed[..size]).unwrap();
        let page_t = self.t_decompressor.data_page(n, size)
            .map_err(map_compression_err)?;

        compressed = &compressed[size..];

        (compressed, size) = read_usize(compressed, "value data size")?;
        self.v_decompressor.write_all(&compressed[..size]).unwrap();
        let page_v = self.v_decompressor.data_page(n, size)
            .map_err(map_compression_err)?;

        timestamps.extend(page_t);
        values.extend(page_v);
        Ok(compressed)
    }
}


pub struct CompressedPageIterator<'a> {
    context: DecompressorContext<'a>,
    pages: &'a [PageMetadata],
    timestamps: Vec<i64>,
    values: Vec<f64>,
    index: usize,
    end_index: usize,
    start: i64,
    end: i64,
    done: bool,
}

const DEFAULT_ITERATOR_BUF_SIZE: usize = 512;

impl<'a> CompressedPageIterator<'a> {
    pub fn new(
        compressed: &'a QuantileCompressedChunk,
        start: i64,
        end: i64,
    ) -> TsdbResult<CompressedPageIterator> {
        let pages = compressed.pages();

        let page_idx =  match pages.binary_search_by(|x| x.t_min.cmp(&start)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let end_page_idx = pages
            .iter()
            .rev()
            .position(|page| page.t_max <= end)
            .unwrap_or(0);

        let done = page_idx >= pages.len() || end_page_idx >= pages.len();
        let buf = compressed.buf();
        let context = DecompressorContext::new(buf)?;
        Ok(CompressedPageIterator {
            context,
            pages,
            index: page_idx,
            end_index: end_page_idx,
            start,
            end,
            timestamps: Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE),
            values: Vec::with_capacity(DEFAULT_ITERATOR_BUF_SIZE),
            done
        })
    }

    fn next_page(&mut self) -> Option<&PageMetadata> {
        if self.done {
            return None;
        }
        let page = &self.pages[self.index];
        if self.index >= self.pages.len() {
            self.done = true;
        } else {
            self.index += 1;
        }
        return Some(page);
    }

}

impl<'a> Iterator for CompressedPageIterator<'a> {
    type Item = DataPage<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.next_page();
        if page.is_none() {
            return None;
        }
        let page = page.unwrap();
        let offset = page.offset;

        // find a better way of dealing with the error
        let page_res = &self.context.get_page_data(offset, &mut self.timestamps, &mut self.values);
        match page_res {
            Ok(_) => {}
            Err(_) => {
                return None;
            }
        }
        let timestamps = &self.timestamps[0..];
        let values = &self.values[0..];

        // filter out timestamps out of range
        trim_data(timestamps, values, self.start, self.end);

        Some(DataPage {
            timestamps,
            values,
        })
    }
}
