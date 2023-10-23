use crate::common::types::Timestamp;

pub struct DataBlock<'a> {
    pub timestamps: &'a Vec<i64>,
    pub values: &'a Vec<f64>,
    pub next_idx: usize,
}

impl<'a> DataBlock<'a> {
    pub fn new(timestamps: &'a Vec<i64>, values: &'a Vec<f64>) -> Self {
        Self { timestamps, values, next_idx: 0 }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn max_timestamp(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn min_timestamp(&self) -> i64 {
        self.timestamps[0]
    }
}

// merge_blocks merges ib1 and ib2 to ob.
pub fn merge_blocks(ob: &mut DataBlock, ib1: &mut DataBlock, ib2: &mut DataBlock, retention_deadline: i64) -> usize {

    let mut rows_deleted = skip_samples_outside_retention(ib1, retention_deadline) +
    skip_samples_outside_retention(ib2, retention_deadline);

    if ib1.max_timestamp() < ib2.min_timestamp() {
        // Fast path - ib1 values have smaller timestamps than ib2 values.
        append_rows(ob, ib1);
        append_rows(ob, ib2);
        return rows_deleted
    }
    if ib2.max_timestamp() < ib1.min_timestamp() {
        // Fast path - ib2 values have smaller timestamps than ib1 values.
        append_rows(ob, ib2);
        append_rows(ob, ib1);
        return rows_deleted;
    }
    if ib1.next_idx >= ib1.len() {
        append_rows(ob, ib2);
        return rows_deleted;
    }
    if ib2.next_idx >= ib2.len() {
        append_rows(ob, ib1);
        return rows_deleted;
    }
    loop {
        let mut i = ib1.next_idx;
        let ts2 = ib2.timestamps[ib2.next_idx];
        while i < ib1.len() && ib1.timestamps[i] <= ts2 {
            i += 1;
        }
        ob.timestamps.extend_from_slice(&ib1.timestamps[ib1.next_idx .. i]);
        ob.values.extend_from_slice(&ib1.values[ib1.next_idx .. i]);
        ib1.next_idx = i;
        if ib1.next_idx >= ib1.timestamps.len() {
            append_rows(ob, ib2);
            return rows_deleted;
        }
        (ib1, ib2) = (ib2, ib1)
    }
}

fn skip_samples_outside_retention(b: &mut DataBlock, retention_deadline: Timestamp) -> usize {
    if b.min_timestamp() >= retention_deadline {
        // Fast path - the block contains only samples with timestamps bigger than retention_deadline.
        return 0;
    }
    let timestamps = b.timestamps;
    let mut next_idx = b.next_idx;
    let next_idx_orig = next_idx;
    while next_idx < b.len() && timestamps[next_idx] < retention_deadline {
        next_idx += 1;
    }
    let n = next_idx - next_idx_orig;
    if n > 0 {
        b.next_idx = next_idx
    }
    n
}

fn append_rows(ob: &mut DataBlock, ib: &mut DataBlock) {
    ob.timestamps.extend_from_slice(&ib.timestamps[ib.next_idx .. ]);
    ob.values.extend_from_slice(&ib.values[ib.next_idx..]);
}