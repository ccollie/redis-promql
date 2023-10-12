use crate::ts::Timestamp;

pub fn trim_data<'a>(timestamps: &'a [i64], values: &'a [f64], start_ts: Timestamp, end_ts: Timestamp) -> (&'a [i64], &'a [f64]) {
    let stamps = &timestamps[0..];
    let start_idx = match stamps.binary_search(&start_ts) {
        Ok(idx) => idx,
        Err(idx) => idx,
    };

    let end_idx = match stamps.binary_search(&end_ts) {
        Ok(idx) => idx,
        Err(idx) => idx,
    };

    if start_idx > end_idx {
        return (&[], &[]);
    }

    let timestamps = &stamps[start_idx..end_idx];
    let values = &values[start_idx..end_idx];
    (timestamps, values)
}
