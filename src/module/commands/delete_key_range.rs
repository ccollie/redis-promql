use crate::module::{parse_timestamp_arg, with_timeseries_mut};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// VKM.DELETE-KEY-RANGE key  <rfc3339 | unix_timestamp | + | - | * >  <rfc3339 | unix_timestamp | + | - | * >
///
/// Deletes the data points in the given range for the given key.
pub fn delete_key_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    with_timeseries_mut(ctx, &key, |series| {
        let from = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
        let to = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

        args.done()?;

        let start = from.as_timestamp();
        let end = to.as_timestamp();

        if start > end {
            return Err(ValkeyError::Str("ERR invalid range"));
        }

        let sample_count = series.remove_range(start, end)?;

        Ok(ValkeyValue::from(sample_count))
    })
}
