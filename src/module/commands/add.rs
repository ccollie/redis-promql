use crate::arg_parse::parse_timestamp;
use crate::module::with_timeseries_mut;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

///
/// VKM.ADD key timestamp value
///
pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let timestamp = parse_timestamp(args.next_str()?)?;
    let value = args.next_f64()?;

    with_timeseries_mut(ctx, &key, |series| {
        series.add(timestamp, value, None)?;
        Ok(ValkeyValue::Integer(timestamp))
    })
}