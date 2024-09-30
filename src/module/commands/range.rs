use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::get_range;
use crate::module::get_timeseries;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub(crate) fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;
    let series = get_timeseries(ctx, &key)?;

    let samples = get_range(series, &options, false);
    let result = samples.iter().map(|s| {
        let row = vec![ValkeyValue::from(s.timestamp), ValkeyValue::from(s.value)];
        ValkeyValue::from(row)
    }).collect::<Vec<ValkeyValue>>();

    Ok(ValkeyValue::from(result))
}