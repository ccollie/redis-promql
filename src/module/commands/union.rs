use valkey_module::{Context, ValkeyResult, ValkeyString, VALKEY_OK};

/// VM.UNION fromTimestamp toTimestamp FILTER filter...
/// [
pub(crate) fn union(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    VALKEY_OK
}