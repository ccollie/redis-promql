mod bytes_util;
mod time;
pub mod types;
pub mod regex_util;
pub(crate) mod humanize;
mod utils;

pub use humanize::*;
use regex::Regex;
use std::sync::LazyLock;
pub use utils::*;


// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";
pub static METRIC_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$").unwrap());

pub fn get_metric_name_regex() -> &'static str {
    METRIC_NAME_REGEX.as_str()
}