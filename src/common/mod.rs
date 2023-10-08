mod bytes_util;
mod time;
pub mod types;
pub mod regex_util;
mod humanize;
mod utils;
mod parse;
mod labels;

use std::sync::OnceLock;
use regex::Regex;
pub use humanize::*;
pub use utils::*;
pub use parse::*;



// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";
pub static METRIC_NAME_RE: OnceLock<Regex> = OnceLock::new();

pub fn get_metric_name_regex() -> &'static str {
    METRIC_NAME_RE.get_or_init(|| {
        Regex::new(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$").unwrap()
    }).as_str()
}