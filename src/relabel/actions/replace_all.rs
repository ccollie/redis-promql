use regex::Regex;
use crate::relabel::actions::Action;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;

/// Replace all the occurrences of `regex` at `source_labels` joined with `separator` with the `replacement`
/// and store the result at `target_label`
/// todo: use buffer pool
pub struct ReplaceAllAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    pub regex: Regex,
    pub replacement: String,

}

impl Action for ReplaceAllAction {
    fn apply(&self, labels: &mut Vec<Label>, label_offset: usize) {
        let buf = concat_label_values(labels, &self.source_labels, &self.separator);
        let value_str = self.replace_string_submatches_fast(&buf);
        if value_str != buf {
            set_label_value(labels, label_offset, &self.target_label, value_str)
        }
    }
}
