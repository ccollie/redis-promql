use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::DEFAULT_REGEX_FOR_RELABEL_CONFIG;
use crate::relabel::utils::concat_label_values;
use crate::storage::Label;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepAction {
    pub source_labels: Vec<String>,
    pub separator: String,
    pub regex: regex::Regex,
    pub regex_anchored: regex::Regex,
}

impl Action {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Keep the entry if `source_labels` joined with `separator` matches `regex`
        if self.regex_anchored == *DEFAULT_REGEX_FOR_RELABEL_CONFIG {
            // Fast path for the case with `if` and without explicitly set `regex`:
            //
            // - action: keep
            //   if: 'some{label=~"filters"}'
            //
            return;
        }
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let keep = self.regex.match_string(&buf);
        if !keep {
            labels.truncate(labels_offset);
        }
    }

}