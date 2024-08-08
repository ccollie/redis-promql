use crate::relabel::actions::Action;
use crate::relabel::submatch_replacer::SubmatchReplacer;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Replace all the occurrences of `regex` at `source_labels` joined with `separator` with the `replacement`
/// and store the result at `target_label`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaceAllAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    submatch_replacer: SubmatchReplacer,
}

impl ReplaceAllAction {
    pub fn new(source_labels: Vec<String>,
               target_label: String,
               separator: String,
               regex: Option<Regex>,
               replacement: String) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=replace_all`".to_string());
        }

        let regex = regex.ok_or_else(|| "missing `regex` for `action=replace_all`".to_string())?;
        let submatch_replacer = SubmatchReplacer::new(regex.clone(), replacement.clone())?;
        Ok(Self {
            source_labels,
            target_label,
            separator,
            submatch_replacer,
        })
    }

    fn replace_string_submatches_fast(&self, s: &str) -> String {
        self.submatch_replacer.replace_fast(s)
    }
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
