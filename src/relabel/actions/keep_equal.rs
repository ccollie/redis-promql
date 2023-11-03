use serde::{Deserialize, Serialize};
use crate::relabel::utils::{concat_label_values, get_label_value};
use crate::storage::Label;

/// keep the entry if `source_labels` joined with `separator` matches `target_label`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepEqualAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
}

fn keep_equal(&self, labels: &mut Vec<Label>, labels_offset: usize) {
    let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
    let target_value = get_label_value(&labels[labels_offset..], &self.target_label);
    let keep = buf == target_value;
    if keep {
        return;
    }
    labels.truncate(labels_offset);
}