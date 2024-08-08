use crate::relabel::actions::Action;
use crate::relabel::utils::{concat_label_values, get_label_value};
use crate::storage::Label;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropEqualAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
}

impl DropEqualAction {
    pub fn new(source_labels: Vec<String>, target_label: String, separator: String) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=drop_equal`".to_string());
        }
        Ok(Self {
            source_labels,
            target_label,
            separator,
        })
    }
}

impl Action for DropEqualAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Drop the entry if `source_labels` joined with `separator` matches `target_label`
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let target_value = get_label_value(&labels[labels_offset..], &self.target_label);
        let drop = buf == target_value;
        if !drop {
            return;
        }
        labels.truncate(labels_offset);
    }
}