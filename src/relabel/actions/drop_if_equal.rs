use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::utils::are_equal_label_values;
use crate::storage::Label;

/// Drop the entry if all the label values in source_labels are equal.
/// For example:
///
///   - source_labels: [foo, bar]
///     action: drop_if_equal
///
/// Would drop the entry if `foo` value equals `bar` value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropIfEqualAction {
    pub source_labels: Vec<String>,
}

impl Action for DropIfEqualAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        if are_equal_label_values(labels, &self.source_labels) {
            labels.truncate(labels_offset);
        }
    }
}
