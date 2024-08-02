use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::actions::utils::filter_labels;
use crate::relabel::IfExpression;
use crate::relabel::utils::are_equal_label_values;
use crate::storage::Label;

/// Keep the entry if all the label values in source_labels are equal.
/// For example:
///
///   - source_labels: [foo, bar]
///     action: keep_if_equal
///
/// Would leave the entry if `foo` value equals `bar` value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepIfEqualAction {
    pub source_labels: Vec<String>,
    pub if_expr: Option<IfExpression>,
}

impl KeepIfEqualAction {
    pub fn new(source_labels: Vec<String>, if_expression: Option<IfExpression>) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=keep_if_equal`".to_string());
        }
        Ok(Self { source_labels, if_expr: if_expression })
    }
}

impl Action for KeepIfEqualAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        if !are_equal_label_values(labels, &self.source_labels) {
            labels.truncate(labels_offset);
        }
    }

    fn filter(&self, labels: &[Label]) -> bool {
        filter_labels(&self.if_expr, labels)
    }
}