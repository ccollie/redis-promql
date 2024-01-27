use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::IfExpression;
use crate::relabel::utils::{concat_label_values, get_label_value};
use crate::storage::Label;

/// keep the entry if `source_labels` joined with `separator` matches `target_label`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepEqualAction {
    pub if_expr: Option<IfExpression>,
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
}

impl KeepEqualAction {
    pub fn new(source_labels: Vec<String>, target_label: String, separator: String, if_expression: Option<IfExpression>) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=keep_equal`".to_string());
        }
        Ok(Self {
            if_expr: if_expression,
            source_labels,
            target_label,
            separator,
        })
    }
}

impl Action for KeepEqualAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Keep the entry if `source_labels` joined with `separator` matches `target_label`
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let target_value = get_label_value(&labels[labels_offset..], &self.target_label);
        let keep = buf == target_value;
        if !keep {
            labels.truncate(labels_offset);
        }
    }

    fn filter(&self, labels: &[Label]) -> bool {
        self.if_expr.is_some_and(|if_expr| if_expr.is_match(labels))
    }
}