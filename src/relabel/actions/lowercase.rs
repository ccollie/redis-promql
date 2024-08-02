use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::actions::utils::filter_labels;
use crate::relabel::IfExpression;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;

/// lowercase the entry if `source_labels` joined with `separator` matches `target_label`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LowercaseAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    pub if_expr: Option<IfExpression>,
}


impl LowercaseAction {
    pub fn new(source_labels: Vec<String>, target_label: String, separator: String, if_expression: Option<IfExpression>) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=lowercase`".to_string());
        }
        if target_label.is_empty() {
            return Err("missing `target_label` for `action=lowercase`".to_string());
        }
        Ok(Self { source_labels, target_label, separator, if_expr: if_expression })
    }
}

impl Action for LowercaseAction{
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let value_str = buf.to_lowercase();
        set_label_value(labels, labels_offset, &self.target_label, value_str)
    }

    fn filter(&self, labels: &[Label]) -> bool {
        filter_labels(&self.if_expr, labels)
    }
}
