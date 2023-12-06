use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;
use crate::relabel::actions::Action;
use crate::relabel::IfExpression;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;

/// Store the hashmod of `source_labels` joined with `separator` at `target_label`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HashModAction {
    pub if_expr: Option<IfExpression>,
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    pub modulus: u64,
}

impl HashModAction {
    pub fn new(source_labels: Vec<String>, target_label: String, separator: String, modulus: u64, if_expression: Option<IfExpression>) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err(format!("missing `source_labels` for `action=hashmod`"));
        }
        if target_label.is_empty() {
            return Err(format!("missing `target_label` for `action=hashmod`"));
        }
        if modulus == 0 {
            return Err(format!("`modulus` must be greater than 0 for `action=hashmod`"));
        }
        Ok(Self { source_labels, target_label, separator, modulus, if_expr: if_expression })
    }
}

impl Action for HashModAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let hash_mod = xxh3_64(&buf.as_bytes()) % self.modulus;
        let value_str = hash_mod.to_string();
        set_label_value(labels, labels_offset, &self.target_label, value_str)
    }

    fn filter(&self, labels: &[Label]) -> bool {
        self.if_expr.is_some_and(|if_expr| if_expr.is_match(labels))
    }
}

