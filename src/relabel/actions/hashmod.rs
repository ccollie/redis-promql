use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;
use crate::relabel::actions::Action;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;

/// Store the hashmod of `source_labels` joined with `separator` at `target_label`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HashModAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    pub modulus: u64,
}

impl Action {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let hash_mod = xxh3_64(&buf.as_bytes()) % self.modulus;
        let value_str = hash_mod.to_string();
        set_label_value(labels, labels_offset, &self.target_label, value_str)
    }
}

