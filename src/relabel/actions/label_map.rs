use serde::{Deserialize, Serialize};

use crate::relabel::actions::Action;
use crate::relabel::string_replacer::StringReplacer;
use crate::relabel::utils::set_label_value;
use crate::storage::Label;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelMapAction {
    string_replacer: StringReplacer,
}

impl LabelMapAction {
    pub fn new(replacement: String, regex: regex::Regex) -> Result<Self, String> {
        Ok(Self {
            string_replacer: StringReplacer::new(regex, replacement)?,
        })
    }
}

impl Action for LabelMapAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Copy `source_labels` to `target_label`
        // Replace label names with the `replacement` if they match `regex`
        for label in labels.iter() {
            let label_name = self.string_replacer.replace_fast(&label.name);
            if label_name != label.name {
                let value_str = label.value.clone();
                set_label_value(labels, labels_offset, &label_name, value_str)
            }
        }
    }
}
