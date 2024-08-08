use regex::Regex;

use crate::relabel::actions::Action;
use crate::storage::Label;

#[derive(Debug, Clone)]
pub struct LabelKeepAction {
    pub regex: Regex, // todo: PromRegex
}

impl LabelKeepAction {
    pub fn new(regex: Regex) -> Self {
        Self {
            regex,
        }
    }
}

impl Action for LabelKeepAction {
    fn apply(&self, labels: &mut Vec<Label>, _labels_offset: usize) {
        // Keep all the labels matching `regex`
        labels.retain(|label| self.regex.is_match(&label.name))
    }
}