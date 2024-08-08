use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::relabel::actions::Action;
use crate::storage::Label;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelDropAction {
    pub regex: Regex, // todo: promRegex
}

impl LabelDropAction {
    pub fn new(regex: Regex) -> Self {
        Self {
            regex,
        }
    }
}

impl Action  for LabelDropAction {
    fn apply(&self, labels: &mut Vec<Label>, _labels_offset: usize) {
        // Drop all the labels matching `regex`
        labels.retain(|label| !self.regex.is_match(&label.name))
    }
}