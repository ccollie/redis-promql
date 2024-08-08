use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::common::regex_util::PromRegex;
use crate::relabel::actions::Action;
use crate::relabel::is_default_regex_for_config;
use crate::relabel::regex_parse::parse_regex;
use crate::relabel::utils::concat_label_values;
use crate::storage::Label;

/// Drop the entry if `source_labels` joined with `separator` matches `regex`
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DropAction {
    pub source_labels: Vec<String>,
    separator: String,
    regex: PromRegex,
    is_default_regex: bool
}

impl DropAction {
    pub fn new(source_labels: Vec<String>,
               separator: String,
               regex: Regex) -> Result<Self, String> {

        let (regex_anchored, _, prom_regex) = parse_regex(Some(regex), true)?;

        let is_default_regex = is_default_regex_for_config(&regex_anchored);

        Ok(Self {
            source_labels,
            separator,
            regex: prom_regex,
            is_default_regex
        })
    }
}

impl Action for DropAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        if self.is_default_regex {
            // Fast path for the case with `if` and without explicitly set `regex`:
            //
            // - action: drop
            //   if: 'some{label=~"filters"}'
            //
            return;
        }

        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let drop = self.regex.is_match(&buf);
        if drop {
            labels.truncate(labels_offset);
        }
    }
}
