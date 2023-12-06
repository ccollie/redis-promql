use regex::Regex;
use serde::{Deserialize, Serialize};
use crate::common::regex_util::PromRegex;
use crate::relabel::actions::Action;
use crate::relabel::{DEFAULT_REGEX_FOR_RELABEL_CONFIG, IfExpression};
use crate::relabel::regex_parse::parse_regex;
use crate::relabel::utils::concat_label_values;
use crate::storage::Label;

/// Drop the entry if `source_labels` joined with `separator` matches `regex`
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropAction {
    pub if_expr: Option<IfExpression>,
    pub source_labels: Vec<String>,
    pub separator: String,
    pub regex: PromRegex,
    is_default_regex: bool
}

impl DropAction {
    pub fn new(source_labels: Vec<String>,
               separator: String,
               regex: Regex,
               if_expr: Option<IfExpression>) -> Result<Self, String> {

        if source_labels.is_empty() && if_expr.is_none() {
            return Err(format!("missing `source_labels` for `action=drop`"));
        }

        let (regex_anchored, _, prom_regex) = parse_regex(Some(regex), true)?;

        let is_default_regex = regex_anchored == *DEFAULT_REGEX_FOR_RELABEL_CONFIG;

        Ok(Self {
            source_labels,
            separator,
            regex: prom_regex,
            if_expr,
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
        let drop = self.regex.match_string(&buf);
        if drop {
            labels.truncate(labels_offset);
        }
    }

    fn filter(&self, labels: &[Label]) -> bool {
        self.if_expr.is_some_and(|if_expr| if_expr.is_match(labels))
    }
}
