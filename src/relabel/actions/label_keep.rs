use regex::Regex;
use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::actions::utils::filter_labels;
use crate::relabel::IfExpression;
use crate::storage::Label;

#[derive(Debug, Clone)]
pub struct LabelDropAction {
    pub if_expr: Option<IfExpression>,
    pub regex: Regex, // todo: PromRegex
}

impl LabelDropAction {
    pub fn new(regex: Regex, if_expression: Option<IfExpression>) -> Self {
        Self {
            if_expr: if_expression,
            regex,
        }
    }
}

impl Action for LabelDropAction {
    fn apply(&self, labels: &mut Vec<Label>, _labels_offset: usize) {
        // Keep all the labels matching `regex`
        labels.retain(|label| self.regex.is_match(&label.name))
    }

    fn filter(&self, labels: &[Label]) -> bool {
        filter_labels(&self.if_expr, labels)
    }
}