use regex::Regex;
use serde::{Deserialize, Serialize};
use crate::relabel::actions::Action;
use crate::relabel::IfExpression;
use crate::storage::Label;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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
        labels.retain(|label| self.regex.match_string(&label.name))
    }

    fn filter(&self, labels: &[Label]) -> bool {
        self.if_expr.is_some_and(|if_expr| if_expr.is_match(labels))
    }
}