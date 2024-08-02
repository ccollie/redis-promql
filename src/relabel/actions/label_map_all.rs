use crate::relabel::actions::Action;
use crate::relabel::actions::utils::filter_labels;
use crate::relabel::IfExpression;
use crate::relabel::submatch_replacer::SubmatchReplacer;
use crate::storage::Label;

/// replace all the occurrences of `regex` at label names with `replacement`
pub struct LabelMapAllAction {
    pub if_expr: Option<IfExpression>,
    submatch_replacer: SubmatchReplacer
}

impl LabelMapAllAction {
    pub fn new(replacement: String, regex: regex::Regex, if_expression: Option<IfExpression>) -> Result<Self, String> {
        Ok(
        Self {
            if_expr: if_expression,
            submatch_replacer: SubmatchReplacer::new(regex, replacement)?
        })
    }
}

impl Action for LabelMapAllAction {
    fn apply(&self, labels: &mut Vec<Label>, _labels_offset: usize) {
        // Replace all the occurrences of `regex` at label names with `replacement`
        for label in labels.iter_mut() {
            label.name = self.submatch_replacer.replace_fast(&label.name);
        }
    }

    fn filter(&self, labels: &[Label]) -> bool {
        filter_labels(&self.if_expr, labels)
    }
}