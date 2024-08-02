use crate::relabel::IfExpression;
use crate::storage::Label;

pub(super) fn filter_labels(if_expr: &Option<IfExpression>, labels: &[Label]) -> bool {
    if let Some(if_expr) = &if_expr {
        if !if_expr.is_match(labels) {
            return false;
        }
        return true;
    }
    true
}