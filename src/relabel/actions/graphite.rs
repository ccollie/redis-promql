use serde::{Deserialize, Serialize};

use crate::common::METRIC_NAME_LABEL;
use crate::relabel::{GraphiteLabelRule, GraphiteMatchTemplate};
use crate::relabel::actions::Action;
use crate::relabel::utils::{get_label_value, set_label_value};
use crate::storage::Label;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphiteAction {
    pub graphite_match_template: Option<GraphiteMatchTemplate>,
    pub graphite_label_rules: Vec<GraphiteLabelRule>,
}

impl GraphiteAction {
    pub fn new(
        graphite_match_template: Option<GraphiteMatchTemplate>,
        graphite_label_rules: Vec<GraphiteLabelRule>,
    ) -> Self {
        Self {
            graphite_match_template,
            graphite_label_rules
        }
    }
}

impl Action for GraphiteAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        let metric_name = get_label_value(&labels, METRIC_NAME_LABEL);
        if let Some(gmt) = &self.graphite_match_template {
            // todo: use pool
            let mut matches: Vec<String> = Vec::with_capacity(4);
            if !gmt.is_match(&mut matches, metric_name) {
                // Fast path - name mismatch
                return;
            }
            // Slow path - extract labels from graphite metric name
            for gl in self.graphite_label_rules.iter() {
                let value_str = gl.expand(&matches);
                set_label_value(labels, labels_offset, &gl.target_label, value_str)
            }
        } else {
            return
        }
    }
}