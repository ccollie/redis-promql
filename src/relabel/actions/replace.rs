use crate::common::regex_util::PromRegex;
use crate::relabel::{DEFAULT_REGEX_FOR_RELABEL_CONFIG, fill_label_references, IfExpression};
use crate::relabel::actions::Action;
use crate::relabel::regex_parse::parse_regex;
use crate::relabel::string_replacer::StringReplacer;
use crate::relabel::utils::{concat_label_values, set_label_value};
use crate::storage::Label;

/// replaces the first occurrence of `regex` at `source_labels` joined with `separator` with the `replacement`
/// and store the result at `target_label`
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
    pub separator: String,
    pub regex: PromRegex,
    pub replacement: String,
    pub has_label_reference_in_replacement: bool,
    pub has_capture_group_in_target_label: bool,
    pub if_expr: Option<IfExpression>,
    is_default_regex: bool,
    replacer: StringReplacer
}

impl ReplaceAction {
    pub fn new(source_labels: Vec<String>,
               target_label: String,
               separator: String,
               regex: Option<regex::Regex>,
               replacement: String,
               if_expression: Option<IfExpression>
    ) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=replace`".to_string());
        }

        let (regex_anchored, _, regex_prom) = parse_regex(regex, true)?;

        let has_label_reference_in_replacement = replacement.contains("{{");
        let has_capture_group_in_target_label = target_label.contains("(");
        let replacer = StringReplacer::new(regex_prom.clone(), replacement.clone())?;
        let is_default_regex = regex_anchored == *DEFAULT_REGEX_FOR_RELABEL_CONFIG;

        Ok(Self {
            source_labels,
            target_label,
            separator,
            regex: regex_prom,
            replacement,
            has_label_reference_in_replacement,
            has_capture_group_in_target_label,
            if_expr: if_expression,
            replacer,
            is_default_regex
        })
    }

    fn replace_string(&self, s: &str) -> String {
        self.replacer.replace_string(s)
    }

    fn expand_capture_groups(&self, template: &str, src: &str) -> String {
        self.replacer.expand_capture_groups(template, src)
    }
}

impl Action for ReplaceAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Store `replacement` at `target_label` if the `regex` matches `source_labels` joined with `separator`
        let mut replacement = if self.has_label_reference_in_replacement {
            let mut buf: String = String::with_capacity(128);
            // Fill {{labelName}} references in the replacement
            fill_label_references(&mut buf, &self.replacement, &labels[labels_offset..]);
            buf
        } else {
            self.replacement.clone()
        };

        let buf = concat_label_values(labels, &self.source_labels, &self.separator);
        if self.is_default_regex && !self.has_capture_group_in_target_label {
            if replacement == "$1" {
                // Fast path for the rule that copies source label values to destination:
                // - source_labels: [...]
                //   target_label: foobar
                let value_str = buf;
                return set_label_value(labels, labels_offset, &self.target_label, value_str);
            }
            if !self.has_label_reference_in_replacement {
                // Fast path for the rule that sets label value:
                // - target_label: foobar
                //   replacement: something-here
                set_label_value(labels, labels_offset, &self.target_label, replacement);
                return;
            }
        }
        let source_str = &buf;
        if !self.replacer.is_match(source_str) {
            // Fast path - regexp mismatch.
            return;
        }
        let value_str = if replacement == self.replacement {
            // Fast path - the replacement wasn't modified, so it is safe calling stringReplacer.Transform.
            self.replace_string(source_str)
        } else {
            // Slow path - the replacement has been modified, so the valueStr must be calculated
            // from scratch based on the new replacement value.
            self.expand_capture_groups(&replacement, source_str)
        };
        let mut name_str = &self.target_label;
        if self.has_capture_group_in_target_label {
            // Slow path - target_label contains regex capture groups, so the target_label
            // must be calculated from the regex match.
            name_str = &self.expand_capture_groups(name_str, source_str);
        }

        set_label_value(labels, labels_offset, name_str, value_str)
    }

    fn filter(&self, labels: &[Label]) -> bool {
        self.if_expr.is_some_and(|if_expr| if_expr.is_match(labels))
    }
}
