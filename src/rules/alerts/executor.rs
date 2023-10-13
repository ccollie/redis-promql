use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use ahash::{AHashMap, AHashSet};
use crate::common::constants::STALE_NAN;
use crate::common::types::{Label, Timestamp};
use crate::config::get_global_settings;
use crate::rules::alerts::{AlertingRule, AlertsError, AlertsResult, Notifier, WriteQueue};
use crate::rules::{new_time_series, RawTimeSeries, Rule, RuleType};
use crate::rules::relabel::labels_to_string;

pub type PreviouslySentSeries = HashMap<u64, HashMap<String, Vec<Label>>>;

pub struct Executor {
    pub(crate) notifiers: fn() -> Vec<Box<dyn Notifier>>,
    pub(crate) notifier_headers: AHashMap<String, String>,

    pub(crate) rw: WriteQueue,

    /// previously_sent_series_to_rw stores series sent to RW on previous iteration
    /// HashMap<RuleID, HashMap<ruleLabels, Vec<Label>>
    /// where `ruleID` is id of the Rule within a Group and `ruleLabels` is Vec<Label> marshalled
    /// to a string
    pub(crate) previously_sent_series_to_rw: Mutex<PreviouslySentSeries>,
}

impl Executor {
    /// get_stale_series checks whether there are stale series from previously sent ones.
    fn get_stale_series(&self, rule: impl Rule, tss: &[RawTimeSeries], timestamp: Timestamp) -> Vec<RawTimeSeries> {
        let mut rule_labels: HashMap<String, &Vec<Label>> = HashMap::with_capacity(tss.len());
        for ts in tss.inter() {
            // convert labels to strings so we can compare with previously sent series
            let key = labels_to_string(&ts.labels);
            rule_labels.insert(key, &ts.labels);
        }

        let rid = rule.id();
        let mut stale_s: Vec<RawTimeSeries> = Vec::with_capacity(tss.len());
        // check whether there are series which disappeared and need to be marked as stale
        let mut map = self.previously_sent_series_to_rw.lock().unwrap();

        if let Some(entry) = map.get_mut(&rid) {
            for (key, labels) in entry.iter_mut() {
                if rule_labels.contains_key(&key) {
                    continue;
                }
                let stamps = [timestamp];
                let values = [STALE_NAN.clone()];
                // previously sent series are missing in current series, so we mark them as stale
                let ss = new_time_series(&values, &stamps, &labels);
                stale_s.push(ss)
            }
        }

        // set previous series to current
        map.insert(rid, rule_labels);

        return stale_s;
    }

    /// purge_stale_series deletes references in tracked previously_sent_series_to_rw list to rules
    /// which aren't present in the given active_rules list. The method is used when the list
    /// of loaded rules has changed and executor has to remove references to non-existing rules.
    pub(super) fn purge_stale_series(&mut self, active_rules: &[impl Rule]) {
        let id_hash_set: AHashSet<u64> = active_rules.iter().map(|r| r.id()).collect();

        let mut map = self.previously_sent_series_to_rw.lock().unwrap();

        map.retain(|id, _| id_hash_set.contains(id));
    }

    fn exec_concurrently(&mut self, rules: &[impl Rule], ts: Timestamp, resolve_duration: Duration, limit: usize) -> AlertsResult<()> {
        for rule in rules {
            res < -self.exec(rule, ts, resolve_duration, limit)
        }
        return res;
    }

    pub fn exec(&mut self, rule: &mut impl Rule, ts: Timestamp, resolve_duration: Duration, limit: usize) -> AlertsResult<()> {
        let mut tss = rule.exec(ts, limit);
        let settings = get_global_settings();

        if let Err(err) = tss {
            // todo: specific error type
            let msg = format!("rule {:?}: failed to execute: {:?}", rule, err);
            return Err(AlertsError::QueryExecutionError(msg));
        }
        let tss = tss.unwrap();

        self.push_to_rw(&rule, &tss)?;

        let stale_series = self.get_stale_series(rule, &tss, ts);
        self.push_to_rw(&rule, &stale_series)?;

        if rule.rule_type() == RuleType::Alerting {
            let alerting_rule = rule.as_any().downcast_ref::<AlertingRule>().unwrap();
            return self.send_notifications(alerting_rule, ts, resolve_duration, settings.resend_delay);
        }
        return err_gr.Err();
    }

    fn push_to_rw(&mut self, rule: &impl Rule, tss: &[RawTimeSeries]) -> AlertsResult<()> {
        let mut last_err = "".to_string();
        for ts in tss {
            if let Err(err) = self.rw.push(ts) {
                last_err = format!("rule {:?}: remote write failure: {:?}", rule, err);
            }
        }
        if !last_err.is_empty() {
            // todo: specific error type
            return Err(AlertsError::Generic(last_err));
        }
        Ok(())
    }

    fn send_notifications(&self, rule: &AlertingRule, ts: Timestamp, resolve_duration: Duration, resend_delay: Duration) -> AlertsResult<()> {
        let mut alerts = rule.alerts_to_send(ts, resolve_duration, resend_delay);
        if alerts.is_empty() {
            return Ok(());
        }
        let mut err_gr: Vec<String> = Vec::with_capacity(4);
        for nt in self.notifiers() {
            if let Err(err) = nt.send(&alerts, &self.notifier_headers) {
                let msg = format!("failed to send alerts to addr {}: {:?}", nt.addr(), err);
                err_gr.push(msg);
            }
        }
        return err_gr.Err();
    }
}
