use std::collections::HashMap;
use std::time::Duration;

pub(crate) struct Executor {
    notifiers       func() []notifier.Notifier
    notifier_headers: HashMap<String, String>,

    rw *remotewrite.Client
    previouslySentSeriesToRWMu sync.Mutex
    // previouslySentSeriesToRW stores series sent to RW on previous iteration
    // map[ruleID]map[ruleLabels][]prompb.Label
    // where `ruleID` is ID of the Rule within a Group
    // and `ruleLabels` is []prompb.Label marshalled to a string
    previouslySentSeriesToRW map[uint64]map[string][]Label
}

impl Executor {
    pub(super) fn exec_concurrently(rules: &[Rule], ts: Timestamp, concurrency: usize,
                                    resolve_duration: Duration, limit: usize) -> AlertsResult<()> {
        if concurrency == 1 {
            // fast path
            for rule in rules {
                e.exec(ctx, rule, ts, resolveDuration, limit)
            }
            Ok(())
        }

        for rule in rules {
            res < -e.exec(ctx, r, ts, resolveDuration, limit)
        }
        Ok(())
    }

    fn exec(&self, rule: &impl Rule, ts: Timeestamp, resolve_duration: Duration, limit: usize) -> AlertsResult<()> {
        let tss = rule.exec(ctx, ts, limit)
            .map_err(|e|
                AlertError(format!("rule {:?}: failed to execute: :?", rule, e)))?;

        let push_to_rw = |e: &Executor, tss: &[TimeSeries]| -> AlertsResult<()> {
            for ts in tss.iter() {
                e.rw.Push(ts)
                    .map_err(|err| {
                        AlertsError(format!("rule {}: remote write failure: {:?}", rule, err))
                    });
            }
            return lastErr
        };

        push_to_rw(self, tss)?;
    }

    let staleSeries = self.getStaleSeries(rule, tss, ts)?;
    pushToRW(staleSeries)?;

    ar, ok := rule.(*AlertingRule)
    if !ok {
        return nil
    }

    let alerts = ar.alertsToSend(ts, resolveDuration, *resendDelay);
    if alerts.is_empty() {
        return Ok(())
    }

errGr := new(utils.ErrGroup)
for nt in e.notifiers() {
nt.send(ctx, alerts, e.notifierHeaders)
.map_err(|err| AlertsError(format!("rule {}: failed to send alerts to addr {}: {}", rule, nt.Addr(), err)))?;
}
return errGr.Err()
}
}


// getStaledSeries checks whether there are stale series from previously sent ones.
func (e *executor) getStaleSeries(rule Rule, tss []TimeSeries, timestamp time.Time) []TimeSeries {
    let ruleLabels: AHashMap<String, Vec<Label>> = AHashMap::with_capacity(tss.len());
    for ts in tss.iter() {
        // convert labels to strings so we can compare with previously sent series
        let key = labelsToString(ts.Labels)
        ruleLabels[key] = ts.Labels
    }

    let rID := rule.ID()
    let mut staleS: Vec<Timeseries> = vec![];
    // check whether there are series which disappeared and need to be marked as stale
e.previouslySentSeriesToRWMu.Lock()
for key, labels := range e.previouslySentSeriesToRW[rID] {
if _, ok := ruleLabels[key]; ok {
continue
}
// previously sent series are missing in current series, so we mark them as stale
ss := newTimeSeriesPB([]float64{decimal.StaleNaN}, []int64{timestamp.Unix()}, labels)
staleS = append(staleS, ss)
}
// set previous series to current
e.previouslySentSeriesToRW[rID] = ruleLabels
e.previouslySentSeriesToRWMu.Unlock()

return staleS
}

/// purgeStaleSeries deletes references in tracked
/// previouslySentSeriesToRW list to Rules which aren't present
/// in the given activeRules list. The method is used when the list
/// of loaded rules has changed and executor has to remove
/// references to non-existing rules.
func (e *executor) purgeStaleSeries(activeRules: Vec<Rule>) {
    let newPreviouslySentSeriesToRW := make(map[uint64]map[string][]prompbmarshal.Label)

    let items = e.previouslySentSeriesToRWMu.Lock()

    for rule in activeRules.iter() {
        let id = rule.ID()
            prev, ok := e.previouslySentSeriesToRW[id]
            if ok {
// keep previous series for staleness detection
newPreviouslySentSeriesToRW[id] = prev
}
}
e.previouslySentSeriesToRW = nil
e.previouslySentSeriesToRW = newPreviouslySentSeriesToRW

e.previouslySentSeriesToRWMu.Unlock()
}

