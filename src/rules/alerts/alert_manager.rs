use std::sync::Arc;
use ahash::{AHashMap, HashMap};
use redis_module::Context;
use crate::rules::alerts::{AlertsResult, Group, GroupConfig, Notifier, QuerierBuilder, WriteQueue};

/// manager controls group states
pub struct Manager {
    querier_builder: Box<dyn QuerierBuilder>,
    notifiers: fn() -> Vec<Box<dyn Notifier>>,

    rw: Arc<WriteQueue>,
    // remote read builder.
    rr: Box<dyn QuerierBuilder>,

    labels: AHashMap<String, String>,
    groups: HashMap<u64, Group>
}

impl Manager {
    fn start(&mut self, ctx: &Context, groups_cfg: &[GroupConfig]) -> AlertsResult<()> {
        return self.update(ctx, groups_cfg, true)
    }

    fn start_group(&mut self, ctx: &Context, g: &Group, restore: bool) -> AlertsResult<()> {
        let id = g.ID();
        if restore {
            g.start(ctx, self.notifiers, Arc::clone(&self.rw), self.rr)
        } else {
            g.start(ctx, self.notifiers, Arc::clone(&self.rw), nil)
        }
        self.groups.insert(id, g);
        Ok(())
    }

    fn update(&mut self, ctx: &Context, groups_cfg: &[GroupConfig], restore: bool) -> AlertsResult<()> {
        let mut rr_present = false;
        let mut ar_present = false;

        let mut groups_registry: HashMap<u64, Group> = HashMap::default();
        for cfg in groups_cfg {
            for r in cfg.rules {
                if rr_present && ar_present {
                    continue
                }
                if !r.record.is_empty() {
                    rr_present = true
                }
                if !r.alert.is_empty() {
                    ar_present = true
                }
            }
            let ng = Group::new(cfg, &self.querier_builder, *evaluationInterval, &self.labels);
            groups_registry.insert(ng.ID(), ng)
        }

        if rr_present && self.rw == nil {
            return fmt.Errorf("config contains recording rules but `-remoteWrite.url` isn't set")
        }
        if ar_present && self.notifiers == nil {
            return fmt.Errorf("config contains alerting rules but neither `-notifier.url` nor `-notifier.config` nor `-notifier.blackhole` aren't set")
        }
        struct UpdateItem<'a> {
            old: &'a Group,
            new: &'a Group
        }

        let mut to_update = vec![];

        m.groupsMu.Lock();
        let to_delete = vec![];
        for (_, og) in self.groups {
            let ng = groups_registry.get(og.ID());
            if ng.is_none() {
                // old group is not present in new list,
                // so must be stopped and deleted
                self.remove(og.ID());
                continue
            }
            let ng = ng.unwrap();
            groups_registry.remove(ng.ID());
            if og.checksum != ng.checksum {
                to_update.push(UpdateItem{old: og, new: ng})
            }
        }
        for (_, ng) in groups_registry {
            self.start_group(ctx, &ng, restore)?;
        }
        if !to_update.is_empty() {
            for item in to_update {
                old.update_with(new);
                item.old.InterruptEval()
            }
        }
        Ok(())
    }
}

