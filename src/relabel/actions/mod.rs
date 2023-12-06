use crate::storage::Label;

pub trait Action {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize);
    fn filter(&self, _labels: &[Label]) -> bool {
        true
    }
}

mod drop_equal;
mod drop_if_contains;
mod drop_if_equal;
mod keep_if_equal;
mod keep;
mod keep_equal;
mod keep_if_contains;
mod hashmod;
mod replace_all;
mod graphite;
mod label_drop;
mod lowercase;
mod uppercase;
mod drop;
mod label_map;
mod label_map_all;
mod replace;
mod label_keep;
