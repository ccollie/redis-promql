use crate::common::types::Sample;
use crate::module::commands::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};
use super::join::convert_join_item;

pub struct JoinLeftIter<'a> {
    inner: Box<dyn Iterator<Item = EitherOrBoth<&'a Sample, &'a Sample>> + 'a>
}

impl<'a> JoinLeftIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left.into_iter()
            .merge_join_left_outer_by(right, |x, y| x.timestamp.cmp(&y.timestamp));
        Self {
            inner: Box::new(iter),
        }
    }
}

impl<'a> Iterator for JoinLeftIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(item) => Some(convert_join_item(item))
        }
    }
}
