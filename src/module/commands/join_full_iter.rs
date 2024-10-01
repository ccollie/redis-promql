use crate::common::types::Sample;
use crate::module::commands::join::convert_join_item;
use crate::module::commands::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};

pub struct JoinFullIter<'a> {
    inner: Box<dyn Iterator<Item=EitherOrBoth<&'a Sample, &'a Sample>> + 'a>
}

impl<'a> JoinFullIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left.into_iter()
            .merge_join_full_outer_by(right, |x, y| x.timestamp.cmp(&y.timestamp));

        Self {
            inner: Box::new(iter)
        }
    }
}

impl<'a> Iterator for JoinFullIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(item) => Some(convert_join_item(item))
        }
    }
}