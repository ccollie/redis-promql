use crate::common::types::Sample;
use crate::module::commands::JoinValue;
use joinkit::Joinkit;

// todo: accept iterators instead of slices
pub struct JoinLeftExclusiveIter<'a> {
    inner: Box<dyn Iterator<Item=&'a Sample> + 'a>
}

impl<'a> JoinLeftExclusiveIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left.into_iter()
            .merge_join_left_excl_by(right, |x, y| x.timestamp.cmp(&y.timestamp));

        Self {
            inner: Box::new(iter),
        }
    }
}

impl<'a> Iterator for JoinLeftExclusiveIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(item) => Some(JoinValue::left(item.timestamp, item.value))
        }
    }
}