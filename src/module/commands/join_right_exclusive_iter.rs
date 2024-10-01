use crate::common::types::Sample;
use crate::module::commands::JoinValue;
use joinkit::Joinkit;

// todo: accept iterators instead of slices
pub struct JoinRightExclusiveIter<'a> {
    iter: Box<dyn Iterator<Item = &'a Sample> + 'a>
}

impl<'a> JoinRightExclusiveIter<'a> {
    // todo: accept impl Iterator<Item=Sample>
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let left_iter = left.into_iter().map(|sample| (sample.timestamp, sample));
        let right_iter = right.into_iter().map(|sample| (sample.timestamp, sample));
        let iter = left_iter
            .into_iter()
            .hash_join_right_excl(right_iter)
            .flatten();

        Self {
            iter: Box::new(iter),
        }
    }
}

impl<'a> Iterator for JoinRightExclusiveIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.iter.next() {
            Some(JoinValue::right(item.timestamp, item.value))
        } else {
            None
        }
    }
}