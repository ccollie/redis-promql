use crate::common::types::Sample;
use min_max_heap::MinMaxHeap;
use smallvec::SmallVec;

/// Iterate over multiple Sample iter, returning the samples in timestamp order
pub struct MultiSeriesSampleIter {
    heap: MinMaxHeap<Sample>,
    iter_list: Vec<Box<dyn Iterator<Item=Sample>>>,
}

impl MultiSeriesSampleIter {
    pub fn new(list: Vec<Box<dyn Iterator<Item=Sample>>>) -> Self {
        let len = list.len();
        Self {
            iter_list: list,
            heap: MinMaxHeap::with_capacity(len),
        }
    }

    fn push_samples_to_heap(&mut self) -> bool {
        if !self.iter_list.is_empty() {
            let mut to_remove: SmallVec<usize, 4> = SmallVec::new();
            let mut sample_added = false;

            for (i, iter) in self.iter_list.iter_mut().enumerate() {
                if let Some(sample) = iter.next() {
                    self.heap.push(sample);
                    sample_added = true;
                } else {
                    to_remove.push(i);
                }
            }
            if !to_remove.is_empty() {
                for i in to_remove.iter().rev() {
                    let _ = self.iter_list.swap_remove(*i);
                }
            }
            return sample_added;
        }
        false
    }
}

impl Iterator for MultiSeriesSampleIter {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            if !self.push_samples_to_heap() {
                return None;
            }
        }
        self.heap.pop_min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_supports_duplicates() {
        let mut heap = MinMaxHeap::<u32>::new();

        heap.push(1);
        heap.push(1);
        assert_eq!(heap.len(), 2);
        heap.push(1);
        assert_eq!(heap.len(), 3);
        heap.push(2);
        heap.push(3);
        heap.push(3);
        assert_eq!(heap.len(), 6);
        let mut vec = heap.into_vec();
        vec.sort();
        assert_eq!(vec, vec![1, 1, 1, 2, 3, 3]);
    }
}