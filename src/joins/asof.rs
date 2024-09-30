use crate::common::types::{Sample, SampleLike, Timestamp};
use std::cmp;
use std::time::Duration;

/// MergeAsOfMode describes the roll behavior of the asof merge
pub enum MergeAsOfMode{ RollPrior, RollFollowing, NoRoll }

pub struct PotentiallyUnmatchedPair<'a, TSample: SampleLike + Clone + Eq + Ord = Sample> {
    pub(crate) this: &'a TSample,
    pub(crate) other: Option<&'a TSample>,
}

pub fn merge_apply_asof<'a, TSample: SampleLike + Clone + Eq + Ord>(
    left_samples: &'a [TSample],
    other_samples: &'a [TSample],
    tolerance: &Duration,
    merge_mode: MergeAsOfMode) -> Vec<PotentiallyUnmatchedPair<'a, TSample>>
{
    let tolerance_ms = tolerance.as_millis() as i64;
    let compare_func = match merge_mode {
        MergeAsOfMode::RollFollowing => Some(merge_asof_fwd(tolerance_ms)),
        MergeAsOfMode::RollPrior => Some(merge_asof_prior(tolerance_ms)),
        _ => None
    };

    let other_idx_func:Option<Box<dyn Fn(usize)->usize>> = match merge_mode {
        MergeAsOfMode::RollFollowing => {
            let other_len = other_samples.len();
            Some(Box::new(move |idx: usize| fwd_func(idx, other_len)))
        },
        MergeAsOfMode::RollPrior => Some(Box::new(|idx: usize| prior_func(idx))),
        MergeAsOfMode::NoRoll => None
    };

    get_asof_merge_joined(
        left_samples,
        other_samples,
        compare_func,
        other_idx_func
    )
}

fn prior_func(idx: usize) -> usize {
    if idx == 0 {
        0
    } else {
        idx - 1
    }
}

fn fwd_func(idx: usize, other_len: usize) -> usize {
    if idx >= other_len - 1 {
        other_len - 1
    } else {
        idx + 1
    }
}

/// as of join. this is a variation of merge join that allows for indices to be equal based on a custom comparator func
fn get_asof_merge_joined<'a, TSample: SampleLike + Clone>(
    left: &'a [TSample],
    right: &'a [TSample],
    compare_func: Option<Box<dyn Fn(&Timestamp, &Timestamp, &Timestamp) -> (cmp::Ordering, i64)>>,
    other_idx_func: Option<Box<dyn Fn(usize) -> usize>>) -> Vec<PotentiallyUnmatchedPair<'a, TSample>>
{
    #![allow(clippy::type_complexity)]
    let mut output: Vec<PotentiallyUnmatchedPair<'a, TSample>> = Vec::new();
    let mut pos1: usize = 0;
    let mut pos2: usize = 0;

    let comp_func = match compare_func {
        Some(func) => func,
        None => Box::new(|this: &Timestamp, other: &Timestamp, _other_prior: &Timestamp| (this.cmp(&other), 0)) // use built in ordinal compare if no override
    };

    let cand_idx_func = match other_idx_func {
        Some(func) => func,
        None => Box::new(|idx| idx)
    };

    while pos1 < left.len() {
        let first = &left[pos1];
        let second = &right[pos2];
        let first_ts = first.timestamp();
        let second_ts = second.timestamp();
        let comp_res = comp_func(
            &first_ts,
            &second_ts,
            &right[cand_idx_func(pos2)].timestamp()
        );
        let offset = comp_res.1;
        match comp_res.0 {
            // (Evaluated as,  but is actually)
            cmp::Ordering::Greater => {
                output.push(PotentiallyUnmatchedPair {
                    this: first,
                    other: None,
                });
                if pos2 < (right.len() - 1) {
                    pos1 += 1; //the first index might still be longer so we gotta keep rolling it forward even though we are out of space on the other index
                }
            }
            cmp::Ordering::Less => {
                output.push(PotentiallyUnmatchedPair {
                    this: first,
                    other: None,
                });
                pos1 += 1;
            }
            cmp::Ordering::Equal => {
                let pas64: i64 = pos2.try_into().unwrap();
                let idx0: i64 = pas64 + offset;
                let other = right.get(idx0 as usize);
                output.push(PotentiallyUnmatchedPair {
                    this: first,
                    other,
                });
                if first_ts.eq(&second_ts) && pos2 < (right.len() - 1) { // only incr if things are actually equal and you have room to run
                    pos2 += 1;
                }
                pos1 += 1;
            }
        }
    }
    output
}

pub type MergeAsOfCompareFn = Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)>;

fn merge_asof_prior_impl(this: &i64, other: &i64, other_prior: &i64, lookback_ms: i64) -> (cmp::Ordering, i64) {
    let diff = this - other_prior;
    match diff {
        d if d < 0 && this != other => (cmp::Ordering::Less, 0),
        d if d > lookback_ms && this != other => (cmp::Ordering::Greater, 0),
        d if d <= lookback_ms && this != other => (cmp::Ordering::Equal, -1),
        _ => (cmp::Ordering::Equal, 0)
    }
}

fn merge_asof_fwd_impl(this: &i64, other: &i64, other_peak: &i64, lookback_ms: i64) -> (cmp::Ordering, i64) {
    let diff1 = other_peak - this;
    let diff2 = other - this;
    let diff = cmp::min(diff1, cmp::max(diff2, 0));
    let offset: i64 = if diff == diff2 { 0 } else { 1 };
    match diff {
        d if d < 0 && this != other => (cmp::Ordering::Greater, 0),
        d if d > lookback_ms && this != other => (cmp::Ordering::Less, 0),
        d if d <= lookback_ms && this != other => (cmp::Ordering::Equal, offset),
        _ => (cmp::Ordering::Equal, 0)
    }
}

// todo: Nearest

fn merge_asof_frontend(free_param: i64, func: fn(&i64, &i64, &i64, i64) -> (cmp::Ordering, i64)) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    Box::new(move |this: &i64, other: &i64, other_peak: &i64| func(this, other, other_peak, free_param))
}

/// Implementation for mergeasof for a given duration lookback for a pair of Timeseries that has a HashableIndex<i64>
fn merge_asof_prior(look_back: i64) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    merge_asof_frontend(look_back, merge_asof_prior_impl)
}
/// Implementation for mergeasof for a given duration look-forward for a pair of Timeseries that has a HashableIndex<i64>
fn merge_asof_fwd(look_fwd: i64) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    merge_asof_frontend(look_fwd, merge_asof_fwd_impl)
}

#[cfg(test)]
mod tests {
    use super::{merge_apply_asof, MergeAsOfMode, PotentiallyUnmatchedPair};
    use crate::common::types::{SampleLike, Timestamp};
    use crate::joins::TimeSeriesDataPoint;
    use metricsql_runtime::types::Sample;
    use std::time::Duration;

    fn create_samples(timestamps: &[Timestamp], values: &[f64]) -> Vec<Sample> {
        timestamps.iter()
            .zip(values.iter())
            .map(|(t, v)| Sample::new(*t, *v))
            .collect()
    }

    fn convert_pair(value: &PotentiallyUnmatchedPair) -> TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)> {
        TimeSeriesDataPoint {
            timestamp: value.this.timestamp(),
            value: (
                value.this.value(),
                match value.other {
                    Some(sample) => Some(sample.value),
                    None => None
                })
        }
    }

    #[test]
    fn test_merge_asof_prior() {

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let tolerance = Duration::from_secs(1);
        let result = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollPrior)
            .iter()
            .map(convert_pair)
            .collect::<Vec<_>>();

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_merge_asof_lookingforward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let tolerance = Duration::from_millis(1);

        let custom: Vec<_> = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollFollowing)
            .iter()
            .map(convert_pair)
            .collect();

        let tolerance = Duration::from_millis(2);
        let custom2: Vec<_> = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollFollowing)
            .iter()
            .map(convert_pair)
            .collect();

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        assert_eq!(custom, expected);
        assert_eq!(custom2, expected1);
    }
}