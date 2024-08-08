pub struct FilterByValueArgs {
    pub min: f64,
    pub max: f64,
}

fn check_sample_value(value: f64, by_value_args: &FilterByValueArgs) -> bool {
    value >= by_value_args.min && value <= by_value_args.max
}

pub enum BucketTimestamp {
    BucketStartTimestamp,
    BucketMidTimestamp,
    BucketEndTimestamp,
}