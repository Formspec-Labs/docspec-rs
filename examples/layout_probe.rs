//! Temporary native layout diagnostic for the default-stack regression.
fn main() {
    use docspec::model::{Authority, DatasetPin, PhaseResult, RelatedResult, ReleasePin};
    println!(
        "DatasetPin={} ReleasePin={} Authority={} PhaseResult={} RelatedResult={}",
        std::mem::size_of::<DatasetPin>(),
        std::mem::size_of::<ReleasePin>(),
        std::mem::size_of::<Authority>(),
        std::mem::size_of::<PhaseResult>(),
        std::mem::size_of::<RelatedResult>()
    );
}
