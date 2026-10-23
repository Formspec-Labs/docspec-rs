//! Temporary native layout diagnostic for the default-stack regression.
fn main() {
    use docspec::model::{Authority, DatasetPin, PhaseResult, RelatedResult, ReleasePin};
    let dp = std::mem::size_of::<DatasetPin>();
    let rp = std::mem::size_of::<ReleasePin>();
    let au = std::mem::size_of::<Authority>();
    let pr = std::mem::size_of::<PhaseResult>();
    let rr = std::mem::size_of::<RelatedResult>();
    println!(
        "DatasetPin={} ReleasePin={} Authority={} PhaseResult={} RelatedResult={}",
        dp, rp, au, pr, rr
    );
    assert!(dp <= 1024, "DatasetPin exceeded 1024 bytes");
    assert!(rp <= 1024, "ReleasePin exceeded 1024 bytes");
    assert!(au <= 2048, "Authority exceeded 2048 bytes");
    assert!(pr <= 2048, "PhaseResult exceeded 2048 bytes");
    assert!(rr <= 1536, "RelatedResult exceeded 1536 bytes");
}
