//! Scratch-only Basin maintenance experiment; no application tables or policy.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_names_and_noncanonical_probe_names_refuse() {
        for name in ["captures", "docspec_m1_captures", "../raw", ""] {
            assert!(validate_namespace(name).is_err());
        }
        assert!(validate_namespace("docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272").is_ok());
    }

    #[test]
    fn no_format_verdict_before_ten_complete_maintenance_observations() {
        assert_eq!(format_verdict(9, true, 1000, true), "incomplete");
        assert_eq!(format_verdict(10, true, 59999, true), "iceberg");
        assert_eq!(format_verdict(10, true, 60000, true), "dated_parquet");
        assert_eq!(format_verdict(10, false, 1000, true), "dated_parquet");
        assert_eq!(format_verdict(10, true, 1000, false), "incomplete");
    }
}
