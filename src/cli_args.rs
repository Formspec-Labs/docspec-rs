//! Positional-to-long-flag normalization for backward-compatible CLI invocations.

use std::ffi::OsString;

fn subcommand_positional_flags(cmd: &str) -> &'static [&'static str] {
    match cmd {
        "pause-after" => &["--spec", "--phase"],
        "hash" => &["--input"],
        "plan"
        | "run"
        | "resume"
        | "export"
        | "compact"
        | "seal-generation"
        | "capture-inputs"
        | "correct-dependencies"
        | "plan-refresh"
        | "refresh"
        | "plan-update"
        | "update"
        | "plan-evidence"
        | "evidence"
        | "plan-schema"
        | "schema"
        | "plan-backfill"
        | "backfill"
        | "plan-continuous-backfill"
        | "continuous-backfill" => &["--spec"],
        "inspect" | "latest" | "compact-lifecycle" => &["--dataset"],
        "fence-operations" => &["--dataset", "--expected-epoch"],
        "inspect-changes" => &["--version"],
        "inspect-retention" => &["--objects"],
        "plan-removal" => &["--dataset", "--removal", "--objects", "--reason"],
        "plan-export-stage-removal" => &["--dataset", "--removal", "--operation", "--reason"],
        "plan-export-file-removal" => &[
            "--dataset",
            "--removal",
            "--operation",
            "--file",
            "--reason",
        ],
        "plan-export-retirement"
        | "plan-availability-retirement"
        | "plan-operation-retirement"
        | "plan-compaction-retirement" => &["--dataset", "--retirement", "--operation", "--reason"],
        "retire-export"
        | "remove-objects"
        | "prepare-remote-removal"
        | "retire-phases"
        | "retire-changes"
        | "retire-restore-points"
        | "retire-generation"
        | "retire-grants"
        | "retire-availability"
        | "retire-operation"
        | "retire-compaction" => &["--plan"],
        "resume-remote-removal" => &["--dataset", "--removal", "--authority"],
        "inspect-removal" | "resume-removal" => &["--dataset", "--removal"],
        "plan-phase-retirement" => &["--dataset", "--retirement", "--keys", "--reason"],
        "plan-change-retirement" => &["--dataset", "--retirement", "--versions", "--reason"],
        "plan-restore-point-retirement" => &["--dataset", "--retirement", "--points", "--reason"],
        "plan-generation-retirement" => &["--family", "--retirement", "--generation", "--reason"],
        "plan-grant-retirement" => &["--owner", "--retirement", "--keys", "--reason"],
        "abandon" => &["--dataset", "--operation", "--reason"],
        "release" => &["--dataset", "--version"],
        "latest-export" => &["--dataset", "--channel"],
        "export-pin" => &["--dataset", "--operation"],
        "read-export" => &["--pin", "--output"],
        "create-restore-point" => &["--dataset", "--point"],
        "restore-remote" => &["--point", "--target"],
        "restore-local" => &["--point"],
        "check-succession" => &["--prior", "--target"],
        "verify-inputs" => &["--coverage"],
        "read-combined" | "read-evidence" | "read-receipts" | "read-occurrences" | "read" => {
            &["--release", "--output"]
        }
        "read-keys" => &["--release", "--spec", "--output"],
        "compare" => &["--before", "--after"],
        "changes" => &["--before", "--after", "--output"],
        "invalidate" => &["--dataset", "--reason"],
        "verify" => &["--release"],
        _ => &[],
    }
}

fn is_boolean_flag(flag: &str) -> bool {
    matches!(
        flag,
        "--allow-unregistered-import"
            | "--occurrences"
            | "--values"
            | "--force-full"
            | "--deep"
            | "--shutdown-on-stdin-eof"
            | "-h"
            | "--help"
            | "-V"
            | "--version"
    )
}

pub(crate) fn normalize_cli_args(raw: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let args: Vec<OsString> = raw.into_iter().collect();
    if args.is_empty() {
        return args;
    }
    let mut out = Vec::with_capacity(args.len() + 8);
    out.push(args[0].clone());
    let (mut i, mut subcommand, mut next_pos) = (1, None::<String>, 0usize);
    let mut positional_flags: Vec<&'static str> = Vec::new();
    while i < args.len() {
        let arg = &args[i];
        let s = arg.to_str().unwrap_or("");
        if subcommand.is_none() {
            if s.starts_with('-') {
                out.push(arg.clone());
                if !s.contains('=') && !is_boolean_flag(s) && i + 1 < args.len() {
                    i += 1;
                    out.push(args[i].clone());
                }
            } else {
                subcommand = Some(s.to_owned());
                positional_flags = subcommand_positional_flags(s).to_vec();
                out.push(arg.clone());
            }
        } else if s.starts_with('-') {
            let flag_name = s.split('=').next().unwrap_or(s);
            if let Some(pos) = positional_flags.iter().position(|f| *f == flag_name) {
                positional_flags[pos] = "";
            }
            out.push(arg.clone());
            if !s.contains('=') && !is_boolean_flag(flag_name) && i + 1 < args.len() {
                i += 1;
                out.push(args[i].clone());
            }
        } else {
            while next_pos < positional_flags.len() && positional_flags[next_pos].is_empty() {
                next_pos += 1;
            }
            if next_pos < positional_flags.len() {
                out.push(OsString::from(positional_flags[next_pos]));
                next_pos += 1;
            }
            out.push(arg.clone());
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cli, CliComparisonIdentity, Command};
    use clap::Parser;

    fn parse_cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(normalize_cli_args(args.iter().map(OsString::from))).unwrap()
    }

    #[test]
    fn cli_accepts_both_long_flags_and_positional_args() {
        let by_flags = parse_cli(&[
            "docspec",
            "plan-phase-retirement",
            "--dataset",
            "comments",
            "--retirement",
            "prepared",
            "--keys",
            "keys.json",
            "--reason",
            "done",
        ]);
        let by_pos = parse_cli(&[
            "docspec",
            "plan-phase-retirement",
            "comments",
            "prepared",
            "keys.json",
            "done",
        ]);
        assert_eq!(format!("{by_flags:?}"), format!("{by_pos:?}"));

        let compare_mode = parse_cli(&[
            "docspec",
            "compare",
            "--before",
            "b.json",
            "--after",
            "a.json",
            "--mode",
            "occurrences",
        ]);
        assert!(matches!(
            compare_mode.command,
            Command::Compare {
                identity: Some(CliComparisonIdentity::Occurrences),
                ..
            }
        ));
    }
}
