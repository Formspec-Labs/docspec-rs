//! Duplicate-first proofs preserve matching rules and exact occurrence output.
// Rust guideline compliant 2026-02-21

use super::*;

#[tokio::test]
async fn composite_duplicates_only_refuse_when_the_constrained_keys_match() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let left = source(
        &engine,
        root.path(),
        "left",
        "initial",
        None,
        &[
            (Some(1), Some(1), Some("left-unmatched".into())),
            (Some(1), Some(1), Some("left-unmatched".into())),
            (Some(5), Some(5), Some("matched".into())),
        ],
    )
    .await;
    let right = source(
        &engine,
        root.path(),
        "right",
        "initial",
        None,
        &[
            (Some(2), Some(2), Some("right-unmatched".into())),
            (Some(2), Some(2), Some("right-unmatched".into())),
            (Some(5), Some(5), Some("matched".into())),
        ],
    )
    .await;
    for nulls in [NullMatching::Never, NullMatching::Equal] {
        let mut request = recipe(&left, &right);
        request.dataset = id(&format!("unmatched-{nulls:?}").to_lowercase());
        let definition = &mut join_mut(&mut request).join;
        definition.cardinality = JoinCardinality::OneToOne;
        definition.kind = JoinKind::Full;
        definition.nulls = nulls;
        let release = engine.refresh(&request).await.unwrap();
        check(
            &engine,
            &release,
            &oracle(&left, &right, &join(&request).join).await,
        )
        .await;
    }

    for (index, key) in [
        (None, Some(2)),
        (Some(1), None),
        (None, None),
        (Some(1), Some(2)),
    ]
    .into_iter()
    .enumerate()
    {
        let single = source(
            &engine,
            root.path(),
            &format!("single-{index}"),
            "initial",
            None,
            &[(key.0, key.1, Some("single".into()))],
        )
        .await;
        let duplicate = source(
            &engine,
            root.path(),
            &format!("duplicate-{index}"),
            "initial",
            None,
            &[
                (key.0, key.1, Some("first".into())),
                (key.0, key.1, Some("second".into())),
            ],
        )
        .await;
        for (side, left, right) in [
            (JoinSide::Left, &duplicate, &single),
            (JoinSide::Right, &single, &duplicate),
        ] {
            for nulls in [NullMatching::Never, NullMatching::Equal] {
                let mut request = recipe(left, right);
                request.dataset = id(&format!("matched-{index}-{side:?}-{nulls:?}").to_lowercase());
                let definition = &mut join_mut(&mut request).join;
                definition.kind = JoinKind::Full;
                definition.nulls = nulls;
                definition.cardinality = JoinCardinality::OneToOne;
                if nulls == NullMatching::Equal || (key.0.is_some() && key.1.is_some()) {
                    assert_eq!(
                        engine.refresh(&request).await.unwrap_err().code(),
                        "join_cardinality"
                    );
                } else {
                    let release = engine.refresh(&request).await.unwrap();
                    check(
                        &engine,
                        &release,
                        &oracle(left, right, &join(&request).join).await,
                    )
                    .await;
                }
            }
        }
        // The same immutable source can fill both roles; null equality still decides fanout.
        for nulls in [NullMatching::Never, NullMatching::Equal] {
            let mut request = recipe(&duplicate, &duplicate);
            request.dataset = id(&format!("self-{index}-{nulls:?}").to_lowercase());
            let definition = &mut join_mut(&mut request).join;
            definition.kind = JoinKind::Full;
            definition.nulls = nulls;
            definition.cardinality = JoinCardinality::OneToOne;
            if nulls == NullMatching::Equal || (key.0.is_some() && key.1.is_some()) {
                assert_eq!(
                    engine.refresh(&request).await.unwrap_err().code(),
                    "join_cardinality"
                );
            } else {
                let release = engine.refresh(&request).await.unwrap();
                check(
                    &engine,
                    &release,
                    &oracle(&duplicate, &duplicate, &join(&request).join).await,
                )
                .await;
            }
        }
    }
}
