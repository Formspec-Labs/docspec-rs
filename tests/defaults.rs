//! Additions retain absence independently of nulls, values, and physical rewrites.

#[path = "support/scalar.rs"]
mod scalar;

use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    engine::verify_release,
    model::{
        ColumnAddition, ColumnType, InputSpec, ReleasePin, RunSpec, SchemaField, SchemaSpec,
        UpdateSpec,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use scalar::{fixture, id, input, limits, spec};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
    sync::Arc,
};

fn edit(base: &ReleasePin, operation: &str) -> SchemaSpec {
    SchemaSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: base.dataset.definition.clone(),
        next_definition: operation.into(),
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".into(),
            },
            SchemaField {
                id: 2,
                name: "text".into(),
            },
        ],
        additions: vec![
            ColumnAddition {
                name: "score".into(),
                data_type: ColumnType::Long,
                default: 7.into(),
            },
            ColumnAddition {
                name: "note".into(),
                data_type: ColumnType::String,
                default: "untagged".into(),
            },
            ColumnAddition {
                name: "accepted".into(),
                data_type: ColumnType::Boolean,
                default: false.into(),
            },
            ColumnAddition {
                name: "weight".into(),
                data_type: ColumnType::Double,
                default: serde_json::json!(0.5),
            },
        ],
        write_defaults: BTreeMap::new(),
    }
}

async fn unchanged_edit(base: &ReleasePin, operation: &str) -> SchemaSpec {
    let native = storage::open_pin(&base.dataset.groups["source"])
        .await
        .unwrap();
    let mut request = edit(base, operation);
    request.additions.clear();
    request.fields = native
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|f| !f.name.starts_with("__docspec_"))
        .map(|f| SchemaField {
            id: f.id,
            name: f.name.clone(),
        })
        .collect();
    request
}

fn payloads(release: &ReleasePin) -> BTreeSet<String> {
    release.dataset.groups["source"]
        .objects
        .iter()
        .filter(|o| o.path.ends_with(".parquet"))
        .map(|o| o.path.clone())
        .collect()
}

type Row = (Option<i64>, Option<i64>, bool);

#[derive(Debug, PartialEq)]
struct Values {
    id: Option<i64>,
    score: (Option<i64>, bool),
    note: (Option<String>, bool),
    accepted: (Option<bool>, bool),
    weight: (Option<f64>, bool),
}

fn initial_values(id: Option<i64>) -> Values {
    Values {
        id,
        score: (Some(7), false),
        note: (Some("untagged".into()), false),
        accepted: (Some(false), false),
        weight: (Some(0.5), false),
    }
}

async fn all_values(engine: &Engine, release: &ReleasePin) -> Vec<Values> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut result = vec![];
    while let Some(batch) = stream.try_next().await.unwrap() {
        for (name, kind) in [
            ("id", DataType::Int64),
            ("score", DataType::Int64),
            ("accepted", DataType::Boolean),
            ("weight", DataType::Float64),
        ] {
            assert_eq!(batch.column_by_name(name).unwrap().data_type(), &kind);
        }
        let values = |name: &str, kind: &DataType| {
            arrow::compute::cast(batch.column_by_name(name).unwrap(), kind).unwrap()
        };
        let ids = values("id", &DataType::Int64);
        let score = values("score", &DataType::Int64);
        let note = values("note", &DataType::Utf8);
        let accepted = values("accepted", &DataType::Boolean);
        let weight = values("weight", &DataType::Float64);
        let flags = [3, 5, 7, 9].map(|id| {
            let flag = batch
                .column_by_name(&format!("__docspec_supplied_{id}"))
                .unwrap();
            assert_eq!(flag.null_count(), 0);
            flag.as_any().downcast_ref::<BooleanArray>().unwrap()
        });
        for i in 0..batch.num_rows() {
            result.push(Values {
                id: (!ids.is_null(i))
                    .then(|| ids.as_any().downcast_ref::<Int64Array>().unwrap().value(i)),
                score: (
                    (!score.is_null(i)).then(|| {
                        score
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i)
                    }),
                    flags[0].value(i),
                ),
                note: (
                    (!note.is_null(i)).then(|| {
                        note.as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(i)
                            .to_owned()
                    }),
                    flags[1].value(i),
                ),
                accepted: (
                    (!accepted.is_null(i)).then(|| {
                        accepted
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(i)
                    }),
                    flags[2].value(i),
                ),
                weight: (
                    (!weight.is_null(i)).then(|| {
                        weight
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(i)
                    }),
                    flags[3].value(i),
                ),
            });
        }
    }
    result.sort_by_key(|v| v.id);
    result
}

async fn rows(engine: &Engine, release: &ReleasePin) -> Vec<Row> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut result = vec![];
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let score = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let supplied = batch
            .column_by_name("__docspec_supplied_3")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(supplied.null_count(), 0);
        result.extend(
            ids.iter()
                .zip(score.iter())
                .zip(supplied.values().iter())
                .map(|((id, score), supplied)| (id, score, supplied)),
        );
    }
    result.sort();
    result
}

fn replacements(path: &Path, values: &[Values]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
        Field::new("__docspec_supplied_3", DataType::Boolean, false),
        Field::new("note", DataType::Utf8, true),
        Field::new("__docspec_supplied_5", DataType::Boolean, false),
        Field::new("accepted", DataType::Boolean, true),
        Field::new("__docspec_supplied_7", DataType::Boolean, false),
        Field::new("weight", DataType::Float64, true),
        Field::new("__docspec_supplied_9", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|v| v.id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![Some("changed"); values.len()])),
            Arc::new(Int64Array::from(
                values.iter().map(|v| v.score.0).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.score.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|v| v.note.0.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.note.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.accepted.0).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.accepted.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                values.iter().map(|v| v.weight.0).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.weight.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

#[tokio::test]
async fn additions_updates_defaults_and_rebuild_preserve_supplied_state_and_history() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, original) = fixture(root.path(), false).await;
    let payloads_before = payloads(&base);
    let request = edit(&base, "added");
    let added = engine.evolve_schema(&request).await.unwrap();
    assert_eq!(payloads(&added), payloads_before);
    let native = storage::open_pin(&added.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(
        native.metadata().format_version(),
        iceberg::spec::FormatVersion::V3
    );
    assert_eq!(native.metadata().last_column_id(), 10);
    let mut expected = original
        .iter()
        .map(|v| (v.0, Some(7), false))
        .collect::<Vec<_>>();
    expected.sort();
    let old_expected = expected.clone();
    assert_eq!(rows(&engine, &added).await, expected);
    let mut expected_all = original
        .iter()
        .map(|v| initial_values(v.0))
        .collect::<Vec<_>>();
    expected_all.sort_by_key(|v| v.id);
    assert_eq!(all_values(&engine, &added).await, expected_all);
    assert_eq!(engine.evolve_schema(&request).await.unwrap(), added);
    let mut conflict = request.clone();
    conflict.additions[0].default = 8.into();
    assert_eq!(
        engine.evolve_schema(&conflict).await.unwrap_err().code(),
        "operation_conflict"
    );
    let mut group = spec(
        &added,
        "computed",
        "presence",
        "was_supplied",
        "CASE WHEN __docspec_supplied_3 THEN 1 ELSE 0 END",
    );
    group.definition = added.dataset.definition.clone();
    let enriched = engine.backfill(&group).await.unwrap();
    let mut policy = unchanged_edit(&enriched, "future-nine").await;
    policy.write_defaults.insert(3, 9.into());
    policy.write_defaults.insert(5, "future".into());
    policy.write_defaults.insert(7, true.into());
    policy.write_defaults.insert(9, serde_json::json!(1.5));
    let changed = engine.evolve_schema(&policy).await.unwrap();
    assert_eq!(
        changed.dataset.groups["presence"]
            .objects
            .iter()
            .filter(|o| o.path.ends_with(".parquet"))
            .map(|o| (o.path.clone(), o.digest.clone(), o.bytes))
            .collect::<BTreeSet<_>>(),
        enriched.dataset.groups["presence"]
            .objects
            .iter()
            .filter(|o| o.path.ends_with(".parquet"))
            .map(|o| (o.path.clone(), o.digest.clone(), o.bytes))
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(rows(&engine, &changed).await, expected);
    assert_eq!(all_values(&engine, &changed).await, expected_all);
    let incoming = [
        Values {
            id: Some(1),
            score: (None, true),
            note: (None, true),
            accepted: (None, true),
            weight: (None, true),
        },
        Values {
            id: Some(5),
            score: (Some(9), true),
            note: (Some("future".into()), true),
            accepted: (Some(true), true),
            weight: (Some(1.5), true),
        },
        Values {
            score: (Some(123), false),
            ..initial_values(Some(1001))
        },
    ];
    let update = UpdateSpec {
        dataset: changed.dataset.dataset.clone(),
        operation: id("updated"),
        definition: changed.dataset.definition.clone(),
        base: changed.id.clone(),
        keys: vec!["id".into()],
        replacements: replacements(&root.path().join("updates.parquet"), &incoming),
        deletions: None,
    };
    let updated = engine.update(&update).await.unwrap();
    expected.retain(|row| ![Some(1), Some(5), Some(1001)].contains(&row.0));
    expected.extend([
        (Some(1), None, true),
        (Some(5), Some(9), true),
        (Some(1001), Some(9), false),
    ]);
    expected.sort();
    assert_eq!(rows(&engine, &updated).await, expected);
    expected_all.retain(|v| ![Some(1), Some(5), Some(1001)].contains(&v.id));
    expected_all.extend([
        Values {
            id: Some(1),
            score: (None, true),
            note: (None, true),
            accepted: (None, true),
            weight: (None, true),
        },
        Values {
            id: Some(5),
            score: (Some(9), true),
            note: (Some("future".into()), true),
            accepted: (Some(true), true),
            weight: (Some(1.5), true),
        },
        Values {
            id: Some(1001),
            score: (Some(9), false),
            note: (Some("future".into()), false),
            accepted: (Some(true), false),
            weight: (Some(1.5), false),
        },
    ]);
    expected_all.sort_by_key(|v| v.id);
    assert_eq!(all_values(&engine, &updated).await, expected_all);
    assert!(
        !payloads(&updated).is_disjoint(&payloads_before),
        "an untouched physical file remains shared"
    );
    let mut combined = engine.read_combined(&updated).await.unwrap();
    assert!(
        !combined
            .schema()
            .fields()
            .iter()
            .any(|f| f.name().starts_with("__docspec_supplied_"))
    );
    let mut provided = 0;
    while let Some(batch) = combined.try_next().await.unwrap() {
        let values = batch.column_by_name("was_supplied").unwrap();
        let values = arrow::compute::cast(values, &DataType::Int64).unwrap();
        provided += values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .sum::<i64>();
    }
    assert_eq!(provided, 2);
    verify_release(&updated, limits()).await.unwrap();
    assert_eq!(rows(&engine, &added).await, old_expected);
    drop(combined);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let historical = engine
        .release(&added.dataset.dataset, &added.id)
        .await
        .unwrap();
    let mut old_all = original
        .iter()
        .map(|v| initial_values(v.0))
        .collect::<Vec<_>>();
    old_all.sort_by_key(|v| v.id);
    assert_eq!(all_values(&engine, &historical).await, old_all);
    let fresh = input(
        &root.path().join("fresh.parquet"),
        &[(Some(7), Some("fresh".into()))],
    );
    let rebuild = RunSpec {
        dataset: updated.dataset.dataset.clone(),
        operation: id("rebuilt"),
        base: Some(updated.id.clone()),
        relationship: None,
        input: fresh.path,
        expected_digest: fresh.digest,
        sql: "SELECT text, id FROM input".into(),
        definition: updated.dataset.definition.clone(),
    };
    let rebuilt = engine.run(&rebuild).await.unwrap();
    assert_eq!(
        rows(&engine, &rebuilt).await,
        vec![(Some(7), Some(9), false)]
    );
    assert_eq!(
        all_values(&engine, &rebuilt).await,
        vec![Values {
            id: Some(7),
            score: (Some(9), false),
            note: (Some("future".into()), false),
            accepted: (Some(true), false),
            weight: (Some(1.5), false)
        }]
    );
    assert_eq!(
        storage::open_pin(&rebuilt.dataset.groups["source"])
            .await
            .unwrap()
            .metadata()
            .current_schema(),
        storage::open_pin(&updated.dataset.groups["source"])
            .await
            .unwrap()
            .metadata()
            .current_schema()
    );
    verify_release(&rebuilt, limits()).await.unwrap();
    let mut unfenced = rebuild.clone();
    unfenced.operation = id("unfenced");
    unfenced.base = None;
    assert_eq!(
        engine.run(&unfenced).await.unwrap_err().code(),
        "dependent_groups"
    );
    let mut cleared = unchanged_edit(&rebuilt, "cleared").await;
    cleared.write_defaults = [3, 5, 7, 9].map(|id| (id, serde_json::Value::Null)).into();
    let cleared = engine.evolve_schema(&cleared).await.unwrap();
    assert_eq!(
        all_values(&engine, &cleared).await,
        all_values(&engine, &rebuilt).await
    );
    let mut nullable = rebuild.clone();
    nullable.operation = id("nullable");
    nullable.base = Some(cleared.id.clone());
    nullable.definition = cleared.dataset.definition.clone();
    assert!(
        engine
            .plan(&nullable)
            .await
            .unwrap()
            .required_phases
            .contains(&"processing".into())
    );
    let omitted = engine.run(&nullable).await.unwrap();
    assert_eq!(
        all_values(&engine, &omitted).await,
        vec![Values {
            id: Some(7),
            score: (None, false),
            note: (None, false),
            accepted: (None, false),
            weight: (None, false)
        }]
    );
    nullable.operation = id("explicit-null");
    nullable.base = Some(omitted.id.clone());
    nullable.sql = "SELECT id, text, CAST(NULL AS BIGINT) AS score, CAST(NULL AS VARCHAR) AS note, CAST(NULL AS BOOLEAN) AS accepted, CAST(NULL AS DOUBLE) AS weight FROM input".into();
    let explicit = engine.run(&nullable).await.unwrap();
    assert_eq!(
        all_values(&engine, &explicit).await,
        vec![Values {
            id: Some(7),
            score: (None, true),
            note: (None, true),
            accepted: (None, true),
            weight: (None, true)
        }]
    );
    verify_release(&explicit, limits()).await.unwrap();
}

#[tokio::test]
async fn invalid_additions_refuse_before_admission_and_empty_sources_keep_typed_rules() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), true).await;
    let before = engine.inspect(&base.dataset.dataset).await.unwrap();
    for case in ["type", "duplicate", "reserved", "large", "unknown-default"] {
        let mut request = edit(&base, case);
        match case {
            "type" => request.additions[0].default = "not a long".into(),
            "duplicate" => request.additions[0].name = "TEXT".into(),
            "reserved" => request.additions[0].name = "__docspec_supplied_1".into(),
            "large" => request.additions[1].default = "x".repeat(300_000).into(),
            "unknown-default" => {
                request.write_defaults.insert(1, 1.into());
            }
            _ => unreachable!(),
        }
        assert!(engine.plan_schema(&request).await.is_err(), "{case}");
        assert!(engine.evolve_schema(&request).await.is_err(), "{case}");
        assert_eq!(
            engine.inspect(&base.dataset.dataset).await.unwrap(),
            before,
            "{case}"
        );
    }
    let added = engine
        .evolve_schema(&edit(&base, "empty-added"))
        .await
        .unwrap();
    assert!(rows(&engine, &added).await.is_empty());
    let before = engine.inspect(&base.dataset.dataset).await.unwrap();
    for (operation, name) in [("marker-rename", "exposed"), ("marker-reorder", "flag")] {
        let mut marker = unchanged_edit(&added, operation).await;
        marker.fields.retain(|f| f.id != 3);
        marker.fields.insert(
            0,
            SchemaField {
                id: 4,
                name: name.into(),
            },
        );
        assert_eq!(
            engine.evolve_schema(&marker).await.unwrap_err().code(),
            "invalid_schema"
        );
        assert_eq!(engine.inspect(&base.dataset.dataset).await.unwrap(), before);
    }
    let mut renamed = unchanged_edit(&added, "renamed").await;
    renamed.fields.iter_mut().find(|f| f.id == 3).unwrap().name = "score_new".into();
    let renamed = engine.evolve_schema(&renamed).await.unwrap();
    let native = storage::open_pin(&renamed.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(
        native
            .metadata()
            .current_schema()
            .field_by_name("score_new")
            .unwrap()
            .id,
        3
    );
    assert!(
        native
            .metadata()
            .current_schema()
            .field_by_name("__docspec_supplied_3")
            .is_some()
    );
    let mut dropped = unchanged_edit(&renamed, "dropped").await;
    dropped.fields.retain(|f| f.id <= 2);
    let dropped = engine.evolve_schema(&dropped).await.unwrap();
    let incoming = input(
        &root.path().join("fresh.parquet"),
        &[(Some(7), Some("fresh".into()))],
    );
    let mut request = RunSpec {
        dataset: dropped.dataset.dataset.clone(),
        operation: id("unfenced"),
        base: None,
        relationship: None,
        input: incoming.path,
        expected_digest: incoming.digest,
        sql: "SELECT text, id FROM input".into(),
        definition: dropped.dataset.definition.clone(),
    };
    assert_eq!(
        engine.run(&request).await.unwrap_err().code(),
        "schema_base_required"
    );
    request.operation = id("rebuilt");
    request.base = Some(dropped.id.clone());
    let rebuilt = engine.run(&request).await.unwrap();
    let native = storage::open_pin(&rebuilt.dataset.groups["source"])
        .await
        .unwrap();
    let prior = storage::open_pin(&dropped.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(native.metadata().last_column_id(), 10);
    assert_eq!(
        native.metadata().current_schema(),
        prior.metadata().current_schema()
    );
    assert_eq!(
        native
            .metadata()
            .schemas_iter()
            .map(|s| (s.schema_id(), s))
            .collect::<BTreeMap<_, _>>(),
        prior
            .metadata()
            .schemas_iter()
            .map(|s| (s.schema_id(), s))
            .collect::<BTreeMap<_, _>>()
    );
    verify_release(&rebuilt, limits()).await.unwrap();
    // Physical source heads change, while identical material rules remain reusable.
    request.operation = id("reused");
    request.base = Some(rebuilt.id.clone());
    std::fs::remove_file(&request.input).unwrap();
    assert!(
        engine
            .plan(&request)
            .await
            .unwrap()
            .reusable_phases
            .contains(&"processing".into())
    );
    let reused = engine.run(&request).await.unwrap();
    assert_eq!(
        reused.dataset.groups["source"],
        rebuilt.dataset.groups["source"]
    );
    verify_release(&reused, limits()).await.unwrap();
    // Rehashing a valid candidate with a different allocation high-water mark
    // cannot make it satisfy the exact schema-history relationship to its base.
    let mut forged = rebuilt.clone();
    let pin = forged.dataset.groups.get_mut("source").unwrap();
    let old = pin.metadata.clone();
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&old.path).unwrap()).unwrap();
    metadata["last-column-id"] = 99.into();
    let path = root.path().join("forged.metadata.json");
    std::fs::write(&path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let (digest, bytes) = storage::digest(&path).unwrap();
    pin.metadata = docspec::model::ObjectRef {
        owner: old.owner,
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    };
    *pin.objects.iter_mut().find(|o| o.path == old.path).unwrap() = pin.metadata.clone();
    let path = root.path().join("forged.json");
    std::fs::write(&path, serde_json::to_vec(&forged.dataset).unwrap()).unwrap();
    let (digest, bytes) = storage::digest(&path).unwrap();
    forged.manifest = docspec::model::ObjectRef {
        owner: forged.dataset.dataset.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    };
    assert_eq!(
        verify_release(&forged, limits()).await.unwrap_err().code(),
        "invalid_schema"
    );
    let mut readd = unchanged_edit(&reused, "readded").await;
    readd.additions = vec![ColumnAddition {
        name: "score_new".into(),
        data_type: ColumnType::Long,
        default: 5.into(),
    }];
    let readded = engine.evolve_schema(&readd).await.unwrap();
    let native = storage::open_pin(&readded.dataset.groups["source"])
        .await
        .unwrap();
    assert_eq!(
        native
            .metadata()
            .current_schema()
            .field_by_name("score_new")
            .unwrap()
            .id,
        11
    );
    assert_eq!(native.metadata().last_column_id(), 12);
    verify_release(&readded, limits()).await.unwrap();
    assert!(rows(&engine, &added).await.is_empty());
}

#[tokio::test]
async fn update_scope_refuses_omitted_keys_without_changing_selection() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = fixture(root.path(), false).await;
    let added = engine.evolve_schema(&edit(&base, "added")).await.unwrap();
    let update = UpdateSpec {
        dataset: added.dataset.dataset.clone(),
        operation: id("missing-key"),
        definition: added.dataset.definition.clone(),
        base: added.id.clone(),
        keys: vec!["score".into()],
        deletions: None,
        replacements: replacements(
            &root.path().join("missing.parquet"),
            &[Values {
                score: (Some(123), false),
                ..initial_values(Some(42))
            }],
        ),
    };
    assert_eq!(
        engine.update(&update).await.unwrap_err().code(),
        "invalid_presence"
    );
    assert_eq!(engine.latest(&update.dataset).await.unwrap(), added);
    assert_eq!(payloads(&added), payloads(&base));
    // A key explicitly equal to the default intentionally selects that entire group.
    let mut supplied = update.clone();
    supplied.operation = id("supplied-key");
    supplied.replacements = replacements(
        &root.path().join("supplied.parquet"),
        &[Values {
            score: (Some(7), true),
            ..initial_values(Some(42))
        }],
    );
    let result = engine.update(&supplied).await.unwrap();
    assert_eq!(
        rows(&engine, &result).await,
        vec![(Some(42), Some(7), true)]
    );
    verify_release(&result, limits()).await.unwrap();
}
