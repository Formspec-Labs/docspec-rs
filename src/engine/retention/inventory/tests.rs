//! Unit tests for scalar origins, dependency corrections, release history, and readback fields.

use super::*;
use crate::engine::backfill::origins::OriginalExecution;

mod scalar_origins_tests {
    use super::*;

    fn object(name: &str) -> ObjectRef {
        ObjectRef {
            owner: Id::new("owner").unwrap(),
            path: format!("/unavailable/{name}"),
            digest: "0".repeat(64),
            bytes: 17,
        }
    }

    fn origin() -> RetentionBlocker {
        RetentionBlocker {
            table: "owner".into(),
            key: "phase/backfill/file".into(),
            revision: 1,
        }
    }

    #[test]
    fn shallow_original_roots_keep_schema_as_a_leaf_and_do_not_queue_reads() {
        let phase = FileCompletion {
            execution: Some(OriginalExecution {
                operation: Id::new("original-operation").unwrap(),
                declaration: object("declaration.json"),
            }),
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output.json"),
            outputs: vec![object("output.parquet")],
        };
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        inventory.shallow = true;
        inventory.file_completion(&phase, &origin()).unwrap();
        let origins = object("origins.json");
        inventory
            .enqueue(&origins, Expansion::Origins, &origin())
            .unwrap();
        inventory
            .enqueue(&origins, Expansion::Origins, &origin())
            .unwrap();
        assert!(inventory.pending.is_empty());
        assert!(inventory.pending_native.is_empty());
        assert!(
            !inventory
                .visited
                .contains_key("/unavailable/old-schema.json")
        );
        assert_eq!(inventory.visited.len(), 2);
        assert!(
            inventory
                .enqueue(&origins, Expansion::Declaration, &origin())
                .is_err()
        );
        let references = inventory.into_references().unwrap();
        for reference in [
            phase.source,
            phase.source_metadata.unwrap(),
            phase.descriptor,
            phase.outputs[0].clone(),
            phase.execution.unwrap().declaration,
            origins,
        ] {
            assert!(references.contains(&reference));
        }
    }

    #[tokio::test]
    async fn typed_origins_expand_once_and_retain_original_file_closure() {
        let root = tempfile::tempdir().unwrap();
        let budget = Budget::new(crate::budget::Limits {
            compute_threads: 1,
            memory_bytes: 64 << 20,
            max_metadata_bytes: 1 << 20,
            ..crate::budget::Limits::default()
        })
        .unwrap();
        let definition = GroupDefinition {
            group: Id::new("computed").unwrap(),
            definition: "v1".into(),
            fields: Vec::new(),
        };
        let declaration = OriginalDeclaration {
            processor: "fixture".into(),
            definition: definition.clone(),
            input_fields: BTreeMap::new(),
        };
        let declaration = crate::engine::backfill::origins::write(
            &Id::new("owner").unwrap(),
            &root.path().join("declaration.json"),
            &declaration,
            &budget,
        )
        .unwrap();
        let mut source = TablePin {
            table: "source".into(),
            metadata: object("source-table.json"),
            logical_metadata: None,
            snapshot: Some(1),
            schema_id: 0,
            objects: vec![object("source-table.json"), object("source.parquet")],
            rows: 1,
        };
        let mut output = source.clone();
        output.table = "output".into();
        output.metadata = object("output-table.json");
        output.objects = vec![output.metadata.clone(), object("output.parquet")];
        source.logical_metadata = Some(object("logical-source.json"));
        let completion = FileCompletion {
            execution: Some(OriginalExecution {
                operation: Id::new("original-operation").unwrap(),
                declaration: declaration.clone(),
            }),
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output-file.json"),
            outputs: vec![object("output.parquet")],
        };
        let originals = ScalarOrigins {
            processor: "fixture".into(),
            definition,
            input_fields: BTreeMap::new(),
            dependencies: GroupDependencies {
                source: (&source).into(),
                source_logical_metadata: source.logical_metadata.clone(),
                output: (&output).into(),
            },
            files: vec![crate::engine::backfill::origins::FileOrigin {
                generation: RetainedGeneration {
                    key: "phase/backfill/file".into(),
                    revision: 1,
                },
                completion: completion.clone(),
            }],
        };
        let originals = crate::engine::backfill::origins::write(
            &Id::new("owner").unwrap(),
            &root.path().join("origins.json"),
            &originals,
            &budget,
        )
        .unwrap();
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        // Complete table inventories avoid opening native metadata or payloads.
        inventory.table(&source, &origin()).unwrap();
        inventory.table(&output, &origin()).unwrap();
        inventory
            .enqueue(&originals, Expansion::Origins, &origin())
            .unwrap();
        inventory
            .enqueue(&originals, Expansion::Origins, &origin())
            .unwrap();
        inventory
            .expand(&storage::Objects::Local, &budget, root.path())
            .await
            .unwrap();
        assert_eq!(inventory.metadata_objects_read, 2);
        assert!(
            !inventory
                .visited
                .contains_key("/unavailable/old-schema.json")
        );
        assert!(
            inventory
                .enqueue(&originals, Expansion::Declaration, &origin())
                .is_err()
        );
        let references = inventory.into_references().unwrap();
        for reference in [
            originals,
            declaration,
            completion.source,
            completion.source_metadata.unwrap(),
            completion.descriptor,
            completion.outputs[0].clone(),
            source.logical_metadata.unwrap(),
        ] {
            assert!(references.contains(&reference));
        }
    }

    #[test]
    fn legacy_execution_absence_and_null_remain_unknown_with_strict_decoding() {
        let phase = FileCompletion {
            execution: None,
            source: object("source.parquet"),
            source_metadata: Some(object("old-schema.json")),
            rows: 1,
            descriptor: object("output.json"),
            outputs: vec![object("output.parquet")],
        };
        let mut json = serde_json::to_value(phase).unwrap();
        let explicit_null: FileCompletion = decode(&json.to_string()).unwrap();
        assert!(explicit_null.execution.is_none());
        json.as_object_mut().unwrap().remove("execution");
        let absent: FileCompletion = decode(&json.to_string()).unwrap();
        assert!(absent.execution.is_none());
        json["unrecognized_origins"] = serde_json::json!({"declaration": object("unknown.json")});
        assert!(decode::<FileCompletion>(&json.to_string()).is_err());
    }
}

mod correction_tests {
    use super::*;

    #[tokio::test]
    async fn correction_registry_is_typed_object_free_and_revision_bound() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            root.path(),
            Limits {
                compute_threads: 1,
                memory_bytes: 64 << 20,
                scratch_bytes: 16 << 20,
                max_batch_bytes: 1 << 20,
                max_metadata_bytes: 1 << 20,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let correction = DependencyCorrection {
            dataset: Id::new("comments").unwrap(),
            id: Id::new("language-omission").unwrap(),
            definition: GroupDefinition {
                group: Id::new("classification").unwrap(),
                definition: "text-only-v1".into(),
                fields: vec![ComputedField {
                    id: 10,
                    name: "label".into(),
                    expression: "text".into(),
                }],
            },
            fields: BTreeSet::from([2]),
            reason: "language was material".into(),
        };
        let decision = engine.correct_dependencies(&correction).await.unwrap();
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&correction.dataset).await.unwrap();
        let rows = lifecycle.rows(&view).await.unwrap();
        for (key, revision, json) in rows.prefix("") {
            let origin = RetentionBlocker {
                table: view.identifier().to_string(),
                key: key.into(),
                revision,
            };
            assert!(
                reference_roots(&rows, key, json, &origin, 1 << 20)
                    .unwrap()
                    .is_empty()
            );
            let mut wrong_revision = origin;
            wrong_revision.revision += 1;
            assert!(reference_roots(&rows, key, json, &wrong_revision, 1 << 20).is_err());
        }
        // Restore follows these registry records without inventing physical objects.
        let inspection = engine.inspect_retention(&[]).await.unwrap();
        assert!(inspection.objects.is_empty());
        let (_, objects) = engine
            .restore_dependencies(&correction.dataset)
            .await
            .unwrap();
        assert!(
            objects
                .iter()
                .all(|object| object.owner == correction.dataset)
        );

        let mut unexpected = serde_json::to_value(&decision).unwrap();
        unexpected["unrecognized_dependency"] = serde_json::json!({"path": "/unknown"});
        assert!(decode::<CorrectionDecision>(&unexpected.to_string()).is_err());
        let key = format!("dependency-correction/{}", correction.id);
        let origin = RetentionBlocker {
            table: view.identifier().to_string(),
            key: key.clone(),
            revision: decision.revision,
        };
        let proof = DependencyQualification {
            checker: "docspec.scalar-field-correspondence.v1".into(),
            scope: crate::engine::corrections::records::scope_key(&correction.definition).unwrap(),
            fields: correction.fields.clone(),
            first_revision: decision.revision,
            origins: ObjectRef {
                owner: correction.dataset.clone(),
                path: "/unavailable/origins.json".into(),
                digest: "0".repeat(64),
                bytes: 17,
            },
            target: NativeSnapshotRef {
                metadata: ObjectRef {
                    owner: correction.dataset.clone(),
                    path: "/unavailable/target.json".into(),
                    digest: "0".repeat(64),
                    bytes: 17,
                },
                snapshot: Some(42),
                schema_id: 7,
            },
        };
        let proof_key = crate::engine::corrections::qualification_key(&proof).unwrap();
        let references = reference_roots(
            &rows,
            &proof_key,
            &serde_json::to_string(&proof).unwrap(),
            &origin,
            1 << 20,
        )
        .unwrap();
        assert!(references.contains(&proof.origins));
        assert!(references.contains(&proof.target.metadata));
        assert_eq!(references.len(), 2);
        assert!(
            reference_roots(
                &rows,
                "dependency-qualification/wrong-key",
                &serde_json::to_string(&proof).unwrap(),
                &origin,
                1 << 20,
            )
            .is_err()
        );
        let mut inventory = Inventory::collecting(1 << 20).unwrap();
        inventory.shallow = true;
        inventory.qualification(&proof, &origin).unwrap();
        assert_eq!(
            inventory.visited[&proof.target.metadata.path].binding,
            Some((proof.target.snapshot, proof.target.schema_id)),
        );
        assert!(inventory.pending.is_empty());
        assert!(inventory.pending_native.is_empty());
        assert!(
            reference_roots(
                &rows,
                "dependency-correction/different-id",
                &serde_json::to_string(&decision).unwrap(),
                &origin,
                1 << 20,
            )
            .is_err()
        );
    }

    #[test]
    fn qualification_recovery_can_retire_without_retiring_correction_authority() {
        assert!(super::super::retirement::completion_key(
            "dependency-qualification/proof"
        ));
        for key in [
            "dependency-correction/decision",
            "correction-requirements/scope",
        ] {
            assert!(!super::super::retirement::completion_key(key));
            assert!(!super::super::retirement::retained_key(key));
        }
    }
}

mod release_history_tests {
    use super::*;

    #[tokio::test]
    async fn full_release_upgrades_interpretation_history_in_either_order() {
        let root = tempfile::tempdir().unwrap();
        let owner = Id::new("history-owner").unwrap();
        let payload = ObjectRef {
            owner: owner.clone(),
            path: root
                .path()
                .join("old-input.parquet")
                .to_string_lossy()
                .into_owned(),
            digest: "0".repeat(64),
            bytes: 37,
        };
        let dataset = DatasetPin {
            format: FORMAT,
            dataset: owner.clone(),
            version: Id::new("old").unwrap(),
            groups: BTreeMap::new(),
            computed: BTreeMap::new(),
            inputs: BTreeMap::from([("input".into(), payload.clone())]),
            relationship: SourceRelationship::Independent,
            related: None,
            evidence: None,
            definition: "v1".into(),
            eligibility: 0,
            boundary: 1,
            request: OperationRequest::Build(RunSpec {
                dataset: owner.clone(),
                operation: Id::new("old").unwrap(),
                base: None,
                relationship: None,
                input: payload.path.clone(),
                expected_digest: payload.digest.clone(),
                sql: "SELECT * FROM input".into(),
                definition: "v1".into(),
                partition_by: Vec::new(),
            }),
            parent_manifest: None,
            processor: "fixture".into(),
            validation: VerificationLevel::Routine,
            partition_by: Vec::new(),
        };
        let bytes = serde_json::to_vec(&dataset).unwrap();
        let path = root.path().join("old.json");
        storage::write_new(&path, &bytes).unwrap();
        let manifest = ObjectRef {
            owner,
            path: path.to_string_lossy().into_owned(),
            digest: blake3::hash(&bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        };
        let origin = RetentionBlocker {
            table: "owner".into(),
            key: "result/old".into(),
            revision: 1,
        };
        let budget = Budget::new(crate::budget::Limits::default()).unwrap();
        for history_first in [true, false] {
            let mut inventory = Inventory::new(std::slice::from_ref(&payload), 1 << 20).unwrap();
            if history_first {
                inventory.history(&manifest, &origin).unwrap();
                inventory
                    .expand(&storage::Objects::Local, &budget, root.path())
                    .await
                    .unwrap();
                assert!(inventory.candidates[&payload.path].blocker.is_none());
            }
            inventory.release(&manifest, &origin).unwrap();
            inventory
                .expand(&storage::Objects::Local, &budget, root.path())
                .await
                .unwrap();
            inventory.history(&manifest, &origin).unwrap();
            inventory
                .expand(&storage::Objects::Local, &budget, root.path())
                .await
                .unwrap();
            assert!(inventory.candidates[&payload.path].blocker.is_some());
            assert!(inventory.visited[&manifest.path].release_payloads);
            assert_eq!(
                inventory.metadata_objects_read,
                if history_first { 2 } else { 1 }
            );
        }
        // Both strengths queued before decoding use only the stronger expansion.
        let mut inventory = Inventory::new(std::slice::from_ref(&payload), 1 << 20).unwrap();
        inventory.history(&manifest, &origin).unwrap();
        inventory.release(&manifest, &origin).unwrap();
        inventory
            .expand(&storage::Objects::Local, &budget, root.path())
            .await
            .unwrap();
        assert!(inventory.candidates[&payload.path].blocker.is_some());
        assert_eq!(inventory.metadata_objects_read, 1);
    }
}

mod scalar_readback_fields_tests {
    use super::*;

    #[test]
    fn private_scalar_validator_is_typed_only_on_scalar_readback() {
        let owner = Id::new("owner").unwrap();
        let object = ObjectRef {
            owner: owner.clone(),
            path: "/retained/input.parquet".into(),
            digest: "0".repeat(64),
            bytes: 17,
        };
        let release = ReleasePin {
            id: Id::new("initial").unwrap(),
            dataset: DatasetPin {
                format: FORMAT,
                dataset: owner.clone(),
                version: Id::new("initial").unwrap(),
                groups: BTreeMap::new(),
                computed: BTreeMap::new(),
                inputs: BTreeMap::new(),
                relationship: SourceRelationship::Independent,
                related: None,
                evidence: None,
                definition: "v1".into(),
                eligibility: 0,
                boundary: 1,
                request: OperationRequest::Build(RunSpec {
                    dataset: owner,
                    operation: Id::new("initial").unwrap(),
                    base: None,
                    relationship: None,
                    input: object.path.clone(),
                    expected_digest: object.digest.clone(),
                    sql: "SELECT * FROM input".into(),
                    definition: "v1".into(),
                    partition_by: Vec::new(),
                }),
                parent_manifest: None,
                processor: "fixture".into(),
                validation: VerificationLevel::Routine,
                partition_by: Vec::new(),
            },
            manifest: object,
            verified: true,
        };
        let legacy = serde_json::to_value(&release).unwrap();
        let legacy_decoded: Strict<ReadbackCompletion> =
            serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(legacy_decoded.0.release, release);
        let mut known = legacy.clone();
        known["scalar_validator"] = serde_json::json!("docspec.scalar-validation.v1");
        let completed: Strict<ReadbackCompletion> = serde_json::from_value(known.clone()).unwrap();
        assert_eq!(completed.0.release, release);
        assert!(
            serde_json::from_value::<Strict<ReleasePin>>(known.clone()).is_err(),
            "other release records do not accept the private field"
        );
        for top_level in [true, false] {
            let mut unknown = known.clone();
            if top_level {
                unknown["unqualified_dependency"] = serde_json::json!("/hidden.parquet");
            } else {
                unknown["dataset"]["unqualified_dependency"] = serde_json::json!("/hidden.parquet");
            }
            assert!(serde_json::from_value::<Strict<ReadbackCompletion>>(unknown).is_err());
        }
        known["scalar_validator"] = serde_json::json!(17);
        assert!(serde_json::from_value::<Strict<ReadbackCompletion>>(known).is_err());
        // Generic readback has its own private check identity. Public results and
        // scalar readbacks must not silently accept this dependency-bearing field.
        type Generic = crate::engine::validation::ValidatedReadback;
        let decoded: Strict<Generic> = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(decoded.0.release, release);
        let mut generic = legacy;
        generic["validator"] = serde_json::json!(crate::engine::validation::VALIDATOR);
        let decoded: Strict<Generic> = serde_json::from_value(generic.clone()).unwrap();
        assert_eq!(decoded.0.release, release);
        assert!(serde_json::from_value::<Strict<ReleasePin>>(generic.clone()).is_err());
        assert!(serde_json::from_value::<Strict<ReadbackCompletion>>(generic.clone()).is_err());
        for top_level in [true, false] {
            let mut unknown = generic.clone();
            if top_level {
                unknown["unqualified_dependency"] = serde_json::json!("/hidden.parquet");
            } else {
                unknown["dataset"]["unqualified_dependency"] = serde_json::json!("/hidden.parquet");
            }
            assert!(serde_json::from_value::<Strict<Generic>>(unknown).is_err());
        }
        generic["validator"] = serde_json::json!(17);
        assert!(serde_json::from_value::<Strict<Generic>>(generic).is_err());
    }
}
