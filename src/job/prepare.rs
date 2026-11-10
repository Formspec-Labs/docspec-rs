//! Infer a complete job from declared schemas without opening its data sources.
// Rust guideline compliant 2026-02-21
use super::{Job, JobInput, JobSpec, JobStep, plan::SchemaOnlyTable};
use crate::{
    Error, Result,
    budget::{Budget, Limits},
};
use arrow::{
    datatypes::Schema,
    ipc::{convert::try_schema_from_ipc_buffer, writer::StreamWriter},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::OwnedSemaphorePermit;

/// Exact captured native artifact bytes, distinct from any input data identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobArtifact {
    pub path: PathBuf,
    pub sha256: String,
}

/// Maintained native formats accepted for a declared schema artifact.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaArtifactFormat {
    ArrowSerdeJson,
    IcebergSchemaJson,
}

/// Exact declaration bytes and their explicit native schema format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaArtifact {
    pub path: PathBuf,
    pub sha256: String,
    pub format: SchemaArtifactFormat,
}

/// A named schema using standard IPC or an exact native Arrow-serde artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredInputSchema {
    pub name: String,
    /// Base64 of one encapsulated IPC schema message, without record batches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ipc_base64: Option<String>,
    /// Existing native Arrow Schema JSON, verified before maintained deserialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_artifact: Option<SchemaArtifact>,
}

/// Schema preparation of one unchanged job using explicit named input declarations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareJobSpec {
    pub job: JobArtifact,
    pub inputs: Vec<DeclaredInputSchema>,
}

/// One exact native name and its inferred or declared Arrow schema identity.
#[derive(Debug, Serialize)]
pub struct PreparedRelation {
    pub name: String,
    pub schema_sha256: String,
}

/// A declared input schema and the relation schema exposed by its native provider.
#[derive(Debug, Serialize)]
pub struct PreparedInput {
    pub name: String,
    pub declared_schema_sha256: String,
    pub declared_schema_format: &'static str,
    pub declared_schema_size: usize,
    /// Declared native schema ID, when present; this is not an observed table schema.
    pub declared_native_schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_schema_artifact: Option<SchemaArtifact>,
    pub schema_sha256: String,
}

/// Complete schema inventory; no input admission or row execution is implied.
///
/// `schemas` contains each distinct schema message once. Python consumers decode
/// its base64 value with `pyarrow.ipc.read_schema(pyarrow.BufferReader(bytes))`.
/// Declared input digests identify supplied bytes. Input relation schemas include
/// native provider additions, such as JSON Lines source columns. Output digests identify returned
/// schema bytes. Neither identifies native data or proves observed input types.
#[derive(Debug, Serialize)]
pub struct PreparedJob {
    pub profile: &'static str,
    pub status: &'static str,
    pub job: JobArtifact,
    pub job_size: usize,
    pub inputs: Vec<PreparedInput>,
    pub steps: Vec<PreparedRelation>,
    pub outputs: Vec<PreparedRelation>,
    pub queries: Vec<PreparedRelation>,
    pub schema_format: &'static str,
    pub schemas: BTreeMap<String, String>,
    pub input_admission: &'static str,
    pub execution: &'static str,
    #[serde(skip)]
    _memory: Vec<OwnedSemaphorePermit>,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "job preparation exceeds its metadata allowance",
    )
}

// The wire bytes, decoded metadata, logical plans and schema encoding coexist.
// Match the existing saved-plan decode allowance rather than admitting wire only.
const DECODE_ALLOWANCE: usize = 32;

fn reserve(budget: &Budget, bytes: usize) -> Result<OwnedSemaphorePermit> {
    if bytes > budget.limits.max_metadata_bytes {
        return Err(capacity());
    }
    budget.reserve(bytes.checked_mul(DECODE_ALLOWANCE).ok_or_else(capacity)?)
}

fn read_artifact(
    binding: &JobArtifact,
    budget: &Budget,
    code: &'static str,
) -> Result<(Vec<u8>, OwnedSemaphorePermit)> {
    let file = File::open(&binding.path)?;
    let size = usize::try_from(file.metadata()?.len()).map_err(|_| capacity())?;
    let memory = reserve(budget, size)?;
    let mut bytes = Vec::with_capacity(size);
    file.take(size as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() != size || sha256(&bytes) != binding.sha256 {
        return Err(Error::refusal(
            code,
            "captured native artifact size or SHA-256 differs",
        ));
    }
    Ok((bytes, memory))
}

fn decode_schema(bytes: &[u8]) -> Result<Schema> {
    // Arrow owns the type decoder. Check only encapsulated-message framing so a
    // schema declaration cannot hide trailing dictionaries or record batches.
    let prefix = if bytes.starts_with(&[255; 4]) { 8 } else { 4 };
    let length = bytes
        .get(prefix - 4..prefix)
        .ok_or_else(|| Error::refusal("invalid_schema", "missing IPC schema length"))?;
    let length = i32::from_le_bytes(length.try_into().expect("four-byte IPC length"));
    if length < 0 || prefix + length as usize != bytes.len() {
        return Err(Error::refusal(
            "invalid_schema",
            "expected exactly one IPC schema message",
        ));
    }
    Ok(try_schema_from_ipc_buffer(bytes)?)
}

struct SchemaBuffer<'a> {
    bytes: Vec<u8>,
    budget: &'a Budget,
    memory: OwnedSemaphorePermit,
}
impl Write for SchemaBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other(capacity()))?;
        if size > self.budget.limits.max_metadata_bytes {
            return Err(std::io::Error::other(capacity()));
        }
        let allowance = size
            .checked_mul(DECODE_ALLOWANCE)
            .ok_or_else(|| std::io::Error::other(capacity()))?;
        let current = self.memory.num_permits();
        self.memory.merge(
            self.budget
                .reserve(allowance - current)
                .map_err(std::io::Error::other)?,
        );
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PreparedJob {
    fn retain_schema(&mut self, budget: &Budget, bytes: &[u8]) -> Result<String> {
        super::retained::retain_schema_into(budget, &mut self.schemas, &mut self._memory, bytes)
    }

    fn infer(&mut self, budget: &Budget, name: &str, schema: &Schema) -> Result<PreparedRelation> {
        let mut buffer = SchemaBuffer {
            bytes: Vec::new(),
            budget,
            memory: budget.reserve(0)?,
        };
        // Construction writes only an encapsulated schema message. No batches,
        // stream terminator or file writer are needed for `ipc.read_schema`.
        drop(StreamWriter::try_new(&mut buffer, schema)?);
        let digest = self.retain_schema(budget, &buffer.bytes)?;
        Ok(PreparedRelation {
            name: name.into(),
            schema_sha256: digest,
        })
    }
}

impl PrepareJobSpec {
    /// Infers all job schemas using declarations and native planning rules.
    ///
    /// No input path, raw pin, output destination or catalog is opened. SQL
    /// materializations and graph steps become schema-only relations. Table
    /// operations are outside this profile, including catalog reads.
    ///
    /// # Errors
    /// Refuses changed job bytes, missing/ambiguous declarations, invalid native
    /// names, lifetimes, SQL, functions, parameters, exports, graph types,
    /// unsupported facets and metadata exceeding the host's limits.
    pub async fn prepare(self, limits: Limits) -> Result<PreparedJob> {
        let job = Job::from_budget(Budget::new(limits)?)?;
        let declaration_size = self.inputs.iter().try_fold(0usize, |size, input| {
            let artifact_descriptor = input.schema_artifact.as_ref().map_or(0, |binding| {
                binding
                    .path
                    .as_os_str()
                    .len()
                    .saturating_add(binding.sha256.len())
            });
            size.checked_add(input.name.len())
                .and_then(|size| {
                    size.checked_add(input.schema_ipc_base64.as_ref().map_or(0, String::len))
                })
                .and_then(|size| size.checked_add(artifact_descriptor))
                .ok_or_else(capacity)
        })?;
        let declaration_memory = reserve(&job.budget, declaration_size)?;
        let (bytes, artifact_memory) = read_artifact(&self.job, &job.budget, "job_digest")?;
        let size = bytes.len();
        let spec: JobSpec = serde_json::from_slice(&bytes)?;
        spec.validate()?;
        if !spec.table_operations.is_empty() {
            return Err(Error::refusal(
                "unsupported_preparation",
                "schema-only preparation refuses table operations",
            ));
        }
        let mut prepared = PreparedJob {
            profile: "urn:docspec:job:schema-only:1",
            status: "inferred",
            job: self.job,
            job_size: size,
            inputs: Vec::new(),
            steps: Vec::new(),
            outputs: Vec::new(),
            queries: Vec::new(),
            schema_format: "arrow-ipc-schema",
            schemas: BTreeMap::new(),
            input_admission: "not_run",
            execution: "not_run",
            _memory: vec![artifact_memory, declaration_memory],
        };
        let mut declarations = BTreeMap::new();
        for input in self.inputs {
            if declarations.insert(input.name.clone(), input).is_some() {
                return Err(Error::refusal(
                    "invalid_spec",
                    "duplicate input schema declaration",
                ));
            }
        }
        if declarations.len() != spec.inputs.len() {
            return Err(Error::refusal(
                "invalid_spec",
                "input schemas must match every native input name exactly",
            ));
        }
        for input in &spec.inputs {
            let declaration = declarations.remove(input.name()).ok_or_else(|| {
                Error::refusal(
                    "invalid_spec",
                    "input schemas must match every native input name exactly",
                )
            })?;
            let (
                schema,
                declared_schema_sha256,
                declared_schema_format,
                declared_schema_size,
                declared_schema_artifact,
                declared_ipc,
                declared_native_schema_id,
            ) = match (declaration.schema_ipc_base64, declaration.schema_artifact) {
                (Some(encoded), None) => {
                    let bytes = STANDARD
                        .decode(encoded)
                        .map_err(|error| Error::refusal("invalid_schema", error.to_string()))?;
                    let schema = decode_schema(&bytes)?;
                    let digest = prepared.retain_schema(&job.budget, &bytes)?;
                    (
                        schema,
                        digest,
                        "arrow-ipc-schema",
                        bytes.len(),
                        None,
                        true,
                        None,
                    )
                }
                (None, Some(binding)) => {
                    let native_binding = JobArtifact {
                        path: binding.path.clone(),
                        sha256: binding.sha256.clone(),
                    };
                    let (bytes, memory) =
                        read_artifact(&native_binding, &job.budget, "schema_digest")?;
                    let (schema, format, native_schema_id) = match binding.format {
                        SchemaArtifactFormat::ArrowSerdeJson => (
                            serde_json::from_slice::<Schema>(&bytes)?,
                            "arrow_serde_json",
                            None,
                        ),
                        SchemaArtifactFormat::IcebergSchemaJson => {
                            let native = serde_json::from_slice::<iceberg::spec::Schema>(&bytes)?;
                            (
                                iceberg::arrow::schema_to_arrow_schema(&native)?,
                                "iceberg_schema_json",
                                Some(native.schema_id()),
                            )
                        }
                    };
                    prepared._memory.push(memory);
                    (
                        schema,
                        binding.sha256.clone(),
                        format,
                        bytes.len(),
                        Some(binding),
                        false,
                        native_schema_id,
                    )
                }
                _ => {
                    return Err(Error::refusal(
                        "invalid_schema",
                        "supply exactly one IPC schema or native schema artifact",
                    ));
                }
            };
            let pinned_schema_id = match input {
                JobInput::NativeTable { schema_id, .. } => Some(*schema_id),
                JobInput::Iceberg { pin, .. } => Some(pin.schema_id),
                _ => None,
            };
            if let (Some(declared), Some(pinned)) = (declared_native_schema_id, pinned_schema_id)
                && declared != pinned
            {
                return Err(Error::refusal(
                    "invalid_schema",
                    "declared Iceberg schema ID differs from the job input pin",
                ));
            }
            if let JobInput::JsonLines {
                schema: declared, ..
            } = input
                && declared != &schema
            {
                return Err(Error::refusal(
                    "invalid_schema",
                    "JSON Lines schema differs from the job declaration",
                ));
            }
            let (schema, schema_sha256) = if matches!(input, JobInput::JsonLines { .. }) {
                let schema = super::json_lines::describe(&schema)?;
                let described = prepared.infer(&job.budget, input.name(), &schema)?;
                (schema, described.schema_sha256)
            } else if declared_ipc {
                (schema, declared_schema_sha256.clone())
            } else {
                let described = prepared.infer(&job.budget, input.name(), &schema)?;
                (schema, described.schema_sha256)
            };
            job.context
                .register_table(input.name(), Arc::new(SchemaOnlyTable(Arc::new(schema))))?;
            prepared.inputs.push(PreparedInput {
                name: input.name().into(),
                declared_schema_sha256,
                declared_schema_format,
                declared_schema_size,
                declared_native_schema_id,
                declared_schema_artifact,
                schema_sha256,
            });
        }
        #[cfg(feature = "substrait")]
        let mut saved_plan_memory = Vec::new();
        for step in &spec.steps {
            let schema = match step {
                JobStep::Sql(step) => {
                    if step.materialize && !step.parameters.is_empty() {
                        return Err(Error::refusal(
                            "unsupported_parameters",
                            "parameters are supported for view steps",
                        ));
                    }
                    let frame = job.sql_bound(&step.sql, &step.parameters).await?;
                    if step.materialize {
                        let schema = crate::export::portable_schema(&super::plan::inferred_schema(
                            &frame,
                            &job.budget,
                        )?);
                        job.context.register_table(
                            &step.name,
                            Arc::new(SchemaOnlyTable(schema.clone())),
                        )?;
                        schema.as_ref().clone()
                    } else {
                        let schema = super::plan::inferred_schema(&frame, &job.budget)?;
                        job.register_frame(&step.name, frame)?;
                        schema
                    }
                }
                #[cfg(feature = "substrait")]
                JobStep::Substrait(step) => {
                    let (frame, _, memory) = job.read_substrait(&step.substrait).await?;
                    let schema = super::plan::inferred_schema(&frame, &job.budget)?;
                    job.register_frame(&step.name, frame)?;
                    saved_plan_memory.push(memory);
                    schema
                }
                JobStep::Components(step) => {
                    let schema =
                        super::components::describe(&job, &step.name, &step.components).await?;
                    job.context.register_table(
                        &step.name,
                        Arc::new(SchemaOnlyTable(Arc::new(schema.clone()))),
                    )?;
                    schema
                }
            };
            let relation = prepared.infer(&job.budget, step.name(), &schema)?;
            prepared.steps.push(relation);
            for name in step.release_after() {
                job.release(name)?;
            }
        }
        for output in &spec.outputs {
            let export =
                super::export::prepare(job.sql(&output.sql).await?, &output.settings, &job.budget)?;
            if export.metadata_unit
                > output
                    .settings
                    .footer_limit
                    .saturating_mul(8)
                    .min(job.budget.limits.max_metadata_bytes)
            {
                return Err(capacity());
            }
            let relation = prepared.infer(&job.budget, &output.name, &export.schema)?;
            job.register_declared_output(&output.name, export.schema.clone())?;
            prepared.outputs.push(relation);
        }
        for query in &spec.queries {
            let frame = job.sql(&query.sql).await?;
            let schema = super::plan::inferred_schema(&frame, &job.budget)?;
            let relation = prepared.infer(&job.budget, &query.name, &schema)?;
            prepared.queries.push(relation);
        }
        Ok(prepared)
    }
}
