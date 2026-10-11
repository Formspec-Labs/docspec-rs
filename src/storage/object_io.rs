//! Concrete create-only and admitted identity I/O for retained native objects.

use crate::{
    Error, Result,
    model::{Id, ObjectRef},
};
use iceberg::table::Table;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(crate) enum Objects {
    Local,
    #[cfg(feature = "remote")]
    Remote(super::remote::RemoteObjects),
}

impl Objects {
    pub(crate) fn register_query_store(
        &self,
        _runtime: &datafusion::execution::runtime_env::RuntimeEnv,
    ) -> Result<()> {
        match self {
            Self::Local => Ok(()),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => objects.register_query_store(_runtime),
        }
    }

    pub(crate) fn retained_prefix(&self) -> Option<String> {
        match self {
            Self::Local => None,
            #[cfg(feature = "remote")]
            Self::Remote(objects) => Some(objects.owned_location()),
        }
    }

    pub(crate) fn validate_retained_path(&self, path: &str) -> Result<()> {
        self.validate_path(path)
    }

    pub(crate) async fn admit_source(
        &self,
        expected: &ObjectRef,
        source: PathBuf,
        budget: &crate::budget::Budget,
        _resuming: bool,
    ) -> Result<ObjectRef> {
        match self {
            Self::Local => {
                let expected = expected.clone();
                budget
                    .blocking(move || {
                        super::copy_verified(
                            &expected.owner,
                            &source,
                            Path::new(&expected.path),
                            &expected.digest,
                        )
                    })
                    .await
            }
            #[cfg(feature = "remote")]
            Self::Remote(objects) => {
                if _resuming
                    && let Some(retained) = objects.verify_if_present(expected, budget).await?
                {
                    return Ok(retained);
                }
                // One create-only attempt uses the original protected URI. A
                // competing/unacknowledged writer may duplicate unfinished effort,
                // but conditional completion cannot overwrite immutable bytes.
                match objects.upload_file(expected, source, budget).await {
                    Ok(retained) => objects.verify(&retained, budget).await,
                    Err(error)
                        if matches!(error.code(), "remote_conflict" | "remote_unconfirmed") =>
                    {
                        match objects.verify_if_present(expected, budget).await? {
                            Some(retained) => Ok(retained),
                            None => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    // Explicitly compose owned native I/O; consume no arbitrary Table credentials.
    // Remote runtime lifetime begins here, on the currently driven runtime.
    pub(crate) fn bind_table(&self, table: &Table) -> Result<PinTable> {
        match self {
            Self::Local => Ok(PinTable::local(table)),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => PinTable::remote(
                table.identifier().clone(),
                table.metadata_location_result()?,
                table.metadata_ref(),
                table.readonly(),
                objects.clone(),
            ),
        }
    }

    pub(super) fn create_bytes_admitted(
        self,
        path: &str,
        bytes: Vec<u8>,
        _handle: &tokio::runtime::Handle,
    ) -> Result<()> {
        match self {
            Self::Local => super::write_new(Path::new(path), &bytes),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => _handle.block_on(objects.create_bytes_admitted(path, bytes)),
        }
    }

    // The caller retains the exact buffer and worker admission through parsing.
    // Remote transport adds a bounded stream allowance; local reads add none.
    pub(crate) fn metadata_read_allowance(&self) -> usize {
        match self {
            Self::Local => 0,
            #[cfg(feature = "remote")]
            Self::Remote(_) => super::remote::objects::READ_ADMISSION,
        }
    }

    pub(crate) fn read_metadata_admitted(
        &self,
        expected: &ObjectRef,
        bytes: &mut [u8],
        _handle: &tokio::runtime::Handle,
    ) -> Result<()> {
        let corrupt = || {
            Error::refusal(
                "corrupt_object",
                "input manifest differs from its declared bytes",
            )
        };
        if bytes.len() as u64 != expected.bytes {
            return Err(corrupt());
        }
        match self {
            Self::Local => {
                use std::io::Read;
                let mut file = std::fs::File::open(&expected.path)?;
                if file.metadata()?.len() != expected.bytes {
                    return Err(corrupt());
                }
                file.read_exact(bytes)?;
                if file.read(&mut [0; 1])? != 0 {
                    return Err(corrupt());
                }
            }
            #[cfg(feature = "remote")]
            Self::Remote(objects) => {
                _handle.block_on(objects.read_metadata_admitted(expected, bytes))?
            }
        }
        if blake3::hash(bytes).to_hex().as_str() != expected.digest {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(super) fn read_allowance(&self) -> usize {
        match self {
            // The existing local identity reader owns one MiB of hash buffers.
            Self::Local => 1 << 20,
            #[cfg(feature = "remote")]
            Self::Remote(_) => super::remote::objects::READ_ADMISSION,
        }
    }

    pub(super) fn validate_path(&self, _path: &str) -> Result<()> {
        match self {
            Self::Local => Ok(()),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => objects.validate_path(_path),
        }
    }

    pub(super) fn is_remote(&self) -> bool {
        match self {
            Self::Local => false,
            #[cfg(feature = "remote")]
            Self::Remote(_) => true,
        }
    }

    pub(super) fn pin_root(&self, location: &str) -> Result<PinRoot> {
        match self {
            Self::Local => Ok(PinRoot::Local(std::fs::canonicalize(location)?)),
            #[cfg(feature = "remote")]
            Self::Remote(_) => Ok(PinRoot::Remote(format!("{location}/"))),
        }
    }

    pub(crate) fn check_available_admitted(
        &self,
        expected: &ObjectRef,
        _handle: &tokio::runtime::Handle,
    ) -> Result<()> {
        match self {
            Self::Local => super::check_object_available(expected),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => _handle.block_on(objects.check_available_admitted(expected)),
        }
    }

    pub(super) fn identity_admitted(
        &self,
        owner: &Id,
        path: &str,
        _handle: &tokio::runtime::Handle,
    ) -> Result<ObjectRef> {
        match self {
            Self::Local => super::object(owner, Path::new(path)),
            #[cfg(feature = "remote")]
            Self::Remote(objects) => _handle.block_on(objects.identity_admitted(owner, path)),
        }
    }
}

// Table containment is narrower than configured remote read scope. It applies
// only to new references; inherited exact identities retain their original owner.
pub(super) enum PinRoot {
    Local(PathBuf),
    #[cfg(feature = "remote")]
    Remote(String),
}

impl PinRoot {
    pub(super) fn check_new_path(&self, path: &str) -> Result<()> {
        let contained = match self {
            Self::Local(root) => {
                std::fs::canonicalize(path)?.starts_with(root)
                    && !std::fs::symlink_metadata(path)?.file_type().is_symlink()
            }
            #[cfg(feature = "remote")]
            Self::Remote(prefix) => path
                .strip_prefix(prefix)
                .is_some_and(|suffix| !suffix.is_empty()),
        };
        if !contained {
            return Err(Error::refusal(
                "unprotected_reference",
                "native snapshot references an object without an inherited protection owner",
            ));
        }
        Ok(())
    }
}

// Native inventory and identities share this explicitly composed backend.
// No constructor infers credential equality from an arbitrary Table's FileIO.
pub(crate) struct PinTable {
    pub(super) table: Table,
    pub(super) objects: Objects,
}

impl PinTable {
    pub(crate) fn local(table: &Table) -> Self {
        Self {
            table: table.clone(),
            objects: Objects::Local,
        }
    }

    #[cfg(feature = "remote")]
    pub(crate) fn remote(
        identifier: iceberg::TableIdent,
        metadata_location: &str,
        metadata: iceberg::spec::TableMetadataRef,
        readonly: bool,
        objects: super::remote::RemoteObjects,
    ) -> Result<Self> {
        objects.validate_table_location(metadata.location())?;
        objects.validate_path(metadata_location)?;
        let prefix = format!("{}/metadata/", metadata.location());
        // A catalog may compress selected metadata independently of the table's
        // policy for future writes. Canonical URI syntax binds the location;
        // native/pinned readers decode the retained bytes by gzip magic.
        let parsed = metadata_location.parse::<iceberg::MetadataLocation>()?;
        if !metadata_location
            .strip_prefix(&prefix)
            .is_some_and(|filename| !filename.is_empty() && !filename.contains('/'))
            || parsed.to_string() != metadata_location
        {
            return Err(Error::refusal(
                "metadata_location",
                "owned candidate metadata must name canonical native metadata beneath its table location",
            ));
        }
        // Native Table does not expose its old runtime. This constructor is an
        // explicit composition/lifetime boundary using the currently driven runtime.
        let table = Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(identifier)
            .readonly(readonly)
            .file_io(objects.file_io())
            .runtime(iceberg::Runtime::try_current()?)
            .build()?;
        Ok(Self {
            table,
            objects: Objects::Remote(objects),
        })
    }

    #[cfg(feature = "remote")]
    pub(super) fn into_native(self) -> Table {
        self.table
    }
}

// Borrowed only during the existing native inventory walk. No second inventory,
// registry, scan or alternate decoder; known paths retain captured protection owners.
pub(super) struct PinPaths<'a> {
    pub objects: &'a Objects,
    pub root: &'a PinRoot,
    pub inherited: &'a std::collections::BTreeMap<String, ObjectRef>,
}

impl PinPaths<'_> {
    pub(super) fn check(&self, path: &str) -> Result<()> {
        self.objects.validate_path(path)?;
        if !self.inherited.contains_key(path) {
            self.root.check_new_path(path)?;
        }
        Ok(())
    }
}

// Rust guideline compliant 2026-02-21

// Syntax alone does not grant ownership or transport scope. Callers separately
// check the configured Objects and exact lifecycle reservation/owner bindings.
pub(crate) fn valid_object_locator(path: &str) -> bool {
    let normalized: PathBuf = Path::new(path).components().collect();
    if Path::new(path).is_absolute()
        && normalized.to_str() == Some(path)
        && !Path::new(path)
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return true;
    }
    #[cfg(feature = "remote")]
    {
        super::remote::RemoteObjects::validate_uri(path).is_ok()
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = path;
        false
    }
}
