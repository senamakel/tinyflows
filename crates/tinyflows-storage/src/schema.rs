//! TinyFlows-owned collection declarations and resumable owner migrations.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_api::{DocumentStore, DocumentStoreExt};
use tinystoragedrivers_types::{
    CollectionSpec, ErrorKind, Filter, ID_FIELD, Meta, Precondition, Query, Result, Sort,
    StorageError, WriteOptions,
};

use crate::Key;

const PAGE_SIZE: usize = 100;
const MIGRATIONS_COLLECTION: &str = "tsd.migrations";

/// Which product scope owns a declared collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeClass {
    /// Tenant or local user data.
    Tenant,
}

/// A validated TinyFlows storage owner name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Owner(String);

impl Owner {
    /// Validate an owner name.
    pub fn new(name: &str) -> Result<Self> {
        let valid = (2..=24).contains(&name.len())
            && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !valid || name == "tsd" {
            return Err(StorageError::invalid_input("invalid storage owner name"));
        }
        Ok(Self(name.to_owned()))
    }

    /// Owner name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A source document passed to a migration step.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord {
    /// Existing source id.
    pub id: String,
    /// Existing document body.
    pub body: Value,
    /// Existing storage metadata.
    pub meta: Meta,
}

/// A document emitted by a migration step.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetRecord {
    /// Destination collection.
    pub collection: String,
    /// Destination id.
    pub id: String,
    /// Destination body.
    pub body: Value,
    /// Creation timestamp to preserve where available.
    pub created_ms: Option<i64>,
}

impl TargetRecord {
    /// Create a migrated document.
    #[must_use]
    pub fn new(collection: impl Into<String>, id: impl Into<String>, body: Value) -> Self {
        Self {
            collection: collection.into(),
            id: id.into(),
            body,
            created_ms: None,
        }
    }

    /// Preserve the source creation timestamp.
    #[must_use]
    pub fn created_ms(mut self, value: i64) -> Self {
        self.created_ms = Some(value);
        self
    }
}

type StepFn = dyn Fn(Vec<SourceRecord>) -> Result<Vec<TargetRecord>> + Send + Sync;

/// One document-collection migration step.
#[derive(Clone)]
pub struct Step {
    /// Stable step id.
    pub id: &'static str,
    /// Destination collection.
    pub item: &'static str,
    /// Source schema version.
    pub from: u32,
    /// Destination schema version.
    pub to: u32,
    /// Legacy source collection, when different from `item`.
    pub source: Option<&'static str>,
    run: Arc<StepFn>,
}

impl Step {
    /// Define a document migration step.
    #[must_use]
    pub fn new(
        id: &'static str,
        item: &'static str,
        from: u32,
        to: u32,
        run: impl Fn(Vec<SourceRecord>) -> Result<Vec<TargetRecord>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            id,
            item,
            from,
            to,
            source: None,
            run: Arc::new(run),
        }
    }

    /// Read from a legacy collection rather than the destination.
    #[must_use]
    pub fn source(mut self, name: &'static str) -> Self {
        self.source = Some(name);
        self
    }
}

/// One owner’s collections and migrations, opened by that owner.
#[derive(Clone)]
pub struct Schema {
    /// Storage owner for these collections.
    pub owner: Owner,
    /// Declared collections and versions.
    pub items: Vec<(CollectionSpec, u32, ScopeClass)>,
    /// Ordered migrations.
    pub steps: Vec<Step>,
}

impl Schema {
    /// Start a declaration for `owner`.
    #[must_use]
    pub fn new(owner: Owner) -> Self {
        Self {
            owner,
            items: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Declare a collection and schema version.
    #[must_use]
    pub fn collection(self, name: &str, version: u32, scope: ScopeClass) -> Self {
        self.collection_with(version, CollectionSpec::new(name), scope)
    }

    /// Declare a fully specified collection and schema version.
    #[must_use]
    pub fn collection_with(
        mut self,
        version: u32,
        spec: CollectionSpec,
        scope: ScopeClass,
    ) -> Self {
        self.items.push((spec, version, scope));
        self
    }

    /// Add an ordered collection migration.
    #[must_use]
    pub fn step(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    /// Pure collection declarations for host bootstrap and documentation.
    #[must_use]
    pub fn collections(&self) -> Vec<CollectionSpec> {
        let mut specs = self
            .items
            .iter()
            .map(|(spec, _, _)| spec.clone())
            .collect::<Vec<_>>();
        specs.push(CollectionSpec::new(MIGRATIONS_COLLECTION));
        specs
    }

    /// Ensure this owner’s collections, then migrate its data in place.
    ///
    /// The shared `tsd.migrations` collection records one progress row per
    /// owner, item and step. Backends are supplied by the caller; this method
    /// never constructs an engine or an in-memory fallback.
    pub async fn migrate(
        &self,
        docs: &Arc<dyn DocumentStore>,
        options: &MigrateOptions,
    ) -> Result<MigrationReport> {
        if options.batch_size == 0 {
            return Err(StorageError::invalid_input(
                "migration batch size must be at least 1",
            ));
        }
        for spec in self.collections() {
            docs.ensure_collection(&spec).await?;
        }
        let migrations = Migrations {
            docs: Arc::clone(docs),
        };
        let mut report = MigrationReport::default();
        for step in &self.steps {
            let progress_key = Key::of([self.owner.as_str(), step.item, step.id])?
                .as_str()
                .to_owned();
            let progress = migrations.load(&progress_key).await?;
            if progress.as_ref().is_some_and(|p| p.done) {
                report.already_done += 1;
                continue;
            }
            let source = step.source.unwrap_or(step.item);
            if source != step.item {
                docs.ensure_collection(&CollectionSpec::new(source)).await?;
            }
            let mut query = Query::filter(Filter::default())
                .sort(Sort::asc(ID_FIELD))
                .limit(options.batch_size);
            if let Some(cursor) = progress.as_ref().and_then(|p| p.cursor.clone()) {
                query = query.after(tinystoragedrivers_types::Cursor(cursor));
            }
            let mut written = progress.as_ref().map_or(0, |saved| saved.written);
            loop {
                let page = docs.query(source, &query).await?;
                if page.items.is_empty() {
                    break;
                }
                let last_cursor = page.next.clone();
                let inputs = page
                    .items
                    .iter()
                    .filter(|row| source != step.item || row.meta.schema < step.to)
                    .map(|row| SourceRecord {
                        id: row.id.clone(),
                        body: row.doc.clone(),
                        meta: row.meta.clone(),
                    })
                    .collect();
                let outputs = (step.run)(inputs)?;
                let output_ids: std::collections::HashSet<(String, String)> = outputs
                    .iter()
                    .map(|r| (r.collection.clone(), r.id.clone()))
                    .collect();
                for target in outputs {
                    if !target.body.is_object() {
                        return Err(StorageError::invalid_input(
                            "migration output must be an object",
                        ));
                    }
                    let output_schema = self.target_schema(step, &target)?;
                    let source_row = (source == step.item && target.collection == step.item)
                        .then(|| page.items.iter().find(|row| row.id == target.id));
                    let source_row = source_row.flatten();
                    let precondition = source_row.map_or(Precondition::Absent, |row| {
                        Precondition::Version(row.version)
                    });
                    let created_ms = target
                        .created_ms
                        .or_else(|| source_row.map(|row| row.meta.created_ms));
                    let mut options = WriteOptions::new();
                    options = options.schema(output_schema);
                    if let Some(created_ms) = created_ms {
                        options = options.created_ms(created_ms);
                    }
                    if let Some(expires_ms) = source_row.and_then(|row| row.meta.expires_ms) {
                        options = options.expires_ms(expires_ms);
                    }
                    match docs
                        .put_with(
                            &target.collection,
                            &target.id,
                            target.body.clone(),
                            precondition,
                            options,
                        )
                        .await
                    {
                        Ok(_) => written += 1,
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::Conflict | ErrorKind::AlreadyExists
                            ) =>
                        {
                            let existing = docs.get(&target.collection, &target.id).await?;
                            let expected_expiry =
                                source_row.and_then(|source| source.meta.expires_ms);
                            let identical = existing.is_some_and(|row| {
                                row.doc == target.body
                                    && row.meta.schema == output_schema
                                    && created_ms
                                        .is_none_or(|created| row.meta.created_ms == created)
                                    && row.meta.expires_ms == expected_expiry
                            });
                            if !identical {
                                return Err(error);
                            }
                            written += 1;
                        }
                        Err(error) => return Err(error),
                    }
                }
                for row in &page.items {
                    if (source != step.item || row.meta.schema < step.to)
                        && (source != step.item
                            || !output_ids.contains(&(step.item.to_owned(), row.id.clone())))
                    {
                        match docs
                            .delete(source, &row.id, Precondition::Version(row.version))
                            .await
                        {
                            Err(error)
                                if error.kind() == ErrorKind::Conflict
                                    && docs.get(source, &row.id).await?.is_none() => {}
                            other => {
                                other?;
                            }
                        }
                    }
                }
                let cursor = last_cursor.map(|cursor| cursor.0);
                migrations
                    .save(
                        &progress_key,
                        &Progress {
                            owner: self.owner.as_str().to_owned(),
                            step: step.id.to_owned(),
                            from: step.from,
                            to: step.to,
                            cursor,
                            written,
                            done: false,
                        },
                    )
                    .await?;
                if let Some(next) = page.next {
                    query = query.after(next);
                } else {
                    break;
                }
            }
            migrations
                .save(
                    &progress_key,
                    &Progress {
                        owner: self.owner.as_str().to_owned(),
                        step: step.id.to_owned(),
                        from: step.from,
                        to: step.to,
                        cursor: None,
                        written,
                        done: true,
                    },
                )
                .await?;
            report.ran.push(StepReport {
                owner: self.owner.as_str().to_owned(),
                item: step.item.to_owned(),
                step: step.id.to_owned(),
                written,
            });
        }
        Ok(report)
    }

    fn target_schema(&self, step: &Step, target: &TargetRecord) -> Result<u32> {
        let owned = target
            .collection
            .strip_prefix(self.owner.as_str())
            .is_some_and(|rest| rest.starts_with('.'));
        let version = self
            .items
            .iter()
            .find(|(spec, _, _)| spec.name == target.collection)
            .map(|(_, version, _)| *version)
            .filter(|_| owned)
            .ok_or_else(|| {
                StorageError::invalid_input(
                    "migration output targets an undeclared owner collection",
                )
            })?;
        Ok(if target.collection == step.item {
            step.to
        } else {
            version
        })
    }
}

/// Batch size for owner migration runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateOptions {
    batch_size: usize,
}
impl MigrateOptions {
    /// Defaults to 100 documents per batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            batch_size: PAGE_SIZE,
        }
    }
    /// Set the number of source documents read per batch.
    #[must_use]
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}
impl Default for MigrateOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of an owner migration run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Steps completed by this invocation.
    pub ran: Vec<StepReport>,
    /// Steps already marked complete.
    pub already_done: usize,
}

/// A completed migration step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepReport {
    /// Owner name.
    pub owner: String,
    /// Collection being migrated.
    pub item: String,
    /// Stable step identifier.
    pub step: String,
    /// Number of destination records written.
    pub written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Progress {
    owner: String,
    step: String,
    from: u32,
    to: u32,
    cursor: Option<String>,
    written: u64,
    done: bool,
}

/// Per-owner migration completion markers.
#[derive(Debug, Clone)]
pub struct Migrations {
    docs: Arc<dyn DocumentStore>,
}
impl Migrations {
    /// Bind markers to the caller-provided document store and owner.
    #[must_use]
    pub fn new(docs: &Arc<dyn DocumentStore>, _owner: &str) -> Self {
        Self {
            docs: Arc::clone(docs),
        }
    }

    /// Whether a step has completed.
    pub async fn done(&self, owner: &str, step: &str) -> Result<bool> {
        self.ensure().await?;
        let id = Key::of([owner, step])?.as_str().to_owned();
        Ok(self
            .docs
            .get_as::<Progress>(MIGRATIONS_COLLECTION, &id)
            .await?
            .is_some_and(|row| row.doc.done))
    }

    /// Mark an external import step complete.
    pub async fn mark_done(&self, owner: &str, step: &str) -> Result<()> {
        self.ensure().await?;
        let id = Key::of([owner, step])?.as_str().to_owned();
        self.save(
            &id,
            &Progress {
                owner: owner.to_owned(),
                step: step.to_owned(),
                from: 0,
                to: 0,
                cursor: None,
                written: 0,
                done: true,
            },
        )
        .await
    }

    async fn load(&self, id: &str) -> Result<Option<Progress>> {
        Ok(self
            .docs
            .get_as::<Progress>(MIGRATIONS_COLLECTION, id)
            .await?
            .map(|row| row.doc))
    }

    async fn ensure(&self) -> Result<()> {
        self.docs
            .ensure_collection(&CollectionSpec::new(MIGRATIONS_COLLECTION))
            .await
    }

    async fn save(&self, id: &str, progress: &Progress) -> Result<()> {
        self.docs
            .put_as(MIGRATIONS_COLLECTION, id, progress, Precondition::None)
            .await
            .map(|_| ())
    }
}
