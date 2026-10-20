//! TinyFlows-owned collection declarations and resumable owner migrations.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_api::{DocumentStore, DocumentStoreExt};
use tinystoragedrivers_types::{
    CollectionSpec, ErrorKind, Filter, Meta, Precondition, Query, Result, Sort, StorageError,
    WriteOptions, SCHEMA_FIELD,
};

use crate::Key;

const PAGE_SIZE: usize = 100;

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
            && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !valid || name == "tsd" {
            return Err(StorageError::invalid_input("invalid storage owner name"));
        }
        Ok(Self(name.to_owned()))
    }

    /// Owner name.
    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
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
        Self { collection: collection.into(), id: id.into(), body, created_ms: None }
    }

    /// Preserve the source creation timestamp.
    #[must_use]
    pub fn created_ms(mut self, value: i64) -> Self { self.created_ms = Some(value); self }
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
        Self { id, item, from, to, source: None, run: Arc::new(run) }
    }

    /// Read from a legacy collection rather than the destination.
    #[must_use]
    pub fn source(mut self, name: &'static str) -> Self { self.source = Some(name); self }
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
    pub fn new(owner: Owner) -> Self { Self { owner, items: Vec::new(), steps: Vec::new() } }

    /// Declare a collection and schema version.
    #[must_use]
    pub fn collection(self, name: &str, version: u32, scope: ScopeClass) -> Self {
        self.collection_with(version, CollectionSpec::new(name), scope)
    }

    /// Declare a fully specified collection and schema version.
    #[must_use]
    pub fn collection_with(mut self, version: u32, spec: CollectionSpec, scope: ScopeClass) -> Self {
        self.items.push((spec, version, scope));
        self
    }

    /// Add an ordered collection migration.
    #[must_use]
    pub fn step(mut self, step: Step) -> Self { self.steps.push(step); self }

    /// Pure collection declarations for host bootstrap and documentation.
    #[must_use]
    pub fn collections(&self) -> Vec<CollectionSpec> {
        let mut specs = self.items.iter().map(|(spec, _, _)| spec.clone()).collect::<Vec<_>>();
        specs.push(CollectionSpec::new(self.migrations_collection()));
        specs
    }

    fn migrations_collection(&self) -> String { format!("{}.migrations", self.owner.as_str()) }

    /// Ensure this owner’s collections, then migrate its data in place.
    ///
    /// A separate `<owner>.migrations` collection records one progress row per
    /// step. Backends are supplied by the caller; this method never constructs
    /// an engine or an in-memory fallback.
    pub async fn migrate(&self, docs: &Arc<dyn DocumentStore>, options: &MigrateOptions) -> Result<MigrationReport> {
        if options.batch_size == 0 { return Err(StorageError::invalid_input("migration batch size must be at least 1")); }
        for spec in self.collections() { docs.ensure_collection(&spec).await?; }
        let migrations = Migrations { docs: Arc::clone(docs), collection: self.migrations_collection() };
        let mut report = MigrationReport::default();
        for step in &self.steps {
            if migrations.done(self.owner.as_str(), step.id).await? { report.already_done += 1; continue; }
            let progress_id = Key::of([self.owner.as_str(), step.id])?.as_str().to_owned();
            let progress = migrations.docs.get_as::<Progress>(&migrations.collection, &progress_id).await?;
            let mut query = Query::filter(Filter::default())
                .sort(Sort::asc("_id"))
                .limit(options.batch_size);
            if let Some(cursor) = progress.and_then(|p| p.doc.cursor) {
                query = query.after(tinystoragedrivers_types::Cursor(cursor));
            }
            let source = step.source.unwrap_or(step.item);
            let mut written = 0_u64;
            loop {
                let page = docs.query(source, &query).await?;
                if page.items.is_empty() { break; }
                let last_cursor = page.next.clone();
                let inputs = page.items.iter()
                    .filter(|row| row.meta.schema < step.to)
                    .map(|row| SourceRecord { id: row.id.clone(), body: row.doc.clone(), meta: row.meta.clone() })
                    .collect();
                let outputs = (step.run)(inputs)?;
                let output_ids: std::collections::HashSet<(String, String)> = outputs.iter().map(|r| (r.collection.clone(), r.id.clone())).collect();
                for mut target in outputs {
                    if !target.body.is_object() { return Err(StorageError::invalid_input("migration output must be an object")); }
                    target.body[SCHEMA_FIELD] = step.to.into();
                    let options = target.created_ms.map_or_else(WriteOptions::new, |ms| WriteOptions::new().created_ms(ms));
                    let source_row = (source == step.item)
                        .then(|| page.items.iter().find(|row| row.id == target.id));
                    let precondition = source_row.flatten()
                        .map_or(Precondition::Absent, |row| Precondition::Version(row.version));
                    match docs.put_with(&target.collection, &target.id, target.body.clone(), precondition, options).await {
                        Ok(_) => written += 1,
                        Err(error) if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) => {
                            let existing = docs.get(&target.collection, &target.id).await?;
                            if existing.is_none_or(|row| row.doc != target.body) { return Err(error); }
                            written += 1;
                        }
                        Err(error) => return Err(error),
                    }
                }
                for row in &page.items {
                    if row.meta.schema >= step.to { continue; }
                    if source != step.item || !output_ids.contains(&(step.item.to_owned(), row.id.clone())) {
                        docs.delete(source, &row.id, Precondition::Version(row.version)).await?;
                    }
                }
                let cursor = last_cursor.map(|cursor| cursor.0);
                migrations.save(&progress_id, &Progress { owner: self.owner.as_str().to_owned(), step: step.id.to_owned(), cursor, done: false }).await?;
                if let Some(next) = page.next { query = query.after(next); } else { break; }
            }
            migrations.save(&progress_id, &Progress { owner: self.owner.as_str().to_owned(), step: step.id.to_owned(), cursor: None, done: true }).await?;
            report.ran.push(StepReport { owner: self.owner.as_str().to_owned(), item: step.item.to_owned(), step: step.id.to_owned(), written });
        }
        Ok(report)
    }
}

/// Batch size for owner migration runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateOptions { batch_size: usize }
impl MigrateOptions {
    /// Defaults to 100 documents per batch.
    #[must_use]
    pub const fn new() -> Self { Self { batch_size: PAGE_SIZE } }
    /// Set the number of source documents read per batch.
    #[must_use]
    pub const fn batch_size(mut self, batch_size: usize) -> Self { self.batch_size = batch_size; self }
}
impl Default for MigrateOptions { fn default() -> Self { Self::new() } }

/// Summary of an owner migration run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport { /// Steps completed by this invocation.
    pub ran: Vec<StepReport>, /// Steps already marked complete.
    pub already_done: usize }

/// A completed migration step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepReport { /// Owner name.
    pub owner: String, /// Collection being migrated.
    pub item: String, /// Stable step identifier.
    pub step: String, /// Number of destination records written.
    pub written: u64 }

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Progress { owner: String, step: String, cursor: Option<String>, done: bool }

/// Per-owner migration completion markers.
#[derive(Debug, Clone)]
pub struct Migrations { docs: Arc<dyn DocumentStore>, collection: String }
impl Migrations {
    /// Bind markers to the caller-provided document store and private collection.
    #[must_use]
    pub fn new(docs: &Arc<dyn DocumentStore>, owner: &str) -> Self { Self { docs: Arc::clone(docs), collection: format!("{owner}.migrations") } }

    /// Whether a step has completed.
    pub async fn done(&self, owner: &str, step: &str) -> Result<bool> {
        let id = Key::of([owner, step])?.as_str().to_owned();
        Ok(self.docs.get_as::<Progress>(&self.collection, &id).await?.is_some_and(|row| row.doc.done))
    }

    /// Mark an external import step complete.
    pub async fn mark_done(&self, owner: &str, step: &str) -> Result<()> {
        let id = Key::of([owner, step])?.as_str().to_owned();
        self.save(&id, &Progress { owner: owner.to_owned(), step: step.to_owned(), cursor: None, done: true }).await
    }

    async fn save(&self, id: &str, progress: &Progress) -> Result<()> {
        self.docs.put_as(&self.collection, id, progress, Precondition::None).await.map(|_| ())
    }
}
