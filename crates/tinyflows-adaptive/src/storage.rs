//! One config value, the whole persistence stack.
//!
//! The ledger and the vault are chosen the same way, from the same setting, so
//! every host was writing the same two `match`es — and scoping the two handles
//! separately, which is the leak waiting to happen: a request that calls
//! `for_tenant` on the ledger and forgets the vault has isolated the learning
//! and shared the graphs.
//!
//! [`Storage::open`] does the picking; [`Storage::for_tenant`] scopes **both
//! halves in one call**, so there is nothing to forget.
//!
//! ```text
//! "memory"                          → forgets on restart; tests, first look
//! ```
//!
//! Anything durable is the host's storage backend: the crate opens no database
//! of its own. A host that has opened a `tinystoragedrivers` backend (SQLite,
//! MongoDB, files, memory) puts both halves on its document handle with
//! [`Storage::from_documents`] (feature `storage-drivers`).
//!
//! A setting that names a database (a SQLite path, a `mongodb://` URI) fails
//! **at parse time**, saying so — a config error at boot, not a service that
//! runs perfectly and learns nothing.

use async_trait::async_trait;
use tinyflows::store::types::{WorkflowError, WorkflowRecord};

use crate::execute::StepRecord;
use crate::ledger::memory::MemoryLedger;
use crate::ledger::{
    Episode, Ledger, LedgerRow, Lesson, LessonKind, Page, Result as LedgerResult, Score,
};
use crate::workflows::Vault;
use crate::workflows::memory::MemoryVault;

/// What went wrong turning a config value into storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The value does not name a storage this build can open.
    #[error("storage config: {0}")]
    Config(String),
    /// The ledger backend refused to open.
    #[error("ledger: {0}")]
    Ledger(#[from] crate::ledger::LedgerError),
    /// The vault backend refused to open.
    #[error("vault: {0}")]
    Vault(#[from] WorkflowError),
}

/// Where the storage setting is read from, when the environment supplies it.
pub const STORAGE_VAR: &str = "TINYFLOWS_ADAPTIVE_STORAGE";

/// Where everything durable goes, parsed from one setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Config {
    /// In memory; forgets on restart. Never picked implicitly — the value has
    /// to literally say `memory`.
    Memory,
}

impl Config {
    /// Read a storage setting.
    ///
    /// * `memory` (or `:memory:`) — the ledger and vault that forget;
    /// * anything else — an error. The crate opens no database of its own: a
    ///   SQLite path or a `mongodb://` URI names storage the host opens as a
    ///   `tinystoragedrivers` backend and hands over with
    ///   [`Storage::from_documents`].
    ///
    /// Read the setting from the environment: [`STORAGE_VAR`].
    ///
    /// An unset variable is an **error naming the variable**, never a default.
    /// The tempting fallbacks are both wrong: defaulting to a disk location
    /// invents a path on the operator's machine nobody named, and defaulting
    /// to memory is a service that runs perfectly and learns nothing — the
    /// failure shape this crate is built to refuse.
    ///
    /// # Errors
    /// When the variable is unset, or its value fails [`parse`](Self::parse).
    pub fn from_env() -> Result<Self, StorageError> {
        Self::from_setting(std::env::var(STORAGE_VAR).ok().as_deref())
    }

    /// [`from_env`](Self::from_env) with the read made explicit, so the rule is
    /// testable without any test mutating process-wide state.
    pub fn from_setting(value: Option<&str>) -> Result<Self, StorageError> {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            Some(value) => Self::parse(value),
            None => Err(StorageError::Config(format!(
                "{STORAGE_VAR} is not set; expected `memory` (durable storage is a tinystoragedrivers backend the host opens, not a setting)"
            ))),
        }
    }

    /// # Errors
    /// When the value names a database. Caught here so it fails at boot with
    /// the way forward named, not as a service that quietly forgets.
    pub fn parse(value: &str) -> Result<Self, StorageError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(StorageError::Config(
                "empty storage setting; expected `memory` (durable storage is a tinystoragedrivers backend the host opens, not a setting)"
                    .to_string(),
            ));
        }
        if value == "memory" || value == ":memory:" {
            return Ok(Self::Memory);
        }
        let names = if value.starts_with("mongodb://") || value.starts_with("mongodb+srv://") {
            "a mongodb:// URI"
        } else {
            "a sqlite path"
        };
        Err(StorageError::Config(format!(
            "`{value}` reads as {names}, but this crate opens no database of its own; \
             open a tinystoragedrivers backend and use `Storage::from_documents` \
             (feature `storage-drivers`), or set `memory`"
        )))
    }
}

/// A ledger and a vault, opened from one [`Config`] and scoped together.
pub struct Storage {
    ledger: AnyLedger,
    vault: AnyVault,
}

impl Storage {
    /// [`Config::from_env`] and [`open`](Self::open) in one call — the whole
    /// persistence stack from the environment.
    ///
    /// # Errors
    /// As both halves.
    pub async fn from_env() -> Result<Self, StorageError> {
        Self::open(&Config::from_env()?).await
    }

    /// Open both halves of the configured backend.
    ///
    /// # Errors
    /// When the backend cannot be opened or reached.
    pub async fn open(config: &Config) -> Result<Self, StorageError> {
        Ok(match config {
            Config::Memory => Self {
                ledger: AnyLedger::Memory(MemoryLedger::new()),
                vault: AnyVault::Memory(MemoryVault::new()),
            },
        })
    }

    /// Both halves, scoped to one tenant, in one call.
    ///
    /// One call rather than two because the failure this module exists to
    /// prevent is scoping the ledger and forgetting the vault — isolated
    /// learning over shared graphs, or the reverse.
    #[must_use]
    pub fn for_tenant(&self, scope: &str) -> Self {
        Self {
            ledger: self.ledger.for_tenant(scope),
            vault: self.vault.for_tenant(scope),
        }
    }

    /// The ledger half.
    #[must_use]
    pub fn ledger(&self) -> &AnyLedger {
        &self.ledger
    }

    /// The vault half.
    #[must_use]
    pub fn vault(&self) -> &AnyVault {
        &self.vault
    }

    /// Both halves on a `tinystoragedrivers` document handle, in the global
    /// bucket; [`for_tenant`](Self::for_tenant) scopes them as for every
    /// other backend.
    ///
    /// The handle is usually one the host already bound to a storage scope
    /// (its agent, its user), so the adaptive tenants live inside it.
    #[cfg(feature = "storage-drivers")]
    #[must_use]
    pub fn from_documents(
        docs: std::sync::Arc<dyn tinyflows_storage::DocumentStore>,
    ) -> Self {
        Self {
            ledger: AnyLedger::Driver(crate::drivers::DriverLedger::new(std::sync::Arc::clone(
                &docs,
            ))),
            vault: AnyVault::Driver(crate::drivers::DriverVault::new(docs)),
        }
    }
}

/// Whichever ledger the config picked, behind the one trait.
pub enum AnyLedger {
    /// Forgets on restart.
    Memory(MemoryLedger),
    /// A `tinystoragedrivers` document store.
    #[cfg(feature = "storage-drivers")]
    Driver(crate::drivers::DriverLedger),
}

impl AnyLedger {
    /// A handle onto the same store, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: &str) -> Self {
        match self {
            Self::Memory(l) => Self::Memory(l.for_tenant(scope)),
            #[cfg(feature = "storage-drivers")]
            Self::Driver(l) => Self::Driver(l.for_tenant(scope)),
        }
    }
}

/// Delegate one call to whichever backend is inside.
macro_rules! on_ledger {
    ($self:ident, $l:ident => $call:expr) => {
        match $self {
            AnyLedger::Memory($l) => $call,
            #[cfg(feature = "storage-drivers")]
            AnyLedger::Driver($l) => $call,
        }
    };
}

#[async_trait]
impl Ledger for AnyLedger {
    fn scope(&self) -> Option<&str> {
        on_ledger!(self, l => l.scope())
    }
    async fn append(&self, row: &LedgerRow) -> LedgerResult<String> {
        on_ledger!(self, l => l.append(row).await)
    }
    async fn rows(&self, episode: &str) -> LedgerResult<Vec<LedgerRow>> {
        on_ledger!(self, l => l.rows(episode).await)
    }
    async fn promote(&self, lesson: &Lesson, cites: &[String]) -> LedgerResult<String> {
        on_ledger!(self, l => l.promote(lesson, cites).await)
    }
    async fn lessons(&self, kind: Option<LessonKind>) -> LedgerResult<Vec<Lesson>> {
        on_ledger!(self, l => l.lessons(kind).await)
    }
    async fn evidence(&self, lesson_id: &str) -> LedgerResult<Vec<LedgerRow>> {
        on_ledger!(self, l => l.evidence(lesson_id).await)
    }
    async fn score_lesson(&self, lesson_id: &str, helped: bool) -> LedgerResult<()> {
        on_ledger!(self, l => l.score_lesson(lesson_id, helped).await)
    }
    async fn score_workflow(&self, workflow_id: &str, helped: bool) -> LedgerResult<()> {
        on_ledger!(self, l => l.score_workflow(workflow_id, helped).await)
    }
    async fn workflow_score(&self, workflow_id: &str) -> LedgerResult<Score> {
        on_ledger!(self, l => l.workflow_score(workflow_id).await)
    }
    async fn link_variant(&self, parent: &str, variant: &str) -> LedgerResult<()> {
        on_ledger!(self, l => l.link_variant(parent, variant).await)
    }
    async fn parent_of(&self, id: &str) -> LedgerResult<Option<String>> {
        on_ledger!(self, l => l.parent_of(id).await)
    }
    async fn children_of(&self, id: &str) -> LedgerResult<Vec<String>> {
        on_ledger!(self, l => l.children_of(id).await)
    }
    async fn save_episode(&self, episode: &Episode) -> LedgerResult<()> {
        on_ledger!(self, l => l.save_episode(episode).await)
    }
    async fn episode(&self, id: &str) -> LedgerResult<Option<Episode>> {
        on_ledger!(self, l => l.episode(id).await)
    }
    async fn episodes(&self, running_only: bool, page: Page) -> LedgerResult<Vec<Episode>> {
        on_ledger!(self, l => l.episodes(running_only, page).await)
    }
    async fn save_steps(&self, row_id: &str, steps: &[StepRecord]) -> LedgerResult<()> {
        on_ledger!(self, l => l.save_steps(row_id, steps).await)
    }
    async fn steps(&self, row_id: &str) -> LedgerResult<Vec<StepRecord>> {
        on_ledger!(self, l => l.steps(row_id).await)
    }
}

/// Whichever vault the config picked, behind the one trait.
pub enum AnyVault {
    /// Forgets on restart.
    Memory(MemoryVault),
    /// A `tinystoragedrivers` document store.
    #[cfg(feature = "storage-drivers")]
    Driver(crate::drivers::DriverVault),
}

impl AnyVault {
    /// A handle onto the same store, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: &str) -> Self {
        match self {
            Self::Memory(v) => Self::Memory(v.for_tenant(scope)),
            #[cfg(feature = "storage-drivers")]
            Self::Driver(v) => Self::Driver(v.for_tenant(scope)),
        }
    }
}

macro_rules! on_vault {
    ($self:ident, $v:ident => $call:expr) => {
        match $self {
            AnyVault::Memory($v) => $call,
            #[cfg(feature = "storage-drivers")]
            AnyVault::Driver($v) => $call,
        }
    };
}

#[async_trait]
impl Vault for AnyVault {
    fn scope(&self) -> Option<&str> {
        on_vault!(self, v => v.scope())
    }
    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        on_vault!(self, v => v.load().await)
    }
    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        on_vault!(self, v => v.put(record).await)
    }
    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        on_vault!(self, v => v.remove(id).await)
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
