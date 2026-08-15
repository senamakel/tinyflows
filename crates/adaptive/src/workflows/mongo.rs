//! Workflows in the same MongoDB database as the ledger.

use async_trait::async_trait;
use mongodb::bson::{Document, doc};
use mongodb::{Client, Collection, Database};
use tinyflows::store::types::{WorkflowError, WorkflowRecord};

use super::Vault;

const WORKFLOWS: &str = "workflows";

/// A vault backed by a MongoDB database.
#[derive(Clone)]
pub struct MongoVault {
    db: Database,
    scope: Option<String>,
}

impl MongoVault {
    /// Connect to `uri` and use the database named `database`.
    ///
    /// # Errors
    /// When the URI is malformed or the server is unreachable.
    pub async fn connect(uri: &str, database: &str) -> Result<Self, WorkflowError> {
        let client = Client::with_uri_str(uri)
            .await
            .map_err(|e| WorkflowError::Engine(e.to_string()))?;
        Ok(Self::with_database(client.database(database)))
    }

    /// Use an already-connected database, for a host managing its own pool.
    #[must_use]
    pub fn with_database(db: Database) -> Self {
        Self { db, scope: None }
    }

    /// A handle onto the same database, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: impl Into<String>) -> Self {
        Self {
            db: self.db.clone(),
            scope: Some(scope.into()),
        }
    }

    /// Stored as a present empty string rather than an absent field, so the
    /// upsert filter matches one document — the same reason the ledger does it.
    fn bucket(&self) -> &str {
        self.scope.as_deref().unwrap_or_default()
    }

    fn workflows(&self) -> Collection<Document> {
        self.db.collection(WORKFLOWS)
    }
}

fn mongo(err: mongodb::error::Error) -> WorkflowError {
    WorkflowError::Engine(err.to_string())
}

#[async_trait]
impl Vault for MongoVault {
    fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        // This bucket plus global. A record written before scoping existed has
        // no field at all, which `$in` with "" does not match — but nothing
        // wrote one, because this collection is new.
        let mut cursor = self
            .workflows()
            .find(doc! { "scope_key": { "$in": [self.bucket(), ""] } })
            .sort(doc! { "_id": 1 })
            .await
            .map_err(mongo)?;

        let mut out = Vec::new();
        while cursor.advance().await.map_err(mongo)? {
            let document = cursor.deserialize_current().map_err(mongo)?;
            let raw = document.get_str("document").unwrap_or_default();
            out.push(serde_json::from_str(raw).map_err(|e| {
                WorkflowError::Engine(format!("stored workflow no longer parses: {e}"))
            })?);
        }
        Ok(out)
    }

    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        let document = serde_json::to_string(record)
            .map_err(|e| WorkflowError::Engine(format!("workflow will not serialize: {e}")))?;
        // Stored as a JSON string rather than a BSON subdocument: a node config
        // is arbitrary JSON, and BSON refuses keys containing a dot — which a
        // config keyed by a filename or a version has.
        self.workflows()
            .update_one(
                doc! { "scope_key": self.bucket(), "workflow_id": &record.id },
                doc! { "$set": { "document": document } },
            )
            .upsert(true)
            .await
            .map_err(mongo)?;
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        self.workflows()
            .delete_one(doc! { "scope_key": self.bucket(), "workflow_id": id })
            .await
            .map_err(mongo)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflows::conformance;

    /// Needs a real server, so it is `#[ignore]` and visible in the run summary
    /// rather than silently skipped — the same posture as the mongo ledger.
    #[tokio::test]
    #[ignore = "needs a MongoDB server; set ADAPTIVE_MONGO_URI"]
    async fn passes_the_conformance_suite() {
        let uri = std::env::var("ADAPTIVE_MONGO_URI").expect("ADAPTIVE_MONGO_URI");
        let name = format!("adaptive_vault_{}", std::process::id());
        let vault = MongoVault::connect(&uri, &name).await.expect("connect");
        conformance::run_all(&vault).await;
        conformance::run_tenants(&vault, &vault.for_tenant("a"), &vault.for_tenant("b")).await;
        vault.db.drop().await.expect("drop the throwaway database");
    }
}
