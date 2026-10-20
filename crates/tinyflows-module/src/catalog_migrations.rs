//! Explicit, scope-bound catalog storage migration.

use anyhow::Result;
use std::sync::Arc;
use tinybus::Connection;
use tinyflows_bus::catalog::CatalogStorage;
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinyflows_drivers::{catalog_storage_schema, import_legacy_kv};
use tinyflows_storage::MigrateOptions;

pub(super) async fn migrate(connection: Connection, storage: CatalogStorage) -> Result<()> {
    let mut documents =
        super::catalog_documents::Documents::new(connection, storage.callback).await?;
    documents.require_record_metadata().await?;
    let _guard = super::migration_lock::catalog().await;
    let documents = Arc::new(documents);
    FlowCatalogDocuments::new(documents).ensure().await?;
    let schema = catalog_storage_schema();
    let report = schema.migrate(&documents, &MigrateOptions::default()).await?;
    let imported_kv = import_legacy_kv(&documents).await?;
    tracing::debug!(
        steps_run = report.ran.len(),
        already_done = report.already_done,
        imported_kv,
        "catalog migration complete"
    );
    Ok(())
}
