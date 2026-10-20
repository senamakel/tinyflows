//! Bounded entry point for importing legacy records through a scoped callback.

use std::sync::Arc;

use serde_json::Value;
use tinybus::{Connection, Error};
use tinyflows_bus::catalog::{ImportLegacyBatchRequest, ImportLegacyBatchResponse};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};
use tinyflows_storage::DocumentStore;

fn failure(error: impl std::fmt::Display) -> Error {
    Error::failed(error.to_string())
}

pub(super) async fn import(
    connection: &Connection,
    request: ImportLegacyBatchRequest,
) -> tinybus::Result<ImportLegacyBatchResponse> {
    if request.records.len() > tinyflows_bus::catalog::MAX_IMPORT_BATCH_RECORDS {
        return Err(Error::failed("legacy import batch exceeds record limit"));
    }
    if serde_json::to_vec(&request.storage).map_err(failure)?.len()
        > tinyflows_bus::catalog::MAX_IMPORT_REQUEST_ENVELOPE_BYTES
    {
        return Err(Error::failed(
            "legacy import storage envelope exceeds byte limit",
        ));
    }
    if serde_json::to_vec(&request).map_err(failure)?.len()
        > tinyflows_bus::catalog::MAX_IMPORT_BATCH_BYTES
    {
        return Err(Error::failed("legacy import batch exceeds byte limit"));
    }
    let port = Arc::new(
        super::catalog_documents::Documents::new(connection.clone(), request.storage.callback)
            .await
            .map_err(failure)?,
    );
    let docs = Arc::clone(&port) as Arc<dyn DocumentStore>;
    let catalog = FlowCatalogDocuments::new(Arc::clone(&docs));
    let checkpointer = DriverCheckpointer::<Value>::new(docs);
    tinyflows_import::apply_legacy_batch(&request.records, &catalog, Some(&checkpointer))
        .await
        .map_err(failure)
}
