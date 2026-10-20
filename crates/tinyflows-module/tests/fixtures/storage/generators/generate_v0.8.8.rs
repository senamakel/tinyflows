use std::sync::Arc;

use tinyflows_drivers::schedule::{CronDocuments, JOBS};
use tinyflows_schedule::Schedule;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryStorage::new();
    let docs = Arc::clone(backend.for_scope(&Scope::new("fixture")?)?.documents());
    let store = CronDocuments::new(Arc::clone(&docs));
    let job = store
        .add_flow_schedule_job("legacy-fixture-flow", Schedule::Every { every_ms: 60_000 })
        .await?;
    let record = docs
        .get(JOBS, &job.id)
        .await?
        .expect("the public writer persisted the job");
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "id": record.id,
            "doc": record.doc,
        }))?
    );
    Ok(())
}
