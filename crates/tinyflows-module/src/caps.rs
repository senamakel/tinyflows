//! Adapters from the exact per-run `CapsHost` callback to engine capabilities.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tinybus::Connection;
use tinybus::{InterfaceName, InterfaceVersion, PeerManifest, Version};
use tinyflows::{
    caps::{
        self, ApprovalDecision, ApprovalOutcome, ApprovalProvider, ApprovalRequest, CodeLanguage,
        CodeRunner, HttpClient, LlmProvider, MemoryProvider, ShellInterpreter, ShellOutcome,
        ShellRequest, ShellRunner, ShellScript, StateStore, TaskRunner, TokioTaskRunner,
        ToolInvoker, WorkflowResolver,
    },
    error::{EngineError, Result},
};
use tinyflows_bus::{
    catalog::CatalogStorage,
    runs::{self, CapsHost, caps as members},
};
use tinyflows_drivers::catalog::{FlowCatalogDocuments, FlowStateDocuments};
use tinyflows_storage::DocumentStore;

#[path = "caps/task_executor.rs"]
mod task_executor;
use task_executor::{CapabilityTaskExecutor, TaskRunnerLink};

#[path = "caps/agent.rs"]
mod agent;

/// Maximum time a host operation or agent event stream may take.
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
/// Delay between empty, nonterminal agent event polls.
const EMPTY_POLL_DELAY: Duration = Duration::from_millis(25);

/// Engine adapters fixed to one host-created callback address and run context.
pub struct CapsHostPorts {
    connection: Connection,
    address: runs::CallbackAddress,
    progress: Option<RunProgressSink>,
}

/// Synchronous, non-blocking sink for bounded host agent progress events.
pub type RunProgressSink = Arc<dyn Fn(String, Value) + Send + Sync>;

/// Trusted values supplied by run management, separate from graph and trigger
/// JSON. `storage` is already bound to the acting owner scope.
pub struct RunScope {
    /// The trusted flow id used to namespace durable workflow state.
    pub flow_id: String,
    /// Document callback already bound by the host to the owner scope.
    pub storage: CatalogStorage,
}

impl CapsHostPorts {
    /// Builds one run's engine bundle from handles created by the trusted
    /// run manager. `host.callback` must be bound to that run's flow, run id,
    /// approval policy and owner scope; `scope.storage` must be bound to the
    /// same owner scope, and `scope.flow_id` is the trusted flow id used for
    /// durable state. Graph and trigger JSON never supply these values.
    pub async fn capabilities(
        connection: Connection,
        host: CapsHost,
        scope: RunScope,
    ) -> Result<caps::Capabilities> {
        Self::capabilities_with_progress(connection, host, scope, None).await
    }

    /// Builds the run capability bundle and forwards agent progress to the
    /// module's run event writer when a sink is supplied.
    pub async fn capabilities_with_progress(
        connection: Connection,
        host: CapsHost,
        scope: RunScope,
        progress: Option<RunProgressSink>,
    ) -> Result<caps::Capabilities> {
        require_caps_host_v2(&connection, &host.callback.destination).await?;
        let documents =
            super::catalog_documents::Documents::new(connection.clone(), scope.storage.callback)
                .await
                .map_err(capability_error)?;
        let catalog = FlowCatalogDocuments::new(Arc::new(documents) as Arc<dyn DocumentStore>);
        let state: Arc<dyn StateStore> = Arc::new(
            FlowStateDocuments::open(catalog.clone(), format!("flow:{}", scope.flow_id))
                .await
                .map_err(capability_error)?,
        );
        let resolver: Arc<dyn WorkflowResolver> = Arc::new(CatalogResolver { catalog });
        let ports = Arc::new(Self {
            connection,
            address: host.callback,
            progress,
        });
        let mut capabilities = caps::Capabilities {
            llm: ports.clone(),
            tools: ports.clone(),
            http: ports.clone(),
            code: ports.clone(),
            state,
            resolver,
            agent: Some(ports.clone()),
            shell: Some(ports.clone()),
            memory: Some(ports.clone()),
            tasks: None,
            approvals: Some(ports),
        };
        // Capture a taskless base bundle and reattach the shared runner to child
        // workflows through a weak link. This preserves nested spawn/gate
        // execution without a runner -> capabilities -> runner Arc cycle.
        let task_runner = Arc::new(TaskRunnerLink::default());
        let executor = Arc::new(CapabilityTaskExecutor::new(
            capabilities.clone(),
            Arc::clone(&task_runner),
        ));
        let runner = Arc::new(TokioTaskRunner::with_executor(executor)) as Arc<dyn TaskRunner>;
        task_runner.bind(&runner);
        capabilities.tasks = Some(runner);
        Ok(capabilities)
    }

    pub(super) fn publish_progress(&self, kind: String, data: Value) {
        if let Some(progress) = &self.progress {
            progress(kind, data);
        }
    }

    async fn call<A, R>(&self, member: &str, args: &A) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        let proxy = self
            .connection
            .proxy(
                &self.address.destination,
                &self.address.object_path,
                runs::CAPS_INTERFACE,
            )
            .map_err(capability_error)?
            .with_timeout(CALLBACK_TIMEOUT);
        proxy.call(member, args).await.map_err(capability_error)
    }

    async fn call_one<A, R>(&self, member: &str, request: &A) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        self.call(member, &(request,)).await
    }

    async fn call_one_timeout<A, R>(
        &self,
        member: &str,
        request: &A,
        timeout: Duration,
    ) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        let proxy = self
            .connection
            .proxy(
                &self.address.destination,
                &self.address.object_path,
                runs::CAPS_INTERFACE,
            )
            .map_err(capability_error)?
            .with_timeout(timeout);
        proxy
            .call(member, &(request,))
            .await
            .map_err(capability_error)
    }
}

async fn require_caps_host_v2(connection: &Connection, destination: &str) -> Result<()> {
    let interface = InterfaceName::new(runs::CAPS_INTERFACE).map_err(capability_error)?;
    let manifest = connection
        .manifest_of(destination)
        .await
        .map_err(capability_error)?
        .ok_or_else(|| capability_error("CapsHost v2 peer has not announced a manifest"))?;
    if manifest.provided(&interface).is_none() {
        return Err(capability_error(
            "CapsHost v2 is required for cancellable agent callbacks",
        ));
    }
    let local = PeerManifest::new("tinyflows-module").consumes(InterfaceVersion::consumed(
        interface,
        Version::new(u64::from(runs::CAPS_CONTRACT_VERSION), 0, 0),
    ));
    let compatibility = connection
        .compatibility(destination, runs::CAPS_INTERFACE, &local)
        .await
        .map_err(capability_error)?;
    if !compatibility.is_compatible() {
        return Err(capability_error(format!(
            "CapsHost v2 is required for cancellable agent callbacks; peer compatibility is {compatibility:?}"
        )));
    }
    Ok(())
}

struct CatalogResolver {
    catalog: FlowCatalogDocuments,
}

#[async_trait]
impl WorkflowResolver for CatalogResolver {
    async fn resolve(&self, id: &str) -> Result<tinyflows::model::WorkflowGraph> {
        let graph = self
            .catalog
            .get_flow(id)
            .await
            .map_err(capability_error)?
            .map(|flow| flow.graph)
            .ok_or_else(|| capability_error(format!("missing workflow {id}")))?;
        tinyflows::compat::ensure_compatible(&graph).map_err(capability_error)?;
        Ok(graph)
    }
}

fn capability_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::Capability(error.to_string())
}

fn convert<A: Serialize, B: DeserializeOwned>(value: A) -> Result<B> {
    let wire = serde_json::to_value(value).map_err(capability_error)?;
    serde_json::from_value(wire).map_err(capability_error)
}

#[cfg(test)]
#[path = "caps_tests.rs"]
mod tests;

#[async_trait]
impl LlmProvider for CapsHostPorts {
    async fn complete(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::LLM_COMPLETE, &(request, conn)).await
    }
}

#[async_trait]
impl ToolInvoker for CapsHostPorts {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::TOOL_INVOKE, &(slug, args, conn)).await
    }
}

#[async_trait]
impl HttpClient for CapsHostPorts {
    async fn request(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::HTTP_REQUEST, &(request, conn)).await
    }
}

#[async_trait]
impl CodeRunner for CapsHostPorts {
    async fn run(&self, language: CodeLanguage, source: &str, input: Value) -> Result<Value> {
        let language = match language {
            CodeLanguage::JavaScript => runs::CodeLanguage::JavaScript,
            CodeLanguage::Python => runs::CodeLanguage::Python,
        };
        let request = runs::CodeRunRequest {
            language,
            source: source.to_owned(),
            input,
        };
        self.call_one(members::CODE_RUN, &request).await
    }
}

#[async_trait]
impl ShellRunner for CapsHostPorts {
    async fn run(&self, request: ShellRequest) -> Result<ShellOutcome> {
        let request = runs::ShellRunRequest {
            interpreter: match request.interpreter {
                ShellInterpreter::Sh => runs::ShellInterpreter::Sh,
                ShellInterpreter::Bash => runs::ShellInterpreter::Bash,
            },
            script: match request.script {
                ShellScript::Inline(script) => runs::ShellScript::Inline(script),
                ShellScript::Path(path) => runs::ShellScript::Path(path),
            },
            cwd: request.cwd,
            env: request.env,
            input: request.input,
        };
        let outcome: runs::ShellRunOutcome = self.call_one(members::SHELL_RUN, &request).await?;
        Ok(ShellOutcome {
            exit_code: outcome.exit_code,
            stdout: outcome.stdout,
            stderr: outcome.stderr,
        })
    }
}

#[async_trait]
impl MemoryProvider for CapsHostPorts {
    async fn recall(&self, scope: &str, query: &str, opts: Value) -> Result<Value> {
        self.call(members::MEMORY_RECALL, &(scope, query, opts))
            .await
    }

    async fn flavour(&self, slug: &str) -> Result<Value> {
        self.call(members::MEMORY_FLAVOUR, &(slug,)).await
    }

    async fn people(&self, query: Option<&str>) -> Result<Value> {
        self.call(members::MEMORY_PEOPLE, &(query,)).await
    }

    async fn remember(&self, scope: &str, key: &str, value: Value) -> Result<()> {
        self.call(members::MEMORY_REMEMBER, &(scope, key, value))
            .await
    }

    async fn forget(&self, scope: &str, key: &str) -> Result<()> {
        self.call(members::MEMORY_FORGET, &(scope, key)).await
    }
}

#[async_trait]
impl ApprovalProvider for CapsHostPorts {
    async fn decide(&self, request: &ApprovalRequest) -> Result<ApprovalOutcome> {
        let wire = runs::ApprovalRequest {
            request_id: request.request_id.clone(),
            node_id: request.node_id.clone(),
            run_id: request.run_id.clone(),
            title: request.title.clone(),
            prompt: request.prompt.clone(),
            subject: runs::ApprovalSubject {
                kind: request.subject.kind.clone(),
                value: request.subject.value.clone(),
            },
            assignees: request.assignees.clone(),
            metadata: request.metadata.clone(),
        };
        match self
            .call_one::<_, runs::ApprovalOutcome>(members::APPROVAL_DECIDE, &wire)
            .await?
        {
            runs::ApprovalOutcome::Pending => Ok(ApprovalOutcome::Pending),
            runs::ApprovalOutcome::Decided(decision) => {
                Ok(ApprovalOutcome::Decided(ApprovalDecision {
                    approved: decision.approved,
                    decided_by: decision.decided_by,
                    comment: decision.comment,
                    payload: decision.payload,
                }))
            }
        }
    }

    async fn cancel(&self, request_id: &str, reason: &str) -> Result<()> {
        self.call_one(
            members::APPROVAL_CANCEL,
            &runs::ApprovalCancelRequest {
                request_id: request_id.to_owned(),
                reason: reason.to_owned(),
            },
        )
        .await
    }
}
