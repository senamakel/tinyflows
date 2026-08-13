use super::*;

impl CompanionServer {
    /// Validates configuration and creates a disconnected server.
    pub fn new(config: CompanionServerConfig) -> Result<Self, CompanionServerError> {
        config.policy.validate()?;
        let bind_addr = config.policy.bind_addr;
        let native_secret = config.pairing_secret.expose().to_owned();
        let authenticator = Authenticator::new(&config.extension_id, config.pairing_secret)?;
        Ok(Self {
            inner: Arc::new(ServerInner {
                bind_addr,
                native_secret,
                authenticator,
                relay: Mutex::new(RelayState::new(config.policy)?),
                outbound: Mutex::new(None),
                pending: tokio::sync::Mutex::new(HashMap::new()),
                workflows_dir: config.workflows_dir,
                capabilities: config.capabilities,
                run_host: config.run_host,
                runs: Mutex::new(HashMap::new()),
                next_session: AtomicU64::new(0),
                next_run: AtomicU64::new(0),
            }),
        })
    }

    /// Returns the loopback socket address the server will bind.
    pub fn bind_addr(&self) -> SocketAddr {
        self.inner.bind_addr
    }

    /// Runs the authenticated WebSocket endpoint until listener failure.
    pub async fn serve(self) -> Result<(), CompanionServerError> {
        let listener = tokio::net::TcpListener::bind(self.inner.bind_addr)
            .await
            .map_err(|error| CompanionServerError::Listener(error.to_string()))?;
        let app = Router::new()
            .route("/v1/extension", get(upgrade))
            .route("/v1/native/tabs", get(native_tabs))
            .route("/v1/native/workflows", get(native_workflows))
            .route("/v1/native/runs", post(native_run))
            .with_state(self);
        axum::serve(listener, app)
            .await
            .map_err(|error| CompanionServerError::Listener(error.to_string()))
    }

    /// Lists valid workflow JSON files from the configured directory.
    pub fn workflows(&self) -> Result<Vec<WorkflowSummary>, CompanionServerError> {
        list_workflows(&self.inner.workflows_dir)
    }

    /// Lists workflows via the configured [`CompanionRunHost`](super::CompanionRunHost)
    /// if present, else from `workflows_dir`. Used by both request paths.
    pub(super) async fn dispatch_list_workflows(&self) -> Result<Vec<WorkflowSummary>, String> {
        match &self.inner.run_host {
            Some(host) => host.list_workflows().await,
            None => self.workflows().map_err(|error| error.to_string()),
        }
    }

    /// Starts a run via the run host if present, else via the built-in
    /// [`start_workflow`](Self::start_workflow). Returns the run id.
    pub(super) async fn dispatch_start_run(
        &self,
        workflow_id: &str,
        tab_id: TabId,
        input: Value,
    ) -> Result<String, String> {
        match &self.inner.run_host {
            Some(host) => host.start_run(workflow_id, tab_id, input).await,
            None => self
                .start_workflow(workflow_id, tab_id, input)
                .await
                .map_err(|error| error.to_string()),
        }
    }

    /// Cancels a run via the run host if present, else via the built-in
    /// [`cancel_workflow`](Self::cancel_workflow).
    pub(super) async fn dispatch_cancel_run(&self, run_id: &str) -> bool {
        match &self.inner.run_host {
            Some(host) => host.cancel_run(run_id).await,
            None => self.cancel_workflow(run_id).await,
        }
    }

    /// Returns a browser relay handle usable by an **external** workflow runner
    /// (an embedding host that drives its own engine rather than calling
    /// [`start_workflow`](Self::start_workflow)). The returned handle shares this
    /// server's live WebSocket session and pending-response map, so wrapping it in
    /// a [`RoutingToolInvoker`](crate::browser::RoutingToolInvoker) lets a host
    /// route `slug:"browser"` tool calls to the paired extension.
    ///
    /// The handle is always valid; if no extension is currently connected each
    /// `execute` fails closed with `relay_disconnected`.
    pub fn browser_relay(&self) -> Arc<dyn BrowserRelay> {
        Arc::new(SocketRelay {
            inner: self.inner.clone(),
        })
    }

    /// Whether a paired extension currently holds an authenticated relay session.
    /// External hosts use this to gate author-time / run-time browser readiness.
    pub fn is_extension_connected(&self) -> bool {
        self.inner
            .relay
            .lock()
            .map(|relay| relay.is_connected())
            .unwrap_or(false)
    }

    /// Snapshot of the tabs the user has explicitly shared with the companion.
    /// Empty when no extension is connected or nothing is shared.
    pub fn shared_tabs(&self) -> Vec<SharedTab> {
        self.inner
            .relay
            .lock()
            .map(|relay| relay.tabs().list().into_iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Binds a workflow run to an explicitly-shared tab so an **external**
    /// runner's `slug:"browser"` calls (dispatched through the handle from
    /// [`browser_relay`](Self::browser_relay)) are authorized against that tab.
    /// This mirrors what [`start_workflow`](Self::start_workflow) does
    /// internally for native runs — an embedding host must call this before
    /// executing a graph that contains browser nodes, or every browser action
    /// fails with `tab_not_shared`. External runners are not registered in the
    /// native run table, so their cancellation path must call
    /// [`cancel_bound_run`](Self::cancel_bound_run) before unbinding.
    pub fn bind_run(
        &self,
        run_id: impl Into<String>,
        tab_id: TabId,
    ) -> Result<(), CompanionServerError> {
        self.inner
            .relay
            .lock()
            .map_err(|_| lock_error())?
            .tabs_mut()
            .bind_run(run_id.into(), tab_id)
            .map_err(super::RelayError::from)?;
        Ok(())
    }

    /// Releases a run→tab binding after an external run settles. Idempotent.
    ///
    /// This does not cancel in-flight browser actions. On cancellation, call
    /// [`cancel_bound_run`](Self::cancel_bound_run) instead so the extension and
    /// pending relay requests are both notified.
    pub fn unbind_run(&self, run_id: &str) {
        if let Ok(mut relay) = self.inner.relay.lock() {
            relay.tabs_mut().unbind_run(run_id);
        }
    }

    /// Cancels browser work for an externally-owned run and releases its tab
    /// binding.
    ///
    /// An embedding host should call this alongside cancellation of its own
    /// workflow engine. Unlike [`cancel_workflow`](Self::cancel_workflow), this
    /// method does not expect the run to exist in the companion's native run
    /// table. Returns whether the run had a live tab binding.
    pub async fn cancel_bound_run(&self, run_id: &str) -> bool {
        let (was_bound, responses) = self
            .inner
            .relay
            .lock()
            .map(|mut relay| {
                let was_bound = relay.tabs().binding(run_id).is_some();
                (was_bound, relay.cancel_run(run_id))
            })
            .unwrap_or_default();
        self.dispatch(responses).await;
        was_bound
    }

    /// Starts a native run bound to one explicit shared tab.
    pub async fn start_workflow(
        &self,
        workflow_id: &str,
        tab_id: TabId,
        input: Value,
    ) -> Result<String, CompanionServerError> {
        let graph = load_workflow(&self.inner.workflows_dir, workflow_id)?;
        let node_kinds = graph
            .nodes
            .iter()
            .map(|node| {
                let kind = serde_json::to_value(&node.kind)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                (node.id.clone(), kind)
            })
            .collect::<HashMap<_, _>>();
        let compiled =
            compile(&graph).map_err(|error| CompanionServerError::Workflow(error.to_string()))?;
        let sequence = self.inner.next_run.fetch_add(1, Ordering::Relaxed) + 1;
        let run_id = format!("chrome-run-{sequence}");
        self.inner
            .relay
            .lock()
            .map_err(|_| lock_error())?
            .tabs_mut()
            .bind_run(run_id.clone(), tab_id)
            .map_err(super::RelayError::from)?;

        let token = CancellationToken::new();
        self.inner
            .runs
            .lock()
            .map_err(|_| lock_error())?
            .insert(run_id.clone(), token.clone());
        let server = self.clone();
        let spawned_run_id = run_id.clone();
        tokio::spawn(async move {
            server.send_json(&RunEvent::Started {
                protocol_version: BROWSER_PROTOCOL_VERSION,
                run_id: spawned_run_id.clone(),
                tab_id,
            });
            let browser = Arc::new(ChromeToolInvoker::new(
                Arc::new(SocketRelay {
                    inner: server.inner.clone(),
                }),
                spawned_run_id.clone(),
                tab_id,
            ));
            let mut capabilities = server.inner.capabilities.clone();
            capabilities.tools = Arc::new(RoutingToolInvoker::new(
                browser,
                server.inner.capabilities.tools.clone(),
            ));
            let observer = Arc::new(CompanionObserver {
                server: server.clone(),
                run_id: spawned_run_id.clone(),
                node_kinds,
            }) as Arc<dyn RunObserver>;
            match run_cancellable_with_observer(&compiled, input, &capabilities, token, &observer)
                .await
            {
                Ok(value) if value.cancelled => server.send_json(&RunEvent::Cancelled {
                    protocol_version: BROWSER_PROTOCOL_VERSION,
                    run_id: spawned_run_id.clone(),
                }),
                Ok(value) if !value.pending_approvals.is_empty() => {
                    server.send_json(&RunEvent::AwaitingApproval {
                        protocol_version: BROWSER_PROTOCOL_VERSION,
                        run_id: spawned_run_id.clone(),
                        pending_approvals: value.pending_approvals,
                    });
                }
                Ok(_) => server.send_json(&RunEvent::Completed {
                    protocol_version: BROWSER_PROTOCOL_VERSION,
                    run_id: spawned_run_id.clone(),
                    status: "success".into(),
                }),
                Err(_) => server.send_json(&RunEvent::Failed {
                    protocol_version: BROWSER_PROTOCOL_VERSION,
                    run_id: spawned_run_id.clone(),
                    code: "workflow_failed".into(),
                    message: "Workflow execution failed in the native companion".into(),
                }),
            }
            if let Ok(mut relay) = server.inner.relay.lock() {
                relay.tabs_mut().unbind_run(&spawned_run_id);
            }
            if let Ok(mut runs) = server.inner.runs.lock() {
                runs.remove(&spawned_run_id);
            }
        });
        Ok(run_id)
    }

    /// Cancels a companion-native run and its in-flight browser action.
    ///
    /// Runs registered only through [`bind_run`](Self::bind_run) are externally
    /// owned and must use [`cancel_bound_run`](Self::cancel_bound_run).
    pub async fn cancel_workflow(&self, run_id: &str) -> bool {
        let token = self
            .inner
            .runs
            .lock()
            .ok()
            .and_then(|runs| runs.get(run_id).cloned());
        let Some(token) = token else { return false };
        token.cancel();
        self.cancel_bound_run(run_id).await;
        true
    }

    pub(super) fn send_json<T: serde::Serialize>(&self, value: &T) {
        let Ok(text) = serde_json::to_string(value) else {
            return;
        };
        if let Ok(outbound) = self.inner.outbound.lock()
            && let Some(sender) = outbound.as_ref()
        {
            let _ = sender.send(Message::Text(text.into()));
        }
    }

    pub(super) async fn dispatch(&self, responses: Vec<BrowserResponse>) {
        let mut pending = self.inner.pending.lock().await;
        for response in responses {
            if matches!(response, BrowserResponse::Error { .. }) {
                self.send_json(&BrowserCancel {
                    protocol_version: BROWSER_PROTOCOL_VERSION,
                    message_type: BrowserCancelType::BrowserCancel,
                    request_id: response.request_id().to_owned(),
                });
            }
            if let Some(sender) = pending.remove(response.request_id()) {
                let result = match response {
                    BrowserResponse::Error { error, .. } => Err(error),
                    success => Ok(success),
                };
                let _ = sender.send(result);
            }
        }
    }
}
