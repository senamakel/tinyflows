use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeRunRequest {
    workflow_id: String,
    tab_id: TabId,
    #[serde(default)]
    input: Value,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatMessage {
    protocol_version: u32,
    #[serde(rename = "type")]
    message_type: HeartbeatType,
}

#[derive(serde::Deserialize)]
enum HeartbeatType {
    #[serde(rename = "heartbeat")]
    Heartbeat,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TabSharedMessage {
    protocol_version: u32,
    event: TabSharedType,
    tab: AnnouncedTab,
}

#[derive(serde::Deserialize)]
enum TabSharedType {
    #[serde(rename = "tab_shared")]
    TabShared,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnouncedTab {
    id: u64,
    window_id: u64,
    url: String,
    title: String,
}

pub(super) async fn native_tabs(
    State(server): State<CompanionServer>,
    headers: HeaderMap,
) -> Response {
    if !native_authorized(&server, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let tabs = server
        .inner
        .relay
        .lock()
        .map(|relay| relay.tabs().list().into_iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    Json(json!({"protocol_version":BROWSER_PROTOCOL_VERSION,"tabs":tabs})).into_response()
}

pub(super) async fn native_workflows(
    State(server): State<CompanionServer>,
    headers: HeaderMap,
) -> Response {
    if !native_authorized(&server, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match server.dispatch_list_workflows().await {
        Ok(workflows) => Json(json!({
            "protocol_version":BROWSER_PROTOCOL_VERSION,
            "workflows":workflows
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"code":"workflow_list_failed","message":error})),
        )
            .into_response(),
    }
}

pub(super) async fn native_run(
    State(server): State<CompanionServer>,
    headers: HeaderMap,
    Json(request): Json<NativeRunRequest>,
) -> Response {
    if !native_authorized(&server, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match server
        .dispatch_start_run(&request.workflow_id, request.tab_id, request.input)
        .await
    {
        Ok(run_id) => Json(json!({
            "protocol_version":BROWSER_PROTOCOL_VERSION,
            "run_id":run_id
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"code":"workflow_start_failed","message":error})),
        )
            .into_response(),
    }
}

fn native_authorized(server: &CompanionServer, headers: &HeaderMap) -> bool {
    let candidate = header(headers, "authorization");
    let candidate = candidate.strip_prefix("Bearer ").unwrap_or_default();
    constant_time_eq(candidate.as_bytes(), server.inner.native_secret.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

pub(super) async fn upgrade(
    State(server): State<CompanionServer>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let origin = header(&headers, "origin");
    let protocols_header = header(&headers, "sec-websocket-protocol");
    let offered = protocols_header
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if server
        .inner
        .authenticator
        .authenticate(&WebSocketHandshake {
            origin: &origin,
            subprotocols: &offered,
        })
        .is_err()
    {
        return (StatusCode::UNAUTHORIZED, "unauthorized extension relay").into_response();
    }
    websocket
        .protocols([PROTOCOL_SUBPROTOCOL])
        .on_upgrade(move |socket| extension_session(server, socket))
}

async fn extension_session(server: CompanionServer, socket: WebSocket) {
    let session_id = format!(
        "extension-session-{}",
        server.inner.next_session.fetch_add(1, Ordering::Relaxed) + 1
    );
    let (mut sink, mut stream) = socket.split();
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
    {
        let Ok(mut relay) = server.inner.relay.lock() else {
            return;
        };
        if relay.is_connected() || relay.connect(session_id.clone(), Instant::now()).is_err() {
            return;
        }
    }
    if let Ok(mut outbound) = server.inner.outbound.lock() {
        *outbound = Some(sender);
    }
    let writer = tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });
    let mut heartbeat_check = tokio::time::interval(Duration::from_secs(5));
    heartbeat_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat_check.tick().await;
    loop {
        tokio::select! {
            message = stream.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if handle_text(&server, &session_id, &text).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            },
            _ = heartbeat_check.tick() => {
                let responses = {
                    server.inner.relay.lock().ok()
                        .and_then(|mut relay| relay.disconnect_if_stale(Instant::now()))
                        .map(|outcome| outcome.responses)
                };
                if let Some(responses) = responses {
                    server.dispatch(responses).await;
                    break;
                }
            },
        }
    }
    if let Ok(mut outbound) = server.inner.outbound.lock() {
        *outbound = None;
    }
    let responses = {
        server
            .inner
            .relay
            .lock()
            .map(|mut relay| relay.disconnect(&session_id).responses)
            .unwrap_or_default()
    };
    server.dispatch(responses).await;
    writer.abort();
}

async fn handle_text(server: &CompanionServer, session: &str, text: &str) -> Result<(), ()> {
    let value: Value = serde_json::from_str(text).map_err(|_| ())?;
    if let Ok(heartbeat) = serde_json::from_value::<HeartbeatMessage>(value.clone()) {
        if heartbeat.protocol_version != BROWSER_PROTOCOL_VERSION {
            return Err(());
        }
        let HeartbeatType::Heartbeat = heartbeat.message_type;
        return server
            .inner
            .relay
            .lock()
            .map_err(|_| ())?
            .heartbeat(session, Instant::now())
            .map_err(|_| ());
    }
    if let Ok(response) = serde_json::from_value::<BrowserResponse>(value.clone()) {
        let completion = server
            .inner
            .relay
            .lock()
            .map_err(|_| ())?
            .complete_action(session, &response);
        if matches!(
            completion,
            Err(crate::companion::RelayError::UnknownRequestId)
        ) {
            return Ok(());
        }
        completion.map_err(|_| ())?;
        if let Some(sender) = server
            .inner
            .pending
            .lock()
            .await
            .remove(response.request_id())
        {
            let _ = sender.send(Ok(response));
        }
        return Ok(());
    }
    if let Ok(message) = serde_json::from_value::<TabSharedMessage>(value.clone()) {
        if message.protocol_version != BROWSER_PROTOCOL_VERSION {
            return Err(());
        }
        let TabSharedType::TabShared = message.event;
        server
            .inner
            .relay
            .lock()
            .map_err(|_| ())?
            .tabs_mut()
            .share(
                message.tab.id,
                message.tab.window_id,
                message.tab.url,
                message.tab.title,
            )
            .map_err(|_| ())?;
        return Ok(());
    }
    if let Ok(event) = serde_json::from_value::<BrowserEvent>(value.clone()) {
        let version = match &event {
            BrowserEvent::ActionStarted {
                protocol_version, ..
            }
            | BrowserEvent::ActionCompleted {
                protocol_version, ..
            }
            | BrowserEvent::ActionFailed {
                protocol_version, ..
            }
            | BrowserEvent::TabRevoked {
                protocol_version, ..
            }
            | BrowserEvent::RelayDisconnected { protocol_version } => *protocol_version,
        };
        if version != BROWSER_PROTOCOL_VERSION {
            return Err(());
        }
        match event {
            BrowserEvent::TabRevoked { tab_id, .. } => {
                let (_, responses) = server
                    .inner
                    .relay
                    .lock()
                    .map_err(|_| ())?
                    .revoke_tab(tab_id);
                server.dispatch(responses).await;
                return Ok(());
            }
            BrowserEvent::ActionStarted { .. }
            | BrowserEvent::ActionCompleted { .. }
            | BrowserEvent::ActionFailed { .. } => return Ok(()),
            BrowserEvent::RelayDisconnected { .. } => return Err(()),
        }
    }
    let request = serde_json::from_value::<CompanionControlRequest>(value).map_err(|_| ())?;
    let response = handle_control(server, request).await;
    server.send_json(&response);
    Ok(())
}

async fn handle_control(
    server: &CompanionServer,
    request: CompanionControlRequest,
) -> CompanionControlResponse {
    let request_id = request.request_id().to_owned();
    if request.protocol_version() != BROWSER_PROTOCOL_VERSION {
        return control_error(
            request_id,
            "protocol_mismatch",
            "unsupported control protocol",
        );
    }
    match request {
        CompanionControlRequest::WorkflowList { .. } => {
            match server.dispatch_list_workflows().await {
                Ok(workflows) => CompanionControlResponse::Workflows {
                    protocol_version: BROWSER_PROTOCOL_VERSION,
                    request_id,
                    workflows,
                },
                Err(error) => control_error(request_id, "workflow_list_failed", &error),
            }
        }
        CompanionControlRequest::WorkflowStart {
            workflow_id,
            tab_id,
            input,
            ..
        } => match server.dispatch_start_run(&workflow_id, tab_id, input).await {
            Ok(run_id) => control_ok(request_id, json!({"run_id":run_id})),
            Err(error) => control_error(request_id, "workflow_start_failed", &error),
        },
        CompanionControlRequest::WorkflowCancel { run_id, .. } => control_ok(
            request_id,
            json!({"cancelled":server.dispatch_cancel_run(&run_id).await}),
        ),
        CompanionControlRequest::RunSubscribe { run_id, .. } => {
            control_ok(request_id, json!({"subscribed":run_id}))
        }
        CompanionControlRequest::TabList { .. } => {
            let tabs = server
                .inner
                .relay
                .lock()
                .map(|relay| relay.tabs().list().into_iter().cloned().collect())
                .unwrap_or_default();
            CompanionControlResponse::Tabs {
                protocol_version: BROWSER_PROTOCOL_VERSION,
                request_id,
                tabs,
            }
        }
        CompanionControlRequest::ConnectionStatus { .. } => {
            let connected = server
                .inner
                .relay
                .lock()
                .map(|relay| relay.is_connected())
                .unwrap_or(false);
            CompanionControlResponse::Connection {
                protocol_version: BROWSER_PROTOCOL_VERSION,
                request_id,
                connected,
            }
        }
    }
}

pub(super) fn load_workflow(
    directory: &Path,
    id: &str,
) -> Result<WorkflowGraph, CompanionServerError> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CompanionServerError::Workflow("invalid workflow id".into()));
    }
    let path = directory.join(format!("{id}.json"));
    let source = std::fs::read_to_string(&path)
        .map_err(|error| CompanionServerError::Workflow(format!("{}: {error}", path.display())))?;
    serde_json::from_str(&source)
        .map_err(|error| CompanionServerError::Workflow(format!("{}: {error}", path.display())))
}

pub(super) fn list_workflows(
    directory: &Path,
) -> Result<Vec<WorkflowSummary>, CompanionServerError> {
    let entries = std::fs::read_dir(directory).map_err(|error| {
        CompanionServerError::Workflow(format!("{}: {error}", directory.display()))
    })?;
    let mut workflows = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| CompanionServerError::Workflow(error.to_string()))?
            .path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(id) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let graph = load_workflow(directory, id)?;
        workflows.push(WorkflowSummary {
            id: id.to_owned(),
            name: if graph.name.is_empty() {
                id.to_owned()
            } else {
                graph.name
            },
        });
    }
    workflows.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(workflows)
}

fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

pub(super) fn lock_error() -> CompanionServerError {
    CompanionServerError::Listener("companion state lock poisoned".into())
}

pub(super) fn browser_error(code: BrowserErrorCode, message: &str) -> BrowserError {
    BrowserError {
        code,
        message: message.to_owned(),
        details: None,
    }
}

fn control_ok(request_id: String, result: Value) -> CompanionControlResponse {
    CompanionControlResponse::Ok {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        request_id,
        result,
    }
}

fn control_error(request_id: String, code: &str, message: &str) -> CompanionControlResponse {
    CompanionControlResponse::Error {
        protocol_version: BROWSER_PROTOCOL_VERSION,
        request_id,
        code: code.to_owned(),
        message: message.to_owned(),
    }
}
