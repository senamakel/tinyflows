use std::sync::Mutex;

use serde_json::json;

use super::*;
use crate::error::EngineError;

struct RecordingRelay {
    requests: Mutex<Vec<BrowserRequest>>,
    response: Mutex<Option<std::result::Result<BrowserResponse, BrowserError>>>,
}

impl RecordingRelay {
    fn success(data: Value) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            response: Mutex::new(Some(Ok(BrowserResponse::Ok {
                protocol_version: BROWSER_PROTOCOL_VERSION,
                request_id: String::new(),
                result: super::super::protocol::BrowserResult { data },
            }))),
        })
    }
}

#[async_trait]
impl BrowserRelay for RecordingRelay {
    async fn execute(
        &self,
        request: BrowserRequest,
    ) -> std::result::Result<BrowserResponse, BrowserError> {
        self.requests.lock().unwrap().push(request.clone());
        let response = self.response.lock().unwrap().take().unwrap();
        match response {
            Ok(BrowserResponse::Ok {
                protocol_version,
                result,
                ..
            }) => Ok(BrowserResponse::Ok {
                protocol_version,
                request_id: request.request_id,
                result,
            }),
            other => other,
        }
    }
}

#[derive(Default)]
struct RecordingFallback {
    calls: Mutex<Vec<(String, Value, Option<String>)>>,
}

#[async_trait]
impl ToolInvoker for RecordingFallback {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> Result<Value> {
        self.calls
            .lock()
            .unwrap()
            .push((slug.to_owned(), args.clone(), conn.map(str::to_owned)));
        Ok(json!({"fallback": slug, "args": args}))
    }
}

#[tokio::test]
async fn chrome_invoker_requires_an_explicit_action() {
    let relay = RecordingRelay::success(Value::Null);
    let invoker = ChromeToolInvoker::new(relay, "run-1", 7);
    let error = invoker
        .invoke("browser", json!({"selector":"main"}), None)
        .await
        .expect_err("missing args.action must fail");
    assert!(
        matches!(error, EngineError::Capability(message) if message.starts_with("browser:invalid_request:"))
    );
}

#[tokio::test]
async fn chrome_invoker_rejects_invalid_semantics_before_relaying() {
    let relay = RecordingRelay::success(Value::Null);
    let invoker = ChromeToolInvoker::new(relay.clone(), "run-1", 7).with_timeout_ms(60_001);
    let error = invoker
        .invoke("browser", json!({"action":"click","selector":"#go"}), None)
        .await
        .expect_err("oversized timeout must fail");
    assert!(error.to_string().contains("timeout must be between"));
    assert!(relay.requests.lock().unwrap().is_empty());

    let relay = RecordingRelay::success(Value::Null);
    let invoker = ChromeToolInvoker::new(relay.clone(), "run-1", 7);
    let error = invoker
        .invoke(
            "browser",
            json!({"action":"open","url":"chrome://settings"}),
            None,
        )
        .await
        .expect_err("restricted URL must fail natively");
    assert!(error.to_string().contains("must use HTTP(S)"));
    assert!(relay.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn chrome_invoker_binds_run_tab_timeout_and_correlation() {
    let relay = RecordingRelay::success(json!({"title":"Example"}));
    let invoker = ChromeToolInvoker::new(relay.clone(), "run-42", 91).with_timeout_ms(1234);

    let output = invoker
        .invoke("browser", json!({"action":"get_title"}), None)
        .await
        .expect("browser action");
    assert_eq!(output, json!({"title":"Example"}));
    let requests = relay.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].protocol_version, BROWSER_PROTOCOL_VERSION);
    assert_eq!(requests[0].request_id, "run-42:1");
    assert_eq!(requests[0].run_id, "run-42");
    assert_eq!(requests[0].tab_id, 91);
    assert_eq!(requests[0].timeout_ms, 1234);
    assert_eq!(requests[0].action, BrowserAction::GetTitle);
}

#[tokio::test]
async fn router_delegates_non_browser_slug_args_and_connection_unchanged() {
    let relay = RecordingRelay::success(Value::Null);
    let browser = Arc::new(ChromeToolInvoker::new(relay.clone(), "run", 1));
    let fallback = Arc::new(RecordingFallback::default());
    let router = RoutingToolInvoker::new(browser, fallback.clone());
    let args = json!({"to":"person@example.com","body":"hello"});

    let output = router
        .invoke("gmail.send", args.clone(), Some("acct-9"))
        .await
        .expect("fallback action");
    assert_eq!(output["fallback"], "gmail.send");
    assert!(relay.requests.lock().unwrap().is_empty());
    assert_eq!(
        fallback.calls.lock().unwrap().as_slice(),
        &[("gmail.send".into(), args, Some("acct-9".into()))]
    );
}

#[tokio::test]
async fn stable_relay_error_code_reaches_engine_retry_surface() {
    let relay = Arc::new(RecordingRelay {
        requests: Mutex::new(Vec::new()),
        response: Mutex::new(Some(Err(BrowserError {
            code: BrowserErrorCode::TabRevoked,
            message: "user removed tab from TinyFlows group".into(),
            details: None,
        }))),
    });
    let invoker = ChromeToolInvoker::new(relay, "run", 1);

    let error = invoker
        .invoke("browser", json!({"action":"snapshot"}), None)
        .await
        .expect_err("revoked tab must fail closed");
    assert!(
        matches!(error, EngineError::Capability(message) if message.starts_with("browser:tab_revoked:"))
    );
}

#[tokio::test]
async fn mismatched_response_correlation_fails_closed() {
    struct WrongCorrelation;
    #[async_trait]
    impl BrowserRelay for WrongCorrelation {
        async fn execute(
            &self,
            _request: BrowserRequest,
        ) -> std::result::Result<BrowserResponse, BrowserError> {
            Ok(BrowserResponse::Ok {
                protocol_version: BROWSER_PROTOCOL_VERSION,
                request_id: "another-run:99".into(),
                result: super::super::protocol::BrowserResult { data: Value::Null },
            })
        }
    }

    let invoker = ChromeToolInvoker::new(Arc::new(WrongCorrelation), "run", 1);
    let error = invoker
        .invoke("browser", json!({"action":"get_url"}), None)
        .await
        .expect_err("cross-run response must fail");
    assert!(
        matches!(error, EngineError::Capability(message) if message.starts_with("browser:invalid_request:"))
    );
}
