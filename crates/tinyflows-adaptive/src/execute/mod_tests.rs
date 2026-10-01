use super::*;

struct Git(&'static str);

#[async_trait]
impl Workspace for Git {
    async fn mark(&self) -> String {
        "abc123".into()
    }
    async fn changed_since(&self, mark: &str) -> String {
        format!("{} since {mark}", self.0)
    }
}

#[tokio::test]
async fn a_host_that_cannot_say_reports_nothing_rather_than_guessing() {
    let quiet = Unobserved;
    assert!(quiet.mark().await.is_empty());
    assert!(quiet.changed_since("").await.is_empty());
}

#[tokio::test]
async fn the_baseline_is_passed_back_to_the_comparison() {
    // The reason this is a trait and not a closure: the mark taken before
    // the run has to reach the reading taken after it.
    let git = Git("1 file changed");
    let mark = git.mark().await;
    assert_eq!(
        git.changed_since(&mark).await,
        "1 file changed since abc123"
    );
}

fn bare_graph() -> tinyflows::model::WorkflowGraph {
    tinyflows::model::WorkflowGraph {
        schema_version: 1,
        id: Some("g".into()),
        name: "g".into(),
        inputs: Vec::new(),
        agents: Vec::new(),
        nodes: Vec::new(),
        edges: Vec::new(),
    }
}

#[test]
fn a_failure_is_readable_as_evidence_not_as_an_absence() {
    let ran = RunReport {
        failed: Some("node 'fetch' timed out".into()),
        ..RunReport::default()
    }
    .into_ran(&bare_graph());
    let evidence = ran.evidence();
    assert_eq!(
        evidence.outcome.output["error"],
        json!("node 'fetch' timed out")
    );
    // No `nodes` key: what the mechanical missing-evidence check reads.
    assert!(evidence.outcome.output.get("nodes").is_none());
}

#[test]
fn an_unreported_run_does_not_claim_nothing_changed() {
    // The bug this exists to prevent. Empty `changed` plus no steps is
    // settled mechanically as MissingEvidence, which is terminal — so a
    // socket blip would end the episode for good. `ExternalWait` is
    // terminal too, so there is no safe blocker to pick; the fix is to stop
    // asserting a fact nobody established.
    let ran = unreported(&bare_graph(), "deadline elapsed after 600s");

    assert!(
        !ran.changed.is_empty(),
        "empty means the host looked and saw nothing; nobody looked"
    );
    assert!(ran.changed.contains("unknown"), "{}", ran.changed);
    assert!(
        ran.failed
            .as_deref()
            .unwrap_or_default()
            .contains("deadline"),
        "the transport's own words survive: {:?}",
        ran.failed
    );
}

#[test]
fn an_unreported_run_carries_no_invented_evidence() {
    let ran = unreported(&bare_graph(), "no runner connected");
    assert!(ran.steps.is_empty());
    assert!(ran.outcome.pending_approvals.is_empty());
    assert!(!ran.outcome.cancelled);
    assert!(ran.outcome.output.get("nodes").is_none());
}
