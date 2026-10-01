use super::*;
use serde_json::json;

fn candidate(id: &str, applied: u32, helped: u32) -> Candidate {
    Candidate {
        id: id.to_string(),
        name: format!("the {id} workflow"),
        description: "reviews a closed issue end to end".to_string(),
        node_count: 4,
        applied,
        helped,
        inputs: vec![("repo".to_string(), true)],
    }
}

#[test]
fn a_listing_shows_both_counters_not_a_rate() {
    let rendered = candidate("pr-review", 40, 30).render();
    assert!(rendered.contains("run 40×, satisfied 30×"), "{rendered}");
    assert!(!rendered.contains("75"), "a rate hides the sample size");
}

#[test]
fn a_workflow_that_has_never_run_says_so_rather_than_showing_zeroes() {
    let rendered = candidate("fresh", 0, 0).render();
    assert!(rendered.contains("never run"), "{rendered}");
}

#[test]
fn a_workflow_with_no_description_says_it_cannot_be_chosen_on_purpose() {
    let mut c = candidate("bare", 0, 0);
    c.description = String::new();
    assert!(c.render().contains("nobody can choose this on purpose"));
}

#[test]
fn a_blank_name_falls_back_to_the_id() {
    let mut c = candidate("only-an-id", 1, 1);
    c.name = String::new();
    assert!(c.render().contains("name: only-an-id"));
}

/// A model that answers with `reply` and records what it was shown.
///
/// The call count is the point of several tests below: whether `select`
/// asks at all is a cost decision, and asserting on the *answer* would not
/// notice a version that skipped the call and returned the same thing.
struct Scripted {
    reply: Value,
    asked: std::sync::Mutex<Vec<String>>,
}

impl Scripted {
    fn new(reply: Value) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            reply,
            asked: std::sync::Mutex::new(Vec::new()),
        })
    }
    fn calls(&self) -> usize {
        self.asked.lock().expect("log").len()
    }
    fn last(&self) -> String {
        self.asked
            .lock()
            .expect("log")
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl tinyflows::caps::LlmProvider for Scripted {
    async fn complete(
        &self,
        request: Value,
        _conn: Option<&str>,
    ) -> tinyflows::error::Result<Value> {
        self.asked.lock().expect("log").push(
            request["messages"][1]["content"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        );
        Ok(self.reply.clone())
    }
}

async fn choose(
    provider: &std::sync::Arc<Scripted>,
    candidates: &[Candidate],
    errand_allowed: bool,
) -> Option<Attempt> {
    let caps = Capabilities {
        llm: provider.clone(),
        ..tinyflows::caps::mock::mock_capabilities()
    };
    select(
        &Goal::new("how much disk is this directory using"),
        candidates,
        "",
        errand_allowed,
        &caps,
        None,
    )
    .await
    .expect("selection answers")
}

#[tokio::test]
async fn an_errand_answer_becomes_an_errand_approach() {
    let provider = Scripted::new(json!({
        "workflow_id": null,
        "errand": true,
        "why": "one turn of work, no procedure in it",
    }));
    let chosen = choose(&provider, &[candidate("pr-review", 4, 4)], true)
        .await
        .expect("an errand is an answer, not a decline");
    match chosen.approach {
        Approach::Errand { why } => assert!(why.contains("one turn")),
        other => panic!("expected an errand, got {other:?}"),
    }
}

#[tokio::test]
async fn an_empty_shelf_is_still_asked_when_an_errand_is_possible() {
    // The bug this exists to prevent, and it would have been invisible: a
    // cold store is exactly where a trivial goal is most likely, so the old
    // unconditional short-circuit made the errand path unreachable at the
    // one moment it pays for itself — while every test of the *answer*
    // still passed.
    let provider = Scripted::new(json!({ "workflow_id": null, "errand": true, "why": "trivial" }));
    let chosen = choose(&provider, &[], true).await;
    assert_eq!(provider.calls(), 1, "an empty shelf must still be asked");
    assert!(matches!(
        chosen.map(|a| a.approach),
        Some(Approach::Errand { .. })
    ));
}

#[tokio::test]
async fn an_empty_shelf_with_no_errand_left_is_not_asked_at_all() {
    // The other half: with nothing to choose from and no errand to offer,
    // the answer can only be "none", and the call is pure cost.
    let provider = Scripted::new(json!({ "workflow_id": null, "errand": true, "why": "x" }));
    assert!(choose(&provider, &[], false).await.is_none());
    assert_eq!(provider.calls(), 0, "nothing to ask about");
}

#[tokio::test]
async fn a_spent_errand_is_refused_even_when_the_model_asks_for_one() {
    // Prompt-only enforcement is not enforcement: attempt three is exactly
    // where a model talks itself back into the answer that needs no inputs.
    let provider = Scripted::new(json!({ "workflow_id": null, "errand": true, "why": "again" }));
    let chosen = choose(&provider, &[candidate("pr-review", 4, 4)], false).await;
    assert!(chosen.is_none(), "a spent errand reads as a decline");
    assert!(
        provider.last().contains("already spent its errand"),
        "and the prompt says why: {}",
        provider.last()
    );
}

#[tokio::test]
async fn naming_a_workflow_wins_over_also_setting_the_errand_flag() {
    // A contradictory answer taken at its more specific — and more easily
    // checked — word, rather than at whichever field is read first.
    let provider = Scripted::new(json!({
        "workflow_id": "pr-review",
        "errand": true,
        "why": "both",
        "inputs": { "repo": "openhuman" },
    }));
    let chosen = choose(&provider, &[candidate("pr-review", 4, 4)], true)
        .await
        .expect("an answer");
    assert!(matches!(chosen.approach, Approach::Selected { .. }));
}

#[test]
fn the_prompt_refuses_to_treat_a_short_goal_as_an_errand() {
    // The distinction the whole triage turns on. A one-step procedure can
    // be the most reused thing in the store, so brevity must not be the
    // test — if this guidance goes, the flag starts eating the shelf.
    assert!(SYSTEM.contains("no procedure in the goal"));
    assert!(SYSTEM.contains("Short is not the test"));
    assert!(SYSTEM.contains("not an escape from a hard goal"));
}

#[test]
fn the_prompt_tells_the_model_that_declining_is_allowed() {
    // The single most important line in it: a model pushed to always pick
    // will pick the nearest thing, and a near miss runs to completion.
    assert!(SYSTEM.contains("or null"));
    assert!(SYSTEM.contains("worse than none"));
}
