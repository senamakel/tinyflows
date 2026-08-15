//! The `approval` node: put something in front of a human, route on their answer.
//!
//! A workflow that posts, pays, emails, or deletes wants a person in the loop at
//! exactly one point, holding exactly one thing — a URL to look at, a draft to
//! read, a payload to sign off. That is what this node is: it carries the
//! **subject** of the review, hands it to the host's review surface through the
//! [`ApprovalProvider`](crate::caps::ApprovalProvider) capability, and emits the
//! verdict on its `approved` / `rejected` ports as ordinary data the graph can
//! branch on.
//!
//! # Not the same thing as `requires_approval`
//!
//! The `requires_approval` flag gates a node that would otherwise run: it says
//! "don't execute this until someone says go", carries nothing, and its answer
//! is a yes/no the graph cannot inspect. This node is the review *itself* —
//! addressable, with a subject, a reviewer, a comment, and a rejection branch.
//! Use the flag to hold back a dangerous node; use this kind when the decision
//! is a step in the workflow.
//!
//! # How waiting works
//!
//! Same two shapes as [`gate`](super::gate), and for the same reasons. A human
//! review is measured in minutes-to-days, so `wait_mode: "suspend"` (the
//! **default** here, unlike `gate`) interrupts the run: nothing is burned while
//! the card sits in someone's queue, and the host resumes the run when they
//! answer. `wait_mode: "poll"` re-activates the node on an interval instead,
//! which is right only when the decision is expected within seconds — each poll
//! costs a super-step and a node visit against the run's budgets, so the poll
//! count is bounded here rather than left to the run-level backstop.
//!
//! # Where a decision can come from
//!
//! In priority order, because more than one channel can be live at once:
//!
//! 1. **The resume value** ([`NodeContext::resume`]) — the checkpointed-resume
//!    path, which replays from the checkpoint rather than re-running with a
//!    merged run input, so it is the *only* channel there. A rejection in the
//!    engine's `{"rejected": [<node id>]}` shape wins over everything, matching
//!    how a `requires_approval` gate treats a denial.
//! 2. **The run's approvals list** (`run.trigger.approvals`) — the
//!    re-execute resume path, and how `engine::resume` has always delivered an
//!    approval. Listing this node's id there approves it.
//! 3. **The host's [`ApprovalProvider`]** — asked once per activation, under
//!    the create-or-fetch contract documented on the trait.
//! 4. **Nobody**, on a host that wired no provider: the node simply waits, so
//!    it reduces to a pause the host settles through `engine::resume`.

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::caps::{ApprovalDecision, ApprovalOutcome, ApprovalRequest, ApprovalSubject};
use crate::data::Item;
use crate::error::{EngineError, Result};
use crate::nodes::{NodeContext, NodeExecutor, NodeOutput, resolve_config_traced};

/// Default gap between polls, in milliseconds. A second, not the `gate`'s 250ms:
/// nothing a human does resolves faster than that.
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

/// Default ceiling on polls before the wait budget is called spent. With the
/// default interval that is a minute of waiting — deliberately short, because
/// polling is the wrong mode for a long review and the timeout should say so
/// rather than quietly spending a run's whole visit budget.
const DEFAULT_MAX_POLLS: u64 = 60;

/// The slot key the poll count is recorded under, so it survives a checkpoint
/// the way a `loop` node's iteration does.
const POLLS_KEY: &str = "polls";

/// The default rendering hint when the graph does not say what the subject is.
const DEFAULT_SUBJECT_KIND: &str = "json";

/// How the node waits for a human.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitMode {
    /// Interrupt the run; the host resumes it when the answer lands. The
    /// default — a review is a human-timescale wait.
    Suspend,
    /// Re-activate on an interval and ask again. Only sane for a decision
    /// expected within seconds.
    Poll,
}

impl WaitMode {
    fn from_config(config: &Value) -> Self {
        match config.get("wait_mode").and_then(Value::as_str) {
            Some("poll") => Self::Poll,
            _ => Self::Suspend,
        }
    }
}

/// What a rejection does to the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnReject {
    /// Emit the verdict on the `rejected` port (default), so a graph can run a
    /// recovery branch — notify the author, revise, ask again.
    Route,
    /// Fail the node, letting the ordinary `on_error` policy take over.
    Error,
    /// Emit nothing and let the branch end here.
    Drop,
}

impl OnReject {
    fn from_config(config: &Value) -> Self {
        match config.get("on_reject").and_then(Value::as_str) {
            Some("error") => Self::Error,
            Some("drop") => Self::Drop,
            _ => Self::Route,
        }
    }
}

/// What a spent poll budget does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnTimeout {
    /// Fail the node (default): nobody answered, and pretending otherwise is
    /// how an unreviewed payload ships.
    Error,
    /// Treat silence as a rejection and follow the `on_reject` policy.
    Reject,
    /// Emit on the `timeout` port, so escalation is its own branch.
    Route,
}

impl OnTimeout {
    fn from_config(config: &Value) -> Self {
        match config.get("on_timeout").and_then(Value::as_str) {
            Some("reject") => Self::Reject,
            Some("route") => Self::Route,
            _ => Self::Error,
        }
    }
}

/// Presents a subject to a human and routes on approve / reject.
#[derive(Debug, Default, Clone)]
pub struct ApprovalNode;

/// Reads a positive integer config field, falling back to `default`.
fn positive_u64(config: &Value, key: &str, default: u64) -> u64 {
    config
        .get(key)
        .and_then(Value::as_u64)
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// The run's host id, when the state carries one under any of the spellings a
/// host might seed (`run.id`, `run.run_id`, `run.trigger.run_id`).
fn run_id(ctx: &NodeContext<'_>) -> Option<String> {
    ["id", "run_id"]
        .iter()
        .find_map(|key| ctx.run.get(*key).and_then(Value::as_str))
        .or_else(|| {
            ctx.run
                .get("trigger")
                .and_then(|t| t.get("run_id"))
                .and_then(Value::as_str)
        })
        .map(str::to_string)
}

/// Builds the review request from the node's resolved config.
///
/// The `request_id` is what makes the provider's create-or-fetch contract
/// work, so it must be **stable across activations**: an interrupt discards the
/// activation's state update, so the node cannot remember an id it generated,
/// and anything derived from the clock or a counter would create a fresh review
/// on every resume. Hence run id + node id, or an explicit `config.request_id`
/// for a host that wants to key reviews its own way.
///
/// Falling back to the bare node id when *neither* is available would let two
/// different runs of the same graph collide on the same `request_id`: since
/// [`ApprovalProvider::decide`](crate::caps::ApprovalProvider::decide) is
/// create-or-fetch, a later run would silently inherit an earlier run's
/// decision and route an unreviewed subject straight through `approved`. So a
/// node with no `config.request_id` and no run-scoped identity is a
/// configuration error, not a degraded default.
fn build_request(ctx: &NodeContext<'_>, config: &Value) -> Result<ApprovalRequest> {
    let run = run_id(ctx);
    let request_id = match config.get("request_id").and_then(Value::as_str) {
        Some(explicit) => explicit.to_string(),
        None => match &run {
            Some(run) => format!("{run}:{}", ctx.node.id),
            None => {
                return Err(EngineError::Capability(format!(
                    "approval node {:?}: no `request_id` configured and no run-scoped identity \
                     available (expected `run.id`, `run.run_id`, or `run.trigger.run_id`); set \
                     `config.request_id` explicitly or seed a run id, otherwise later runs could \
                     reuse an earlier run's decision",
                    ctx.node.id
                )));
            }
        },
    };

    // The subject defaults to the item that arrived, which is the common case:
    // a node upstream produced the thing, and the human looks at it.
    let value = config
        .get("subject")
        .cloned()
        .or_else(|| ctx.input.first().map(|item| item.json.clone()))
        .unwrap_or(Value::Null);

    Ok(ApprovalRequest {
        request_id,
        node_id: ctx.node.id.clone(),
        run_id: run,
        title: string_field(config, "title"),
        prompt: string_field(config, "prompt"),
        subject: ApprovalSubject {
            kind: config
                .get("subject_kind")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_SUBJECT_KIND)
                .to_string(),
            value,
        },
        assignees: config
            .get("assignees")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        metadata: config.get("metadata").cloned().unwrap_or(Value::Null),
    })
}

/// A config field read as a string, ignoring a non-string (an unresolved
/// expression that came back `null`, say).
fn string_field(config: &Value, key: &str) -> Option<String> {
    config.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Whether `list` (an array of strings) names this node or its review.
fn names(list: Option<&Value>, request: &ApprovalRequest) -> bool {
    list.and_then(Value::as_array).is_some_and(|ids| {
        ids.iter()
            .filter_map(Value::as_str)
            .any(|id| id == request.node_id || id == request.request_id)
    })
}

/// Reads a decision out of a resume value, if it carries one.
///
/// Accepts the engine's own `{"rejected": [<id>…]}` denial shape (checked
/// first, so a denial always beats an approval delivered in the same value),
/// the mirror `{"approved": [<id>…]}`, and a full verdict object — either
/// inline or nested under `decision`.
fn decision_from_resume(resume: &Value, request: &ApprovalRequest) -> Option<ApprovalDecision> {
    if names(resume.get("rejected"), request) {
        return Some(ApprovalDecision::rejected(
            resume
                .get("comment")
                .and_then(Value::as_str)
                .map(str::to_string),
        ));
    }
    if names(resume.get("approved"), request) {
        return Some(ApprovalDecision::approved());
    }

    let verdict = resume.get("decision").unwrap_or(resume);
    let approved = verdict.get("approved").and_then(Value::as_bool)?;
    Some(ApprovalDecision {
        approved,
        decided_by: string_field(verdict, "decided_by"),
        comment: string_field(verdict, "comment"),
        payload: verdict.get("payload").cloned(),
    })
}

/// The decision already in hand before the host is asked: a resume value, or
/// this node's id on the run's approvals list.
fn delivered(ctx: &NodeContext<'_>, request: &ApprovalRequest) -> Option<ApprovalDecision> {
    if let Some(decision) = ctx
        .resume
        .as_ref()
        .and_then(|resume| decision_from_resume(resume, request))
    {
        return Some(decision);
    }

    // The re-execute resume path: `engine::resume` merges newly-approved ids
    // into the run input, where they arrive as `run.trigger.approvals`. The
    // top-level `run.approvals` is the same list seeded through the explicit
    // channel; read both, because which one carries the id depends on how the
    // host started the run.
    let trigger_approvals = ctx.run.get("trigger").and_then(|t| t.get("approvals"));
    if names(trigger_approvals, request) || names(ctx.run.get("approvals"), request) {
        return Some(ApprovalDecision::approved());
    }
    None
}

/// The item a settled review emits.
///
/// `subject` is what the human actually signed off on — their edit when the
/// host's surface allowed one, otherwise exactly what was sent — so a
/// downstream node reads one field regardless. The original input is kept under
/// `input` so nothing is lost when the subject was a projection of it.
fn decided_item(request: &ApprovalRequest, decision: &ApprovalDecision, input: Value) -> Item {
    Item::new(json!({
        "approved": decision.approved,
        "subject": decision
            .payload
            .clone()
            .unwrap_or_else(|| request.subject.value.clone()),
        "subject_kind": request.subject.kind,
        "edited": decision.payload.is_some(),
        "decided_by": decision.decided_by,
        "comment": decision.comment,
        "request_id": request.request_id,
        "input": input,
    }))
}

/// The slot state a settled review records, so `=nodes.<id>.decision.approved`
/// resolves from anywhere in the graph — including from a branch that did not
/// receive the emitted item (a `drop`ped rejection has no item at all).
fn decision_meta(decision: &ApprovalDecision, request: &ApprovalRequest) -> Value {
    json!({
        "decision": {
            "approved": decision.approved,
            "decided_by": decision.decided_by,
            "comment": decision.comment,
            "request_id": request.request_id,
        }
    })
}

#[async_trait]
impl NodeExecutor for ApprovalNode {
    async fn execute(&self, ctx: NodeContext<'_>) -> Result<NodeOutput> {
        let (config, diagnostics) = resolve_config_traced(&ctx);
        let request = build_request(&ctx, &config)?;
        let input = ctx
            .input
            .first()
            .map(|item| item.json.clone())
            .unwrap_or(Value::Null);

        // Ask the host only when nothing has already settled this review: a
        // resume or a listed approval is the answer, and re-asking would put a
        // decided review back in front of the provider.
        let outcome = match delivered(&ctx, &request) {
            Some(decision) => {
                // A provider may have a card open for this review from an
                // earlier activation's `decide` call (it went `Pending`, or
                // this run never asked because the answer already arrived some
                // other way). Either way nobody is waiting on the provider's
                // card any more, so withdraw it rather than leave a stale entry
                // in the host's queue. Best-effort: the run has already decided
                // what to do, and a failed withdrawal must not change that.
                if let Some(provider) = ctx.caps.approvals.as_ref() {
                    if let Err(err) = provider
                        .cancel(&request.request_id, "resolved via resume")
                        .await
                    {
                        tracing::warn!(
                            node = %ctx.node.id,
                            request = %request.request_id,
                            error = %err,
                            "withdrawing the provider's review after a resume decision failed"
                        );
                    }
                }
                ApprovalOutcome::Decided(decision)
            }
            None => match ctx.caps.approvals.as_ref() {
                Some(provider) => provider.decide(&request).await?,
                // No provider: the node is a pause the host settles out of band
                // through `engine::resume`, which is exactly what waiting does.
                None => ApprovalOutcome::Pending,
            },
        };

        let decision = match outcome {
            ApprovalOutcome::Decided(decision) => decision,
            ApprovalOutcome::Pending => return self.wait(&ctx, &config, &request).await,
        };

        tracing::info!(
            node = %ctx.node.id,
            request = %request.request_id,
            approved = decision.approved,
            "approval decided"
        );

        let meta = decision_meta(&decision, &request);
        if decision.approved {
            return Ok(NodeOutput::routed(
                vec![decided_item(&request, &decision, input)],
                "approved",
            )
            .with_meta(meta)
            .with_diagnostics(diagnostics));
        }

        Ok(match OnReject::from_config(&config) {
            OnReject::Route => {
                NodeOutput::routed(vec![decided_item(&request, &decision, input)], "rejected")
                    .with_meta(meta)
                    .with_diagnostics(diagnostics)
            }
            OnReject::Drop => NodeOutput::empty()
                .with_meta(meta)
                .with_diagnostics(diagnostics),
            OnReject::Error => {
                return Err(EngineError::Capability(format!(
                    "approval node {:?}: rejected by {}{}",
                    ctx.node.id,
                    decision.decided_by.as_deref().unwrap_or("a reviewer"),
                    decision
                        .comment
                        .as_deref()
                        .map(|c| format!(" ({c})"))
                        .unwrap_or_default(),
                )));
            }
        })
    }
}

impl ApprovalNode {
    /// Nobody has decided yet: suspend the run, or spend a poll.
    async fn wait(
        &self,
        ctx: &NodeContext<'_>,
        config: &Value,
        request: &ApprovalRequest,
    ) -> Result<NodeOutput> {
        if WaitMode::from_config(config) == WaitMode::Suspend {
            // Suspending discards this activation's update, so no poll is
            // charged — a suspended review is not spending a budget, it is
            // waiting for a person. The payload is what the host renders or
            // routes if it did not get the request through a provider.
            return Ok(NodeOutput::interrupt(
                ctx.node.id.clone(),
                json!({
                    "kind": "approval",
                    "node": ctx.node.id,
                    "request_id": request.request_id,
                    "title": request.title,
                    "prompt": request.prompt,
                    "subject": request.subject.value,
                    "subject_kind": request.subject.kind,
                    "assignees": request.assignees,
                    "metadata": request.metadata,
                }),
            ));
        }

        let polls = ctx
            .nodes
            .get(&ctx.node.id)
            .and_then(|slot| slot.get(POLLS_KEY))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let max_polls = positive_u64(config, "max_polls", DEFAULT_MAX_POLLS);
        let meta = json!({ POLLS_KEY: polls + 1, "request_id": request.request_id });

        if polls < max_polls {
            let interval = positive_u64(config, "poll_interval_ms", DEFAULT_POLL_INTERVAL_MS);
            return Ok(NodeOutput::reenter_after(interval, meta));
        }

        // Budget spent. Whatever happens next, nobody is waiting on this review
        // any more, so withdraw it rather than leaving a dead card in a queue.
        // Best-effort: the run has already decided what to do, and a failed
        // withdrawal must not change that.
        if let Some(provider) = ctx.caps.approvals.as_ref() {
            if let Err(err) = provider
                .cancel(&request.request_id, "approval node timed out")
                .await
            {
                tracing::warn!(
                    node = %ctx.node.id,
                    request = %request.request_id,
                    error = %err,
                    "withdrawing the timed-out review failed"
                );
            }
        }

        // The `=nodes.<id>.decision.approved` contract other settled reviews
        // give the graph applies here too: a downstream guard reading it after
        // a timeout must see `false`, not an absent value, and a `rejected`
        // recovery branch reading `=item.comment` / `=item.input` must not get
        // `null` back just because the review timed out rather than being
        // actively declined.
        let comment = format!("no decision after {max_polls} polls");
        let timed_out = json!({
            "approved": false,
            "timed_out": true,
            "request_id": request.request_id,
            "subject": request.subject.value,
            "subject_kind": request.subject.kind,
            "edited": false,
            "decided_by": Value::Null,
            "comment": comment,
            "input": ctx.input.first().map(|item| item.json.clone()).unwrap_or(Value::Null),
        });
        let meta = json!({
            POLLS_KEY: polls + 1,
            "request_id": request.request_id,
            "decision": {
                "approved": false,
                "timed_out": true,
                "decided_by": Value::Null,
                "comment": comment,
                "request_id": request.request_id,
            }
        });

        match OnTimeout::from_config(config) {
            OnTimeout::Error => Err(EngineError::Capability(format!(
                "approval node {:?}: no decision after {max_polls} polls; raise \
                 `max_polls`/`poll_interval_ms`, switch to `wait_mode: \"suspend\"`, or wire a \
                 `timeout` port",
                ctx.node.id
            ))),
            OnTimeout::Route => {
                Ok(NodeOutput::routed(vec![Item::new(timed_out)], "timeout").with_meta(meta))
            }
            OnTimeout::Reject => match OnReject::from_config(config) {
                OnReject::Route => {
                    Ok(NodeOutput::routed(vec![Item::new(timed_out)], "rejected").with_meta(meta))
                }
                OnReject::Drop => Ok(NodeOutput::empty().with_meta(meta)),
                OnReject::Error => Err(EngineError::Capability(format!(
                    "approval node {:?}: no decision after {max_polls} polls, and \
                     `on_timeout: \"reject\"` with `on_reject: \"error\"` fails the node",
                    ctx.node.id
                ))),
            },
        }
    }
}

#[cfg(test)]
#[path = "approval_tests.rs"]
mod tests;
