//! Building an [`ApprovalRequest`] from a node's config, and reading a
//! settled [`ApprovalDecision`] back out of a resume value, the run's
//! approvals list, or a finished review.
//!
//! Split out of `approval.rs` to keep that file under the repository's
//! line-length limit; request-building and decision-reading are one cohesive
//! concern (every function here is pure data shaping, none of it waits or
//! calls a capability), so they belong together rather than split further.

use serde_json::{Value, json};

use crate::caps::{ApprovalDecision, ApprovalRequest, ApprovalSubject};
use crate::data::Item;
use crate::error::{EngineError, Result};
use crate::nodes::NodeContext;

/// The default rendering hint when the graph does not say what the subject is.
const DEFAULT_SUBJECT_KIND: &str = "json";

/// The run's host id, when the state carries one under any of the spellings a
/// host might seed (`run.id`, `run.run_id`, `run.trigger.run_id`).
///
/// **Security note for hosts:** whichever of these is populated becomes part
/// of `request_id`, the provider's create-or-fetch key. `run.trigger.run_id`
/// in particular is read out of the same trigger payload a caller supplies to
/// `engine::run` — for a webhook- or user-facing trigger, that payload can be
/// attacker-influenced. A host that lets untrusted input reach this field
/// lets an attacker choose a `request_id` that collides with an earlier run's
/// and inherit its cached decision, approving or rejecting a new, unreviewed
/// subject without a human ever seeing it. Seed a **server-generated** run id
/// here (or set `config.request_id` explicitly from one) — never forward a
/// caller-supplied field into it unvalidated. This crate is host-agnostic and
/// cannot tell trusted trigger data from untrusted; enforcing that boundary is
/// the host's responsibility, the same as it is for any other identity used as
/// a de-duplication or idempotency key.
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
pub(super) fn build_request(ctx: &NodeContext<'_>, config: &Value) -> Result<ApprovalRequest> {
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

    // `validate::validate_all` only sees `assignees` as authored: a literal
    // non-array (a bare string, the natural single-reviewer mistake) or a
    // literal empty array are both refused there. An `=`-bound `assignees`
    // (e.g. `"=item.reviewers"`) is a string at author time — it passes that
    // check by looking like *some* other field entirely — and resolves to
    // its real shape only here, at execution time. So the same two refusals
    // apply again to the resolved value: present and not an array, or
    // present, an array, and empty (or empty of strings) once resolved. Both
    // reach the same nobody-reviews-this audience a validated graph should
    // never produce.
    let assignees = match config.get("assignees") {
        None => Vec::new(),
        Some(value) => match value.as_array() {
            None => {
                return Err(EngineError::Capability(format!(
                    "approval node {:?}: `assignees` resolved to {value}, not an array of strings",
                    ctx.node.id
                )));
            }
            Some(values) => {
                let assignees: Vec<String> = values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                if assignees.is_empty() {
                    return Err(EngineError::Capability(format!(
                        "approval node {:?}: `assignees` resolved to an empty array; a review \
                         with nobody assigned can never be resolved",
                        ctx.node.id
                    )));
                }
                assignees
            }
        },
    };

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
        assignees,
        metadata: config.get("metadata").cloned().unwrap_or(Value::Null),
    })
}

/// A config field read as a string, ignoring a non-string (an unresolved
/// expression that came back `null`, say).
fn string_field(config: &Value, key: &str) -> Option<String> {
    config.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Whether `list` (an array of strings) names this node or its review.
pub(super) fn names(list: Option<&Value>, request: &ApprovalRequest) -> bool {
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
///
/// The array forms above are always scoped to this request via [`names`], the
/// same way [`super::gate`](crate::nodes::integration::gate) scopes its own
/// `approved` array — required, because several nodes can be interrupted at
/// once and a resume value is not addressed to just one of them. The inline
/// verdict-object form carries no such array to check, so it is accepted
/// unscoped **only** when it does not itself name a different request —
/// matching the same "single-interrupt convenience" precedent
/// `engine::build::activation`'s bare `Value::Bool(true)` case documents, but
/// without silently absorbing a verdict a host explicitly addressed elsewhere.
pub(super) fn decision_from_resume(
    resume: &Value,
    request: &ApprovalRequest,
) -> Option<ApprovalDecision> {
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
    if let Some(named) = verdict
        .get("node_id")
        .or_else(|| verdict.get("request_id"))
        .and_then(Value::as_str)
    {
        if named != request.node_id && named != request.request_id {
            // Explicitly addressed to a different node's review; not ours to take.
            return None;
        }
    }
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
pub(super) fn delivered(
    ctx: &NodeContext<'_>,
    request: &ApprovalRequest,
) -> Option<ApprovalDecision> {
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
pub(super) fn decided_item(
    request: &ApprovalRequest,
    decision: &ApprovalDecision,
    input: Value,
) -> Item {
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
pub(super) fn decision_meta(decision: &ApprovalDecision, request: &ApprovalRequest) -> Value {
    json!({
        "decision": {
            "approved": decision.approved,
            "decided_by": decision.decided_by,
            "comment": decision.comment,
            "request_id": request.request_id,
        }
    })
}
