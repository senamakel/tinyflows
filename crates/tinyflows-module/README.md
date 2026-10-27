# TinyFlows module

The compiled TinyBus module serves Authoring, Schedule, Catalog, Portability,
and Runs objects under the shared `ai.tinyhumans.tinyflows` bus name. Hosts
import the `tinyflows-bus` contract and call the compiled module. The module
manifest advertises each interface's exact member set. Runs owns run lifecycle;
its v2 `CapsHost` interface is the host-served callback contract used by the
module's run-capability adapter.

### CapsHost callbacks (contract v2)

The run manager supplies a `tinyflows_bus::runs::CapsHost` address and a
separate trusted `RunScope` to `caps::CapsHostPorts::capabilities`. The host
must create that callback object for one flow, run, approval policy and owner
scope. The module never chooses a default callback from graph or trigger JSON.
The adapter constructs state over `FlowStateDocuments` with namespace
`flow:<flow_id>` and resolves saved workflows from the same owner-scoped
catalog document port. `TaskRunner` remains module-side; spawned tool, HTTP and
child-workflow tasks run with the bundle's capabilities, and nested workflow
tasks reuse the runner through a weak reference.
An approval parked inside a child workflow fails the task explicitly instead
of returning partial output as success, because task tickets do not yet carry a
checkpoint/resume state and detached task tickets are process-local.

`AgentRun` returns an opaque handle. `AgentEvents` pages are contiguous from
sequence one and bounded to 256 entries; the accumulated transcript is bounded
to 10,000 entries. The typed terminal outcome may arrive on an empty final
page. Its settled host transcript is kept when present; progress entries fill
the transcript only when the terminal transcript is empty. Callback transport,
decode and cursor errors surface as capability failures, and approval errors
cannot become a pending review. Each `AgentRun` carries a module-generated
`client_request_id`; if the callback future exits before accepting a valid
terminal response, a bounded best-effort `AgentCancel` uses that same id to
cancel the individual host request.

Schedule owns claims, bounded dispatch, retries, timing, recurrence,
completion, validation, and cron persistence. The host provides context-bound
execution, delivery and direct flow-trigger callbacks through ScheduleHost.
`StartScheduler.storage` selects module-owned persistence. Without it, the
ScheduleHost must implement the v5 occurrence-lease callbacks. Both paths fail
closed when the backend lacks transactions or lease state is malformed. A
successful claim captures the exact job snapshot and a fencing receipt. The
runtime renews the lease while work is active, commits run history and schedule
completion against that receipt, and recovers expired claims in a fresh
process. Recovery is at least once: a crash after an external send can repeat
that effect, so the contract makes no exactly-once claim.

Due queries filter excluded identities before their batch limit. If a returned
job already has a live lease, the scheduler excludes it and fetches later due
jobs; it does not advance the occurrence or write a skipped run. Recurrences
advance only after terminal completion. Scheduled one-shots stay enabled while
leased and disable at terminal completion; successful `delete_after_run`
removes the completed incarnation. Manual Run Now preserves saved schedule
state.

`RunJobNow` force-claims the captured saved definition even when disabled or
scheduled in the future. It writes a queued history row before an accepted
reply, and returns a typed refusal when another run owns the lease. Its
blocking tool mode may select `delivery: "not_requested"` so the current tool
turn does not send into itself. Manual completion records the final result
without changing `next_run`, `enabled` or `delete_after_run`.

Delivery has two callbacks: `Send` performs the one actual send and returns its
status; `Deliver` receives a `DeliveryProjection` only after the module has
computed final success from execution, send status and best-effort policy.
`Deliver` is a terminal projection and must not send again. Flow triggers
receive the captured `CronJob`, not a flow id that would require a later lookup.
`Attempt.raw_detail` is opaque host classification data; retries preserve the
last non-empty detail and relay it unchanged without logging it.

A host-driven pass can set `StartScheduler.config.now` to an RFC3339 instant
with `manual_ticks: true`. Due selection, run timestamps, and the next recurring
occurrence all use that fixed instant, so tests can advance calendar time
without waiting or changing the machine clock. Start a new manual scheduler
for another instant. Retry sleeps still use Tokio timers. Omitting `now` keeps
the system clock; a fixed instant is refused for an automatic poll loop. This
option requires Schedule contract version 5.

Run history records the captured `job_created_at` with the dispatched job,
so a replacement job cannot receive the older run's history. The occurrence
lease receipt fences completion against a later claim or recovered occurrence.

`ScheduleStore` accepts one StoreRequest: `{storage, operation}`. Storage is
`{backend: {kind: "documents", callback: {destination, object_path}},
max_run_history: 50, max_tasks: 64}`. The host chooses the scoped document
port. The module runs CronDocuments over it and owns no storage of its own;
limits, migrations, validation, truncation, pruning and CAS stay module-owned.

Each operation has an `op` field and these arguments. Responses preserve
existing job/run JSON; mutation-only operations return null.

| op | Arguments | Response |
| --- | --- | --- |
| add | expression, command | job |
| add_shell | name (optional), schedule, command | job |
| add_agent | spec (AgentJobSpec JSON, including enabled and session_target) | job |
| add_flow | flow_id, schedule | job (idempotent per flow) |
| find_flow | flow_id | job or null |
| list | none | jobs |
| get | job_id | job |
| remove | job_id | null |
| clear | none | removed count |
| dedup | none | removed count |
| due | now (RFC3339) | jobs |
| update | job_id, patch (CronJobPatch JSON) | job |
| record_last_run | job_id, finished_at (RFC3339), success, output | null |
| reschedule | job, success, output | null |
| record_run | job_id, started_at, finished_at (RFC3339), status, output (optional), duration_ms, delivery_status (optional) | null |
| delete_queued_runs | job_id | removed count |
| list_runs | job_id, limit | runs |
| set_max_active_jobs | limit (number, or null to clear) | null |
| import_job | job (CronJob JSON) | `{id, written}` (an existing id is left untouched) |
| import_run | run (CronRun JSON) | written (bool) |

`tinyflows_bus::schedule::StoreOperation` is the typed form of `operation`; the
job, run, patch, spec, lease and delivery records (`Attempt`, `RunRecord`,
`Completion`, `OccurrenceClaim`, `DeliveryProjection`) are in
`tinyflows_bus::schedule`, so a host implements ScheduleHost and the document
port without redeclaring them.

### Catalog legacy import (contract v6)

`ImportLegacyBatch` accepts ordered batches of at most 64 raw records and 2 MiB
serialized request size through the supplied `CatalogStorage` callback. The
host reads SQLite and draft files; file paths and scope choices stay in the
host. The module performs typed decode and graph migration, then writes
definitions, revisions, runs, state, suggestions, drafts, checkpoints and
pending writes idempotently, returning per-kind tallies and bounded problems.
Hosts own the raw file readers and source-format policy; the module applies
already bounded records through the same semantic writer used by the catalog
callback.

The host runs every batch during exclusive migration startup, before run
execution or other writers can mutate that scope. Checkpoints use an
append-only sequence and are not atomically fenced against concurrent live
writes. TinyFlows retains the bounded semantic writer in the module, while
hosts own source-file discovery, decoding and compatibility policy.

### Pure members (retained from contract v3)

Each takes one JSON argument and returns JSON; failures are bus errors carrying
the author-facing message.

| Member | Argument | Reply |
| --- | --- | --- |
| NextRun | `{schedule, from}` | RFC3339 instant after `from` |
| ValidateSchedule | `{schedule, now, agent?}`; `agent` adds the 5 minute floor | null |
| RunsCloserThan | `{schedule, from, min_gap_ms}` | `TooFrequent` or null |
| NormalizeExpression | cron expression string | normalised expression |
| ScheduleCronExpression | schedule | expression string or null |
| DefaultDelivery | origin or null | `DeliveryConfig` (`origin` with an origin, else `proactive`) |
| CheckOriginRequirements | `{session_target, delivery, origin?}` | null |
| CheckPatch | `{job, patch}`; refuses re-targeting a flow job | null |
| TruncateCronOutput | output string | output bounded to 16 KiB |

A document callback serves `ai.tinyhumans.tinyflows.ScheduleDocuments.Document`
with a JSON request and returns `{value: JSON}` or `{error: {kind, message}}`.
The error kinds are conflict, already_exists, not_found, invalid_input,
serialization, unavailable or backend. Preserve conflict separately so module
CAS loops can retry. Port values use the stable `tinystoragedrivers-types`
serde contract.

The module first requests capabilities and requires a typed
`{value: {transactions: bool}}` reply; missing or invalid capability reports
fail initialization. Transaction support is advertised only when the scoped
backend provides it.

The request operations are capabilities, atomic_batch (ops), ensure (spec),
get (collection/id), put
(collection/id/doc/precondition), delete (collection/id/precondition), query
(collection/query), count (collection/filter) and delete_where
(collection/filter). Only cron_jobs, cron_runs, cron_counters and legacy
cron_job_policies are used. The host mechanically forwards these to its scoped
document port, without linking TinyFlows storage implementations. Atomic batches
use the storage API's `WriteOp` and `WriteResult` JSON and must be forwarded
as one backend transaction. Every collection in the batch is checked before
forwarding; unsupported transactions fail instead of falling back to individual
writes.

Configured scheduler storage applies claims/completions through upstream
transaction/CAS methods, preserving edited schedules and newer occurrences.
Legacy policy rows migrate into jobs before deletion. Stop joins the scheduler
and aborts/drains owned jobs before releasing claims.

Tests run the actual cdylib, verify both exact manifest method sets, exercise
Authoring and Schedule calls, persist through SQLite and a document callback,
and run a stored flow through direct trigger/delivery/history persistence:

```bash
bash scripts/verify-module-artifact.sh
```
# Portability service

The compiled module provides `ai.tinyhumans.tinyflows.Portability` contract v4
from `tinyflows-bus`. Hosts call it for HTTP credential rendering/injection,
credential-reference inspection and a method/URL summary, LLM and agent prompt
preparation/result formatting, case-insensitive app-event binding, and literal
pinned trigger inputs. Inspection accepts the
request but no resolved-credential argument. Its result omits request headers
and body, including any inline values in them; the summarized URL may itself
contain sensitive data. The credential name is the exact value parsed by the
TinyFlows helper; the module applies no additional suffix normalization. The
host continues to resolve and authorize credentials, then may apply its own
lookup policy before sending a resolved header through `PrepareHttpRequest` for
injection. The LLM/agent methods keep generic request normalization, prompt
composition, timeout precedence and structured-output formatting in the module;
the host retains provider selection and its execution policy.

