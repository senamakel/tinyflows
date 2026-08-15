# Testing and Debugging

Behind the default-off `testkit` feature. Adds no dependencies.

```toml
tinyflows = { version = "0.8", features = ["testkit"] }
```

## The problem

A workflow that runs is not a workflow that works. The engine will happily
execute a graph whose every binding resolved to `null`, whose `agent` node
dispatched a session with an empty prompt, and whose failure was swallowed by an
`on_error` policy — and report all of it as success. Each of those is a *legal
value*, not an error.

So the output of a broken run looks exactly like the output of a correct one:
an object, no errors, every node green. What was missing from the crate was not
execution. It was the means to interrogate an execution.

## The four layers

Each is usable on its own; the harness and the tool surface just wire them
together.

| Module | What it gives you |
| --- | --- |
| `testkit::mocks` | Programmable capability doubles with a call log |
| `testkit::trace` | What each node received, produced, and bound to |
| `testkit::harness` | `TestHarness` and named assertions |
| `testkit::debug` | Breakpoints: pause, inspect, override, step |
| `testkit::tools` | All of the above as agent-callable tools |

## Mocks

```rust
use tinyflows::testkit::{MockCaps, Respond};

let mocks = MockCaps::new()
    .on_tool("slack.send", Respond::value(json!({ "ok": true })))
    .on_tool("gh.issues.*", Respond::sequence([
        Respond::error("429 rate limited"),
        Respond::value(json!({ "number": 7 })),
    ]))
    .on_http("https://*/webhook", Respond::error("connection refused"))
    .on_tool("svc.do", Respond::value(json!("stubbed"))).only_from("node_a");
```

Rules are first-match-wins in declaration order, so a specific rule written
before a general one shadows it. `*` globs. A call matching no rule falls back
to the same echo `caps::mock` gives — a graph under test never fails because a
capability was left unprogrammed.

Other responses: `Respond::schema(…)` synthesizes a value satisfying a JSON
Schema (so a node declaring an `output_parser.schema` does not fail validation
for a reason unrelated to the graph), `Respond::after(duration, …)` exercises
timeouts, and a sequence past its end repeats its last entry rather than
falling through to something the author never wrote.

Every call lands in one log across *all* capabilities, so the ordering is real,
and each is attributed to the node that made it.

## Trace

```rust
let trace = run.trace();
trace.steps_for("send_email");   // every activation, loops and lanes included
trace.calls_from("send_email");  // what it asked the outside world for
trace.null_bindings();           // (node, binding) for every empty one
trace.failed();                  // the steps that errored
```

A `BindingTrace` carries `location`, `expression`, the `value` it resolved to,
`is_null`, and `reads_from` — the upstream node the expression was reading
from. That last field is the point: it turns "it produced null" into a pointer
at the node that should have produced the value.

## Harness

```rust
let run = TestHarness::new(&graph)
    .trigger(json!({ "repo": "acme/api" }))
    .input("branch", json!("main"))
    .mock_tool("slack.send", Respond::value(json!({ "ok": true })))
    .approve("review_gate")
    .run()
    .await?;

run.assert_completed();
run.assert_node_ran("send_email");
run.assert_node_skipped("escalate");
run.assert_no_null_bindings();
run.assert_clean_diagnosis();
run.assert_call_count("tools", Some("slack.send"), 1);
```

`assert_no_null_bindings` is the one worth reaching for first — it is the check
that a green run hides. `assert_clean_diagnosis` is stricter still: it also
catches agent nodes that would dispatch with an empty prompt and failures an
`on_error` policy swallowed.

`assert_node_ran` is worth asserting explicitly: a node a condition routed past
leaves no step behind, so a graph half of which never executed still reports a
clean outcome.

## Breakpoints

```rust
let mut session = DebugSession::start_quiet(compiled, json!({}), caps)?;
session.controller().set_breakpoint(BreakpointSpec::before("send_email"))?;
session.controller().set_breakpoint(BreakpointSpec::on_error())?;
session.controller().set_breakpoint(
    BreakpointSpec::before("retry_body").when(Condition::Activation(3))
)?;

let pause = session.next_pause(Duration::from_secs(5)).await.unwrap();
pause.input;            // what it is about to receive
pause.resolved_config;  // its config with every binding resolved
pause.null_bindings;    // which of them came back empty
pause.state;            // the whole run state

session.controller().release(pause.pause_id, DebugCommand::Override {
    items: vec![Item::new(json!({ "fixed": true }))],
    port: None,
})?;
```

Commands: `Continue`, `Step` (stop at the very next node), `Override`, `Skip`,
`Fail` (into the node's own `on_error` policy), `Patch` (merge into the run
state before the node runs), and `Detach`.

### Why breakpoints are in-process

The engine already knows how to pause: a `requires_approval` gate raises a real
interrupt, checkpoints, and waits. That mechanism is built for waiting on a
*person*, which can take days — so it ends the run and resumes it later by
re-running the interrupted node from the top.

A breakpoint needs the opposite trade, and three things that path structurally
cannot do:

- **break *after* a node** — impossible safely on the interrupt path, because
  resuming re-runs the node and fires its side effects a second time;
- **override what a node produced** — needs the activation still on the stack;
- **be driven from another task** — so an agent can inspect, decide, and step
  across separate tool calls.

So a breakpoint parks the activation in place and a `DebugSession` owns the run.
The cost, stated plainly: a session lives in one process and dies with it.
`PauseMode::Durable` is available for the one case where surviving a restart
matters more — a break *before* a node, where nothing has run and the re-run is
free. It is refused for after-breakpoints rather than silently doubling side
effects.

### Nothing can wedge

A parked activation holds a run task, so every way of getting stuck has a way
out. Any one of these frees the run:

1. a **pause timeout** (5 minutes by default) releases it as `Continue`;
2. **`detach()`** clears every breakpoint and releases everything parked;
3. a **dropped release channel** resolves to `Continue` rather than waiting;
4. **dropping the session** detaches, cancels, and only then aborts — in that
   order.

## For agents

`testkit::tools` exposes every capability above as a named tool with a real
JSON Schema and a JSON-in/JSON-out dispatcher:

| Tool | Does |
| --- | --- |
| `flow_test.run` | Run against mocks; returns status, diagnosis, null bindings |
| `flow_test.trace` | The full trace, or one node's slice |
| `flow_test.node` | One node: input, output, bindings, calls |
| `flow_debug.start` | Start a debuggable run; returns a session id |
| `flow_debug.breakpoint` | Set, list, or clear breakpoints |
| `flow_debug.wait` | Wait for the run to park, and see where |
| `flow_debug.status` | Where the session is |
| `flow_debug.release` | Continue, step, override, skip, fail, patch, detach |
| `flow_debug.trace` | The trace of a session in flight |
| `flow_debug.stop` | End it and return the outcome |

```rust
let registry = TestkitRegistry::new();
let result = registry.dispatch("flow_test.run", json!({ "graph": graph })).await?;
```

**tinyflows registers nothing and talks to no model.** It says what the tools
are, what they do, and what they take; the host decides which to expose, to
whom, and under what name — the same division `catalog` draws for the node-kind
contracts.

Errors carry a closed set of stable snake_case codes (`unknown_tool`,
`invalid_arguments`, `invalid_graph`, `unknown_run`, `unknown_session`,
`unknown_pause`, `session_failed`), so a caller can branch on the code and read
the message rather than parse it.

Sessions are reaped after 15 minutes idle, so an agent that walks away does not
hold a spawned run for the life of the process.

## How it works underneath

All of it rests on one engine seam: `tinyflows::interception::StepInterceptor`,
consulted before and after every non-trigger node activation. Unlike a
`RunObserver` — whose callbacks return `()`, so it can watch a run and never
change one — the `StepAction` an interceptor returns is obeyed.

The action vocabulary is deliberately small, and each variant lands the
activation back on a path the engine already has: an injected failure enters the
node's own `on_error` policy, a replaced output routes through the same port and
lane logic real output does, and a substituted activation is still recorded as a
step and reported to the observer.

With no interceptor attached the engine builds no frame and makes no call. That
no-cost property is asserted by a property test over generated graphs, not only
by inspection: for every graph the validator accepts, a plain run and a run with
an inert interceptor produce the same output.

A host can implement `StepInterceptor` directly for a fault-injection harness of
its own — nothing here is special-cased inside the engine.
