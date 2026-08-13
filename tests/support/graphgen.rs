//! A generator of valid, non-trivial workflow graphs, for property tests.
//!
//! # Why a shape grammar rather than random edges
//!
//! Wiring random nodes to random nodes almost never produces a graph the
//! validator accepts — the overwhelming majority of such graphs are rejected
//! for a missing trigger, an unreachable node, or an unbounded cycle. A
//! generator built that way spends its whole budget proving that
//! [`tinyflows::validate`] says no, and never reaches the engine, which is the
//! part under test.
//!
//! So graphs are built **compositionally** from [`Shape`]s that are correct by
//! construction. Every shape has exactly one entry node and one exit node, so
//! shapes nest and chain without any shape needing to know its context. What
//! gets randomised is the *structure* — nesting, arity, branch depth — not the
//! wiring.
//!
//! # The oracle problem
//!
//! A property test needs to know what the right answer is. Rather than predict
//! outputs, the leaves here are **pure control-flow nodes** (`output_parser`
//! passthroughs and `transform`s over constant values), so no capability is
//! consulted and the whole run is a deterministic function of the graph. That
//! turns "is the output correct?" — which needs a second implementation to
//! answer — into the properties actually worth asserting: it terminates, it is
//! deterministic, it survives a resume. Those hold regardless of what the
//! output *is*.

use proptest::prelude::*;
use serde_json::{Value, json};

use tinyflows::model::{Edge, Node, NodeKind, WorkflowGraph};

/// A structural template for part of a graph.
///
/// Every variant compiles to a subgraph with a single entry and a single exit,
/// which is the invariant that lets them nest and chain freely.
#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    /// `n` passthrough nodes in a row. `n == 0` is a single node, so a linear
    /// shape always has something to be an entry and an exit.
    Linear(usize),
    /// A `condition` whose `true`/`false` ports run different shapes and rejoin
    /// at a `merge`. Exercises conditional routing and the barrier relief that
    /// keeps an untaken branch from deadlocking the join.
    Branch(Box<Shape>, Box<Shape>),
    /// Two or more shapes fanned out from one port and rejoined at a `merge`.
    /// This is the parallel case: every branch runs in the same super-step.
    Fanout(Vec<Shape>),
    /// A bounded `loop` whose `body` runs a shape and returns to the head.
    Loop {
        /// The loop's `max_iterations` cap.
        max_iter: u64,
        /// What runs on each pass.
        body: Box<Shape>,
    },
    /// A node that pauses the run awaiting approval, followed by a shape.
    ///
    /// Present so generated runs actually *suspend*, which is the only way to
    /// exercise the checkpoint/resume path. A shape containing one of these
    /// cannot complete without either a pre-approval on the run input or a
    /// resume that names its gate.
    Gate(Box<Shape>),
}

impl Shape {
    /// An upper bound on the super-steps a run of this shape can take.
    ///
    /// Used to give a generated graph a `recursion_limit` that is generous
    /// enough never to be the reason a run fails, so a run that *does* hit a
    /// bound has found something real. Deliberately an over-estimate.
    fn step_budget(&self) -> u64 {
        match self {
            Self::Linear(n) => *n as u64 + 1,
            Self::Branch(a, b) => 2 + a.step_budget().max(b.step_budget()),
            Self::Fanout(branches) => {
                2 + branches.iter().map(Self::step_budget).max().unwrap_or(0)
            }
            // Each pass costs the body plus the head itself.
            Self::Loop { max_iter, body } => (max_iter + 1) * (body.step_budget() + 1),
            Self::Gate(rest) => 1 + rest.step_budget(),
        }
    }

    /// The ids of every approval gate this shape will contain, in the order
    /// [`Builder`] hands ids out.
    ///
    /// A caller needs these up front to pre-approve a run or to resume one, and
    /// recomputing them by walking the built graph would just be re-deriving
    /// what the builder already knew.
    #[must_use]
    pub fn gate_ids(&self) -> Vec<String> {
        let mut builder = Builder::new();
        builder.build(self);
        builder.gates
    }
}

/// Builds the nodes and edges of a graph, handing out unique ids as it goes.
struct Builder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    next_id: usize,
}

/// The entry and exit node ids of a built subgraph.
struct Span {
    entry: String,
    exit: String,
}

impl Builder {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            next_id: 0,
        }
    }

    /// Adds a node with a fresh id and returns that id.
    fn add(&mut self, kind: NodeKind, config: Value) -> String {
        let id = format!("n{}", self.next_id);
        self.next_id += 1;
        self.nodes.push(Node {
            id: id.clone(),
            kind,
            type_version: 1,
            name: id.clone(),
            config,
            ports: vec![],
            position: None,
        });
        id
    }

    /// Adds a passthrough node — the neutral filler this generator builds
    /// linear runs from. `output_parser` with no config emits its input
    /// unchanged and consults no capability.
    fn passthrough(&mut self) -> String {
        self.add(NodeKind::OutputParser, Value::Null)
    }

    fn connect(&mut self, from: &str, port: &str, to: &str) {
        self.edges.push(Edge {
            from_node: from.to_string(),
            from_port: port.to_string(),
            to_node: to.to_string(),
            to_port: "main".to_string(),
        });
    }

    /// Emits `shape` and returns its entry/exit ids.
    fn build(&mut self, shape: &Shape) -> Span {
        match shape {
            Shape::Linear(n) => {
                let entry = self.passthrough();
                let mut exit = entry.clone();
                for _ in 0..*n {
                    let next = self.passthrough();
                    self.connect(&exit, "main", &next);
                    exit = next;
                }
                Span { entry, exit }
            }

            Shape::Branch(yes, no) => {
                // The field is absent from the items flowing through, so it
                // resolves falsey and the `false` arm is the one taken. That is
                // deliberate: it means every run exercises barrier relief on
                // the join, which is the interesting path.
                let head = self.add(NodeKind::Condition, json!({ "field": "take_yes" }));
                let join = self.add(NodeKind::Merge, Value::Null);
                for (port, arm) in [("true", yes), ("false", no)] {
                    let span = self.build(arm);
                    self.connect(&head, port, &span.entry);
                    self.connect(&span.exit, "main", &join);
                }
                Span {
                    entry: head,
                    exit: join,
                }
            }

            Shape::Fanout(branches) => {
                let apex = self.passthrough();
                let join = self.add(NodeKind::Merge, Value::Null);
                for (index, branch) in branches.iter().enumerate() {
                    // A `transform` at the head of each branch stamps which
                    // branch the items came down, so a determinism failure
                    // names the branch that diverged rather than just the run.
                    let tag = self.add(NodeKind::Transform, json!({ "set": { "branch": index } }));
                    let span = self.build(branch);
                    // Every branch leaves the apex on the *same* port, which is
                    // what the engine reads as a parallel fan-out rather than a
                    // conditional choice.
                    self.connect(&apex, "main", &tag);
                    self.connect(&tag, "main", &span.entry);
                    self.connect(&span.exit, "main", &join);
                }
                Span {
                    entry: apex,
                    exit: join,
                }
            }

            Shape::Loop { max_iter, body } => {
                // `on_exceeded: continue` so exhausting the cap is a normal
                // exit through `done` rather than an error. A generated graph
                // should only fail when something is actually wrong.
                let head = self.add(
                    NodeKind::Loop,
                    json!({ "max_iterations": max_iter, "on_exceeded": "continue" }),
                );
                let span = self.build(body);
                self.connect(&head, "body", &span.entry);
                self.connect(&span.exit, "main", &head); // the back-edge
                let out = self.passthrough();
                self.connect(&head, "done", &out);
                Span {
                    entry: head,
                    exit: out,
                }
            }
        }
    }
}

/// Compiles a [`Shape`] into a runnable [`WorkflowGraph`].
///
/// The graph gets a trigger wired to the shape's entry, and a `recursion_limit`
/// derived from the shape rather than left to the default — a generated graph
/// that legitimately needs many super-steps should not be failed by a budget
/// that has nothing to do with what is being tested.
#[must_use]
pub fn graph_of(shape: &Shape) -> WorkflowGraph {
    let mut builder = Builder::new();
    let span = builder.build(shape);
    let trigger = Node {
        id: "trigger".to_string(),
        kind: NodeKind::Trigger,
        type_version: 1,
        name: "trigger".to_string(),
        config: json!({ "recursion_limit": shape.step_budget() + 8 }),
        ports: vec![],
        position: None,
    };
    builder.edges.push(Edge {
        from_node: "trigger".to_string(),
        from_port: "main".to_string(),
        to_node: span.entry,
        to_port: "main".to_string(),
    });
    let mut nodes = vec![trigger];
    nodes.extend(builder.nodes);
    WorkflowGraph {
        name: "generated".to_string(),
        nodes,
        edges: builder.edges,
        ..Default::default()
    }
}

/// A strategy for shapes, bounded by nesting `depth`.
///
/// Arities are kept small (branch fan-out of 2–3, loops of 1–3 passes) on
/// purpose: property tests find bugs through *structural variety*, not through
/// size, and small counterexamples are the ones a human can read once proptest
/// has shrunk them.
pub fn arb_shape(depth: u32) -> impl Strategy<Value = Shape> {
    let leaf = (0usize..3).prop_map(Shape::Linear);
    leaf.prop_recursive(depth, 24, 3, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| Shape::Branch(Box::new(a), Box::new(b))),
            prop::collection::vec(inner.clone(), 2..4).prop_map(Shape::Fanout),
            (1u64..4, inner).prop_map(|(max_iter, body)| Shape::Loop {
                max_iter,
                body: Box::new(body)
            }),
        ]
    })
}

/// A strategy for whole graphs, at the default nesting depth.
pub fn arb_workflow_graph() -> impl Strategy<Value = WorkflowGraph> {
    arb_shape(3).prop_map(|shape| graph_of(&shape))
}
