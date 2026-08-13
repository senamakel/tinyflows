#![cfg(feature = "mock")]
mod support;
use proptest::prelude::*;
use proptest::test_runner::{TestRunner, Config};
use support::graphgen::{arb_shape, graph_of};
use tinyflows::compiler::compile;

#[test]
fn diagnose_generator_validity() {
    let mut runner = TestRunner::new(Config { cases: 200, ..Config::default() });
    let (mut ok, mut bad) = (0, 0);
    let mut reasons: Vec<String> = Vec::new();
    for _ in 0..200 {
        let shape = arb_shape(3).new_tree(&mut runner).unwrap().current();
        let g = graph_of(&shape);
        match compile(&g) {
            Ok(_) => ok += 1,
            Err(e) => { bad += 1; if reasons.len() < 5 { reasons.push(format!("{e}")); } }
        }
    }
    println!("COMPILED_OK={ok} REJECTED={bad}");
    for r in &reasons { println!("REASON: {r}"); }
}
