//! Tests for working-directory resolution: what a node may name as the
//! directory it runs in, and what happens to the value when the run is pinned
//! to no workspace at all.
//!
//! Filesystem-only and offline — nothing here runs a graph.

use serde_json::json;

use super::{Absolute, resolve_dir_in_workspace, resolve_in_workspace, resolve_node_dir, run_workspace};

/// A workspace with a `worktrees/issue-1` directory and a file in it.
fn workspace() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(root.path().join("worktrees/issue-1")).expect("mkdir");
    std::fs::write(root.path().join("notes.txt"), "not a directory").expect("write");
    root
}

/// The canonical form of `root`, which is what a resolved path is compared
/// against: a temporary directory is a symlink on some platforms.
fn canonical(root: &tempfile::TempDir) -> std::path::PathBuf {
    root.path().canonicalize().expect("canonicalize")
}

#[test]
fn a_relative_directory_resolves_against_the_workspace() {
    let root = workspace();
    let resolved = resolve_dir_in_workspace(
        root.path(),
        "worktrees/issue-1",
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect("a directory in the workspace resolves");

    assert_eq!(resolved, canonical(&root).join("worktrees/issue-1"));
}

#[test]
fn an_absolute_directory_inside_the_workspace_is_allowed() {
    // The motivating case: an earlier node reports the worktree it created as
    // an absolute path, and the next node binds `cwd` straight to it.
    let root = workspace();
    let inside = canonical(&root).join("worktrees/issue-1");
    let resolved = resolve_dir_in_workspace(
        root.path(),
        &inside.to_string_lossy(),
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect("an absolute path inside the workspace resolves");

    assert_eq!(resolved, inside);
}

#[test]
fn an_absolute_directory_is_refused_where_the_rule_is_stricter() {
    // A script step's `args.script_path` has always been workspace-relative.
    let root = workspace();
    let inside = canonical(&root).join("worktrees/issue-1");
    let error = resolve_in_workspace(
        root.path(),
        &inside.to_string_lossy(),
        "args.cwd",
        Absolute::Refuse,
    )
    .expect_err("Absolute::Refuse takes no absolute path, inside or not");

    assert!(error.contains("must be relative to the workspace"), "{error}");
}

#[test]
fn a_directory_outside_the_workspace_is_refused() {
    let root = workspace();
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let error = resolve_dir_in_workspace(
        root.path(),
        &elsewhere.path().to_string_lossy(),
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect_err("an absolute path outside the workspace is refused");

    assert!(error.contains("resolves outside the workspace"), "{error}");
}

#[test]
fn a_relative_directory_may_not_traverse_out_of_the_workspace() {
    let root = workspace();
    let error = resolve_dir_in_workspace(
        root.path(),
        "../elsewhere",
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect_err("`..` is refused before the disk is touched");

    assert!(error.contains("must not traverse outside"), "{error}");
}

#[test]
fn a_directory_that_does_not_exist_fails_naming_the_path() {
    let root = workspace();
    let error = resolve_dir_in_workspace(
        root.path(),
        "worktrees/issue-404",
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect_err("a missing directory fails rather than falling back to the workspace");

    assert!(error.contains("worktrees/issue-404"), "{error}");
    assert!(error.contains("does not resolve inside the workspace"), "{error}");
}

#[test]
fn a_path_that_is_not_a_directory_is_refused() {
    let root = workspace();
    let error = resolve_dir_in_workspace(
        root.path(),
        "notes.txt",
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect_err("a file is not a working directory");

    assert!(error.contains("is not a directory"), "{error}");
}

#[cfg(unix)]
#[test]
fn a_symlink_inside_the_workspace_pointing_out_of_it_is_refused() {
    // The half no amount of string inspection would have caught.
    let root = workspace();
    let elsewhere = tempfile::tempdir().expect("tempdir");
    std::os::unix::fs::symlink(elsewhere.path(), root.path().join("escape")).expect("symlink");

    let error = resolve_dir_in_workspace(
        root.path(),
        "escape",
        "config.cwd",
        Absolute::AllowInside,
    )
    .expect_err("the symlink target is outside the workspace");

    assert!(error.contains("resolves outside the workspace"), "{error}");
}

#[test]
fn the_run_workspace_comes_from_the_run_slice_then_the_trigger() {
    assert_eq!(
        run_workspace(&json!({ "workspace": "/srv/checkout" })),
        Some("/srv/checkout")
    );
    assert_eq!(
        run_workspace(&json!({ "trigger": { "workspace": "/srv/from-trigger" } })),
        Some("/srv/from-trigger"),
        "a host may pin a workspace per run without editing the graph"
    );
    assert_eq!(
        run_workspace(&json!({
            "workspace": "/srv/seeded",
            "trigger": { "workspace": "/srv/from-trigger" }
        })),
        Some("/srv/seeded"),
        "the seeded run workspace wins"
    );
    assert_eq!(run_workspace(&json!({ "workspace": "  " })), None);
    assert_eq!(run_workspace(&json!({})), None);
}

#[test]
fn a_run_with_no_workspace_passes_the_directory_through() {
    // A harness whose agents run in a remote sandbox names directories this
    // process has never heard of; checking them locally would fail every one.
    let resolved = resolve_node_dir(&json!({}), "/srv/checkout", "config.cwd", "agent node a")
        .expect("no workspace, no resolution");

    assert_eq!(resolved, "/srv/checkout");
}

#[test]
fn a_resolved_directory_is_reported_with_the_node_surface() {
    let root = workspace();
    let run = json!({ "workspace": root.path().to_string_lossy() });
    let error = resolve_node_dir(&run, "nope", "config.cwd", "agent node prepare")
        .expect_err("a missing directory fails the step");

    assert!(error.to_string().starts_with("agent node prepare:"), "{error}");
}
