//! Resolving an author-supplied working directory against the run's workspace.
//!
//! A node config's `cwd` (and a `sub_workflow` node's `workspace`) is an
//! **untrusted string**: a workflow arrives as a file, possibly written by an
//! agent, and the directory it names is where a harness will run. The rule this
//! module holds is the one
//! [`ScriptPolicy`](crate::caps::host::ScriptPolicy) already held for a shell
//! step's `args.cwd` — resolve against the configured workspace, follow
//! symlinks, and refuse anything that lands outside it — hoisted here so the
//! `agent` and `sub_workflow` nodes enforce the *same* rule rather than a second
//! one shaped slightly differently.
//!
//! # The workspace
//!
//! The engine has no filesystem of its own, so the boundary has to be declared.
//! A run carries it at `run.workspace`, seeded from the trigger node's
//! `config.workspace` (graph-level) or from the trigger payload's `workspace`
//! key (per-run, host-supplied), and forwarded to `sub_workflow` children.
//!
//! **A run with no workspace resolves nothing.** A `cwd` on such a run is passed
//! through to the harness verbatim, exactly as `working_dir` always has been:
//! a host whose agents run in a remote sandbox names directories the engine's
//! own filesystem knows nothing about, and checking those against the local disk
//! would fail every one of them.

use std::path::{Component, Path, PathBuf};

use serde_json::Value;

/// Whether an author may name an absolute path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Absolute {
    /// Refuse one outright — the historical `args.script_path` / `args.cwd`
    /// rule, where a relative path is the only legal spelling.
    Refuse,
    /// Accept one, provided it resolves inside the workspace.
    ///
    /// What an `agent` node's `cwd` needs: the directory it points at is
    /// usually one an earlier node created and reported back as an absolute
    /// path (`"=nodes.prepare.item.json.worktree"`), and demanding the author
    /// re-derive a relative path from it would be busywork with a worse failure
    /// mode.
    AllowInside,
}

/// The workspace this run is pinned to, if any.
///
/// Precedence: the seeded `run.workspace`, then a `workspace` key on the
/// trigger payload (`run.trigger.workspace`) for a host that pins one per run
/// without editing the graph.
#[must_use]
pub(crate) fn run_workspace(run: &Value) -> Option<&str> {
    run.get("workspace")
        .and_then(Value::as_str)
        .or_else(|| {
            run.get("trigger")
                .and_then(|t| t.get("workspace"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|w| !w.is_empty())
}

/// Resolves `raw` against `workspace`, refusing anything that escapes it.
///
/// Both halves are load-bearing, and both come from the shell step's policy.
/// The syntactic check answers the obvious `../../etc/passwd` without touching
/// the disk; canonicalizing and re-checking afterwards is what catches a symlink
/// *inside* the workspace pointing out of it, which no amount of string
/// inspection would have seen.
///
/// `field` is how the caller spells the offending key (`args.cwd`,
/// `config.cwd`), so the message points at what the author wrote. The error is
/// a bare message: each caller wraps it in its own surface's error prefix.
///
/// # Errors
/// Returns the refusal message when the workspace is unreadable, when `raw` is
/// shaped wrong for `absolute`, when it traverses upwards, or when it does not
/// resolve to an existing path inside the workspace.
pub(crate) fn resolve_in_workspace(
    workspace: &Path,
    raw: &str,
    field: &str,
    absolute: Absolute,
) -> Result<PathBuf, String> {
    let candidate = Path::new(raw);
    if candidate.is_absolute() {
        if absolute == Absolute::Refuse {
            return Err(format!(
                "`{field}` ('{raw}') must be relative to the workspace, not absolute"
            ));
        }
    } else if candidate.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(format!(
            "`{field}` ('{raw}') must not traverse outside the workspace"
        ));
    }

    let workspace = workspace.canonicalize().map_err(|err| {
        format!(
            "the configured workspace ({}) is unreadable: {err}",
            workspace.display()
        )
    })?;
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        workspace.join(candidate)
    };
    let resolved = joined.canonicalize().map_err(|err| {
        format!(
            "`{field}` ('{raw}') does not resolve inside the workspace ({}): {err}",
            joined.display()
        )
    })?;
    if !resolved.starts_with(&workspace) {
        return Err(format!(
            "`{field}` ('{raw}') resolves outside the workspace ({})",
            workspace.display()
        ));
    }
    Ok(resolved)
}

/// [`resolve_in_workspace`], additionally requiring the result to be a
/// directory.
///
/// # Errors
/// As [`resolve_in_workspace`], plus a refusal when the path exists but is not a
/// directory. A path that does not exist at all fails in
/// [`resolve_in_workspace`] rather than falling back to the workspace: a step
/// silently running somewhere other than where its author said is the failure
/// this whole module exists to prevent.
pub(crate) fn resolve_dir_in_workspace(
    workspace: &Path,
    raw: &str,
    field: &str,
    absolute: Absolute,
) -> Result<PathBuf, String> {
    let resolved = resolve_in_workspace(workspace, raw, field, absolute)?;
    if !resolved.is_dir() {
        return Err(format!(
            "`{field}` ('{raw}') is not a directory in the workspace"
        ));
    }
    Ok(resolved)
}

/// Resolves a node's declared working directory against the run's workspace.
///
/// Returns the resolved absolute path when the run is pinned to a workspace, and
/// `raw` unchanged when it is not (see the module docs: the engine cannot check
/// a directory on a filesystem it does not have).
///
/// # Errors
/// Returns [`EngineError::Capability`](crate::error::EngineError::Capability),
/// prefixed with `surface`, when the directory escapes the workspace, does not
/// exist, or is not a directory.
pub(crate) fn resolve_node_dir(
    run: &Value,
    raw: &str,
    field: &str,
    surface: &str,
) -> crate::error::Result<String> {
    let Some(workspace) = run_workspace(run) else {
        tracing::debug!(
            field,
            raw,
            "workdir: the run declares no workspace; passing the directory through unresolved"
        );
        return Ok(raw.to_string());
    };
    let resolved =
        resolve_dir_in_workspace(Path::new(workspace), raw, field, Absolute::AllowInside).map_err(
            |message| crate::error::EngineError::Capability(format!("{surface}: {message}")),
        )?;
    Ok(resolved.to_string_lossy().into_owned())
}

#[cfg(test)]
#[path = "workdir_tests.rs"]
mod tests;
