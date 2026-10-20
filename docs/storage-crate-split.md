# Storage crate split (decision)

Status: updated 2026-10-20. Tracks tinyhumansai/openhuman#7754, #7522 and #8131.

## Decision

`tinyflows-drivers` is **not** folded into the published `tinyflows` crate.
The adapters depend on the stable public `tinystoragedrivers-api` and
`tinystoragedrivers-types` contracts through the internal `tinyflows-storage`
helper crate. `tinyflows-import` was retired after the v0.8.9 migration
transition; file and SQLite discovery is host-owned and no longer lives in
the TinyFlows runtime.

## Why

- The published `tinyflows` engine remains separate from its optional driver
  adapters, and does not gain a dependency on the private workspace helper.
- The helper crate keeps adapter-owned storage behavior separate while using
  the published storage vocabulary.

## What would change this

No further storage crate reshaping is planned. Hosts open and select the
backend, while TinyFlows receives scoped ports and declares only its owned
collections.

## Guard

`scripts/check-published-crate.sh` (CI) fails if the released `tinyflows`
package gains a git or `tinystoragedrivers*` dependency, or if any crate that
names `tinystoragedrivers*` is publishable.

## Host `[patch]`

OpenHuman's root and app `[patch."https://github.com/tinyhumansai/tinystoragedrivers"]`
cannot be removed from this side. tinyagents takes tinystoragedrivers as a path
into its own vendored copy, tinyflows takes it by git tag, and cargo treats those
as two sources (two `DocumentStore` traits, two `libsqlite3-sys`). The patch
collapses them. It goes when tinyagents and tinyflows name the same git
source, or when the ports are published and both use a registry version.
