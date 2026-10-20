#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

engine_pattern='^(rusqlite|libsqlite3-sys|mongodb|bson|aws-sdk-s3|lapin|amq-protocol|sqlx|redb|sled|rocksdb|tinystoragedrivers-(document|kv|blob|queue|secrets|connect)|tinystoragedrivers)$'
engine_names() {
    awk '{print $1}' <<<"$1" | grep -xE "$engine_pattern" || true
}

# Keep the detector itself covered: it must catch an engine and accept public
# port crates and ordinary path dependencies.
if [[ -z "$(engine_names $'tinystoragedrivers-document v2.0.0\n')" ]]; then
    echo "engine detector self-test failed to catch a backend crate" >&2
    exit 1
fi
if [[ -n "$(engine_names $'tinystoragedrivers-api v2.0.0\ntinyflows-storage v0.1.0\n')" ]]; then
    echo "engine detector self-test rejected an API-only tree" >&2
    exit 1
fi

packages=$(cargo metadata --no-deps --format-version 1 | python3 -c '
import json, sys
metadata = json.load(sys.stdin)
library_kinds = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}
for package in metadata["packages"]:
    if any(set(target["kind"]) & library_kinds for target in package["targets"]):
        print(package["name"])
')
if [[ -z "$packages" ]]; then
    echo "cargo metadata returned no library targets; refusing a vacuous engine check" >&2
    exit 1
fi

while IFS= read -r package; do
    [[ -n "$package" ]] || continue
    tree=$(cargo tree -p "$package" --all-features -e normal,build --prefix none)
    engines=$(engine_names "$tree")
    if [[ -n "$engines" ]]; then
        printf 'storage implementation reached by %s:\n%s\n' "$package" "$engines" >&2
        exit 1
    fi
done <<<"$packages"
