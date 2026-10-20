# Schedule storage compatibility fixtures

These records were emitted by the public `CronDocuments::add_flow_schedule_job`
writer and then read back through the tagged release's `DocumentStore::get`.
They are retained to exercise the old-collection migration and the current
collection's read compatibility with the previous TinyFlows release.

`cron-job-v0.8.8.json` came from TinyFlows tag `v0.8.8`
(`5882bf69993475b64ef6bcaaa5265fa389a5708d`). That release declared
`tinystoragedrivers-core` tag `v0.5.0`; the generated lockfile resolved it to
`fd96bea95a07abe06eb6039d2f1b8669ed0d2d09`. The public writer stored the job
in `cron_jobs` with its then-current serialized schedule/delivery fields.
Its fixture contains the returned id and document body.

`cron-job-v0.8.28.json` came from TinyFlows tag `v0.8.28`
(`a587c7239b0726557b379d6840ab3603dbcfaa64`). That release pinned
`tinystoragedrivers-core` revision
`be94f7568fe7650b0729be55128f48270414b9a7` (TSD 1.0.0). Its public writer
stored the job in `cron.jobs`; the fixture records the returned document,
version, and metadata from the same backend read.

Both generator sources are checked in beside this note. They were run from
archives of the exact TinyFlows tags with the declared TSD dependencies:

```sh
mkdir -p target/legacy-v0.8.8
git archive v0.8.8 | tar -x -C target/legacy-v0.8.8
mkdir -p target/legacy-v0.8.8/crates/tinyflows-drivers/examples
cp crates/tinyflows-module/tests/fixtures/storage/generators/generate_v0.8.8.rs \
  target/legacy-v0.8.8/crates/tinyflows-drivers/examples/generate_cron_fixture.rs
cargo run --manifest-path target/legacy-v0.8.8/Cargo.toml -p tinyflows-drivers \
  --no-default-features --features schedule --example generate_cron_fixture \
  > crates/tinyflows-module/tests/fixtures/storage/cron-job-v0.8.8.json

mkdir -p target/legacy-v0.8.28
git archive v0.8.28 | tar -x -C target/legacy-v0.8.28
mkdir -p target/legacy-v0.8.28/crates/tinyflows-drivers/examples
cp crates/tinyflows-module/tests/fixtures/storage/generators/generate_v0.8.28.rs \
  target/legacy-v0.8.28/crates/tinyflows-drivers/examples/generate_cron_fixture.rs
cargo run --manifest-path target/legacy-v0.8.28/Cargo.toml -p tinyflows-drivers \
  --no-default-features --features schedule --example generate_cron_fixture \
  > crates/tinyflows-module/tests/fixtures/storage/cron-job-v0.8.28.json
```

The v0.8.8 output is an envelope containing the legacy `cron_jobs` id and body.
The v0.8.28 output is an envelope containing the document and read-back
metadata. Their recorded SHA-256 digests are `4e7dacedea92c0d16348d45b1794699364b5ccae431d70320820c430845b4339` and
`ef28bcff4573ab326e007a128559801bc76f59b4ebbc352c057968f2272a827d`,
respectively. The writers generate current timestamps and UUIDs, so rerunning
produces different digest values. These are separate release artifacts;
neither combines old writer bytes with a newer engine.
