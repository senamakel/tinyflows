# Repository Guidelines

- Keep source files at 500 physical lines or fewer. When a file would exceed
  that limit, split it into smaller modules organized by responsibility instead
  of compressing or reformatting the code to fit.
- Keep Rust tests in files whose names end in `_tests.rs`; do not add large
  inline test modules to production source files.
- Before finishing a change, check the line counts of all affected source and
  test files and split any file that exceeds the limit.
