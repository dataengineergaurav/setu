# Contributing to setu

Thank you for your interest in contributing! This document covers the workflow and conventions for submitting changes.

## Getting Started

1. Fork the repository and clone your fork.
2. Ensure you have Rust 1.81+ installed (`rustup show` to verify).
3. Build: `cargo build`
4. Run tests: `cargo test` (some tests require a running PostgreSQL instance — see below).

## Development Workflow

### Branches

- `main` — stable, release-ready code.
- Feature branches — create from `main`, merge back via pull request.

### Commits

- Write clear, concise commit messages in the imperative mood ("Add widget", not "Added widget").
- Prefix with the component name when relevant: `ingress:`, `filter:`, `egress`, `config:`, `docs:`.

### Code Style

- Run `cargo fmt` before committing (config in `rustfmt.toml`).
- Follow the existing code conventions: avoid unnecessary `return` statements, use `anyhow::Result` for fallible functions, prefer `map`/`and_then` over explicit `match` for `Option`.
- Keep functions focused and small. Extract helpers when a function exceeds ~30 lines.
- Add unit tests alongside new features.

### Testing

- **Unit tests** live in `#[cfg(test)] mod tests` blocks within each source file.
- **Integration tests** live in `tests/`. Currently two require a running PostgreSQL:
  ```
  DATABASE_URL="host=localhost user=postgres" cargo test -- --ignored
  ```
- Run the full suite before submitting: `cargo test`.

## Adding a New Source Type

1. Add a new variant to `SourceKind` in `src/types.rs`.
2. Add a new variant to `SourceDef` in `src/config.rs` (for the YAML `source` block).
3. Add a new variant to `SourceConfig` in `src/ingress/mod.rs`.
4. Implement `IngressSource` for your new source.
5. Wire it into `create_source()` in `src/ingress/mod.rs`.

See `src/ingress/postgres.rs` as a reference implementation.

## Adding a New Destination Type

1. Add a new variant to `DestinationKind` in `src/types.rs`.
2. Create a module under `src/egress/` (e.g. `src/egress/my_dest.rs`).
3. Implement a `pub async fn send(task: &ActivationTask, client: &Client) -> bool`.
4. Add the match arm in `src/main.rs`.
5. Add a test for the retry constant and a basic failure case.

## Pull Request Process

1. Ensure all CI checks pass (build, test, lint).
2. Update documentation (AGENTS.md, README.md) if the PR changes behaviour.
3. At least one maintainer review is required before merging.
4. Squash-merge on approval.

## Reporting Security Issues

If you find a security vulnerability, **do not** open a public issue. See [SECURITY.md](SECURITY.md) for the disclosure process.

## Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating you agree to abide by its terms.
