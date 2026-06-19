# Conventions

## Testing

- **TDD**: write failing test first, minimum code to pass, refactor
- **Two-commit rule**: test commit (RED) before implementation commit (GREEN)
- Tests must compile and pass before declaring a task done
- Python bindings tested via Rust unit tests in python/src/lib.rs

## Code style

- Rust edition 2024
- All IB calls async via Tokio
- PyO3 bindings in separate workspace crate (not feature-gated in main crate)
- No hardcoded secrets — all credentials via env vars or args
- Full Rustdoc on all public items
- clippy must pass with `-D warnings`

## Module organisation

- `src/` — core Rust API
- `python/` — PyO3 Python bindings (separate crate)
- `specs/` — design documents and constitutional files
