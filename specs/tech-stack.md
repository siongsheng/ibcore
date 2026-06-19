# Tech Stack

## Rust toolchain

- **Rust 1.85+** (edition 2024)
- **ibapi** 3.0 — IB Gateway wire protocol crate
- **Tokio** 1.x — async runtime (features: `full`)
- **chrono** 0.4 — timestamp handling
- **thiserror** 2.x — derive(Error) for IbError
- **tracing** 0.1 — structured logging
- **futures** 0.3 — Stream/StreamExt utilities
- **serde** 1.x — serialisation with derive feature

## Python bindings

- **pyo3** 0.23 with `experimental-async` and `chrono` features
- **pyo3-asyncio** 0.23 with `attributes` and `tokio-runtime` features
- Python bindings live in `python/` workspace crate (`ibcore-python`)

## Version pinning

All major dependencies are pinned to SemVer-compatible ranges.
The Cargo.lock is committed for reproducible builds.
