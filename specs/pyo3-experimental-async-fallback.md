# PyO3 `experimental-async` Fallback Plan

**Date:** 2026-06-22
**Status:** Draft

## What `experimental-async` Provides

The PyO3 `experimental-async` feature (available since PyO3 0.21, still present in 0.23) enables `async fn` methods inside `#[pymethods]` blocks. When enabled, a Python `async def` can call an async Rust method and `await` it directly, bridging the asyncio event loop with Tokio's runtime internally.

```rust
// WITH experimental-async — this compiles:
#[pymethods]
impl PyIbClient {
    async fn stock_snapshot(&self, symbol: &str) -> PyResult<PyStockSnapshot> {
        // ...
    }
}
```

## Why ibcore Doesn't Need It

ibcore-python (the `python/` crate) **does not use** `experimental-async` in any source file:

- Zero `async fn` methods in `#[pymethods]` blocks
- All async Rust operations are executed via `rt.block_on()` using a dedicated Tokio `Runtime`
- The Python API surface is synchronous — `IbClient.connect(...)` blocks the calling thread, not `await`

A `git grep 'async fn' -- python/src/` confirms zero matches. The feature flag was present in `Cargo.toml` but unused.

## How It Was Removed

1. Removed `"experimental-async"` from the `pyo3` features list in `python/Cargo.toml`
2. Verified `cargo check -p ibcore-python` passes (no compilation errors)
3. Verified `maturin build --release` produces a working wheel
4. Verified Python tests pass: `cd python && pytest tests/`

The removal is safe and reversible — adding the feature back is a one-line change.

## Fallback Approaches (If We Need Python `await` Later)

If a future requirement demands true Python async/await support (e.g., streaming market data ticks as Python async generators), these are the options ranked by effort:

### Approach A: Status Quo (`block_on`) — **Recommended**

| Aspect | Detail |
|--------|--------|
| **Effort** | Zero — already implemented |
| **Mechanism** | Sync Python methods call `rt.block_on()` on a Tokio runtime |
| **Pros** | Zero dependencies, works now, simple, no PyO3 feature needed |
| **Cons** | Blocks the Python GIL thread during IB calls (acceptable for <100ms snapshots) |

This is the current architecture and the recommended path for the foreseeable future.

### Approach B: `asyncio` + Channel Bridge

| Aspect | Detail |
|--------|--------|
| **Effort** | Medium (2-3 days) |
| **Mechanism** | Spawn background Tokio task, bridge results to asyncio via `asyncio.run_coroutine_threadsafe()` + `queue.Queue` |
| **Pros** | Python-native async, no GIL issues, works with any PyO3 version |
| **Cons** | Two concurrency models to manage, complex error propagation, manual lifecycle |

### Approach C: `pyo3-async-runtimes` Crate

| Aspect | Detail |
|--------|--------|
| **Effort** | Low (1 day) |
| **Mechanism** | Use the community `pyo3-async-runtimes` crate to bridge Tokio → asyncio |
| **Pros** | Cleaner than hand-rolled bridge, designed for this use case |
| **Cons** | External dependency, maintenance risk if crate is abandoned |

### Approach D: Thread Pool Wrapper

| Aspect | Detail |
|--------|--------|
| **Effort** | Low (1 day) |
| **Mechanism** | Wrap sync methods in `concurrent.futures.ThreadPoolExecutor` from Python side |
| **Pros** | Simple, no Rust changes needed, works with any PyO3 version |
| **Cons** | GIL contention on Python object access, thread overhead |

## Recommendation

**Stay on Approach A (`block_on`).** The Python `IbClient` methods are fast (<100ms for snapshots, <500ms for account queries) — blocking the calling thread is acceptable. If async iteration (streaming ticks or order updates via `async for`) becomes a requirement, Approach B (asyncio channel bridge) is the pragmatic fallback.

## Timeline Concerns

The `experimental-async` feature has been experimental since PyO3 0.21 and remains present in 0.23. There is no announced deprecation timeline. The removal in this document is proactive cleanup, not reactive — ibcore-python does not depend on the feature.
