# Plan: Expose order_updates & open_orders in Python bindings

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Expose the Rust `IbClient::open_orders()` and `IbClient::order_updates()` methods to Python via PyO3, plus the `OpenOrder` and `OrderStatusEvent` types.

**Architecture:** Four new PyO3 `#[pyclass]` types added to `python/src/lib.rs` following the existing patterns: `PyOpenOrder` (flat struct with `#[pyo3(get)]` + `#[new]`), `PyOrderStatusEvent` (flat struct with `kind: str` discriminator + `#[new]`), `PyOrderUpdateReceiver` (blocking iterator backed by `std::sync::mpsc::sync_channel(1024)` + background tokio task, with explicit `Drop` impl). Two new methods on `PyIbClient`: `open_orders()` (returns `list[dict]`, same pattern as `positions()`) and `order_updates()` (returns `PyOrderUpdateReceiver`). One pre-task: re-export `OpenOrder`, `OrderStatusEvent`, `OrderStatusStream` from `src/lib.rs`.

**Key design decisions from orchestrator review (2026-06-21):**
- Bounded channel (`sync_channel(1024)`) — prevents unbounded memory growth; drops newest events on overflow with `tracing::warn!`.
- `__next__` returns `PyResult<Option<PyOrderStatusEvent>>` — matching existing `PyDiagnosticEventReceiver` pattern.
- `Err(IbError)` from the stream is **logged and skipped** (not forwarded to Python).
- `#[new]` constructors on both `PyOpenOrder` and `PyOrderStatusEvent` with keyword defaults.
- Explicit `impl Drop` on `PyOrderUpdateReceiver` calling `.take().abort()` on the background task handle.
- Commission-only events appear as `kind="Filled"`, `order_id=0`, `commission=Some(x.xx)`.
- `next()` blocks indefinitely — by design. Users needing timeouts should use a thread wrapper.
- `KeyboardInterrupt` during `next()` is delayed until the next event arrives (`recv()` is uninterruptible).

**Tech Stack:** Rust (edition 2024), PyO3 0.23, `std::sync::mpsc`, tokio

---

## DAG

```
Task0 (lib.rs re-exports)
  │
  ├── Task1 → Task2 → Task3 → Task4 → Task5 → Task6 → [Task7 ‖ Task8 ‖ Task9 ‖ Task10]
```

Task 0 (re-exports) must complete first — Tasks 1 and 4 depend on `ibcore::OpenOrder` / `ibcore::OrderStatusEvent` being importable. Tasks 1–6 are sequential (all touch `python/src/lib.rs`). Tasks 7–10 touch different files and are parallelizable with each other after Task 6.

---

## IMPACT ASSESSMENT

| File | Change | ~Lines |
|------|--------|--------|
| `src/lib.rs` | Add `pub use orders::{OpenOrder, OrderStatusEvent, OrderStatusStream};` | +1 |
| `python/src/lib.rs` | Add 4 new types (with `#[new]`, `Drop`, bounded channel), 2 new methods, register in module | +280 |
| `python/ibcore/__init__.py` | Add 3 imports, 3 `__all__` entries | +5 |
| `python/tests/test_order_types.py` | NEW — unit tests for PyOpenOrder, PyOrderStatusEvent (incl. constructor kwarg tests) | +70 |
| `python/tests/test_order_receiver.py` | NEW — unit tests for PyOrderUpdateReceiver | +55 |
| `python/tests/test_client.py` | Add 2 method names to existing test | +2 |

**Cascading effects:** None — additive-only feature. No existing code modified (except `src/lib.rs`, `__init__.py`, and `test_client.py` which get additions, not modifications).

**Risk:** LOW for `open_orders()` (trivial pass-through). MEDIUM for `order_updates()` (mpsc bridge + background task — one unverified GIL-release assumption, mitigated by bounded channel + documented fallback).

---

## API / Interface Proposal

### PyOpenOrder (new pyclass)

```
#[pyclass(name = "OpenOrder")]
PyOpenOrder:
    Fields (all #[pyo3(get)]):
        order_id: i32          (default 0)
        symbol: String         (default "")
        action: String         (default "")   # "BUY" / "SELL"
        quantity: f64          (default 0.0)
        order_type: String     (default "")   # "LMT" / "MKT" / "STP"
        limit_price: Option<f64>  (default None)
        status: String         (default "")
        filled_qty: f64        (default 0.0)
        remaining_qty: f64     (default 0.0)
    Methods:
        #[new]
        #[pyo3(signature = (order_id=0, symbol="", action="", quantity=0.0,
                            order_type="", limit_price=None, status="",
                            filled_qty=0.0, remaining_qty=0.0))]
        fn new(...) -> Self

        __repr__() -> String
    From<ibcore::OpenOrder>
```

### PyOrderStatusEvent (new pyclass)

```
#[pyclass(name = "OrderStatusEvent")]
PyOrderStatusEvent:
    Fields (all #[pyo3(get)]):
        kind: String              (default "")   # "Submitted"/"Filled"/"Cancelled"/"Inactive"/"Rejected"/"Other"
        order_id: i32             (default 0)
        filled_qty: f64           (default 0.0)  # 0.0 for non-Filled
        avg_price: f64            (default 0.0)  # 0.0 for non-Filled
        commission: Option<f64>   (default None) # None for non-Filled; commission-only events have order_id=0
        reason: String            (default "")   # "" for non-Cancelled/Rejected
        status: String            (default "")   # "" for non-Other
    Methods:
        #[new]
        #[pyo3(signature = (kind="".into(), order_id=0, filled_qty=0.0,
                            avg_price=0.0, commission=None, reason="".into(),
                            status="".into()))]
        fn new(...) -> Self

        __repr__() -> String
    From<ibcore::OrderStatusEvent>
```

### PyOrderUpdateReceiver (new pyclass)

```
#[pyclass(name = "OrderUpdateReceiver")]
PyOrderUpdateReceiver:
    Fields (internal only — NOT #[pyo3(get)]):
        rx: std::sync::mpsc::Receiver<PyOrderStatusEvent>
        _task: Option<tokio::task::JoinHandle<()>>
    Methods:
        __iter__(slf) -> slf
        __next__(&mut self) -> PyResult<Option<PyOrderStatusEvent>>
            # Blocks on rx.recv() inside py.allow_threads().
            # Returns Ok(Some(event)) for each order event.
            # Returns Ok(None) when channel is disconnected → StopIteration.
            # GIL_FALLBACK: if allow_threads is broken, switch to try_recv() + 10ms sleep loop.
            # KeyboardInterrupt is delayed until next event (recv is uninterruptible).
    Drop:
        # MUST be explicit: self._task.take().map(|h| h.abort())
        # Without this, dropping the receiver detaches the task (it keeps running
        # until mpsc send fails on next event — leaky).
    NOT Send / NOT Sync — safe because Python's GIL serializes access.
```

### Background task (internal — spawned in `order_updates()`)

```
tokio::task::spawn(async move {
    // Use try_send to respect bounded channel backpressure
    // Log and skip Err(IbError) variants — don't forward to Python
    loop {
        match stream.next().await {
            Some(Ok(event)) => {
                let py_event = PyOrderStatusEvent::from(event);
                match tx.try_send(py_event) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        tracing::warn!("order update channel full, dropping event");
                    }
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
            Some(Err(e)) => {
                tracing::warn!(%e, "order update stream error, continuing");
                // Continue — don't break on transient errors
            }
            None => break, // stream ended
        }
    }
})
```

### PyIbClient new methods

```
open_orders(&self, py) -> PyResult<PyObject>
    Returns: list[dict] of open order data (same dict-per-item pattern as positions())
    Dict keys: "order_id", "symbol", "action", "quantity", "order_type",
               "limit_price", "status", "filled_qty", "remaining_qty"
    Errors: IbError if not connected or IB returns error

order_updates(&self) -> PyResult<PyOrderUpdateReceiver>
    Returns: iterable receiver that blocks on next()
    Internally:
      1. with_client locks mutex, calls block_on(client.order_updates()) → OrderStatusStream
      2. Creates std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1024)
      3. Spawns tokio task bridging stream → channel (see Background task above)
      4. Returns PyOrderUpdateReceiver { rx, _task: Some(handle) }
    Errors: IbError if not connected or IB rejects second subscription
    NOTE: with_client holds the mutex ONLY during step 1 (quick subscription call, milliseconds).
          The mutex is released before returning to Python. No deadlock on repeated calls.
```

---

## Task Breakdown

### Task 0: Re-export order types from src/lib.rs

**Files:** `src/lib.rs`
**Dependencies:** [none]
**Parallelizable:** yes (runs before all other tasks)
**Description:** Add `pub use orders::{OpenOrder, OrderStatusEvent, OrderStatusStream};` to the re-exports section of `src/lib.rs` (after line 59, alongside existing `pub use streaming::{TickEvent, TickStream};`). This is required so the Python crate can write `use ibcore::OpenOrder` rather than reaching into `ibcore::orders::OpenOrder` (which would be a private-module access).

**Step 1:** Add the re-export line.
**Step 2:** Run `cargo check` — must compile clean.
**Step 3:** Commit.

---

### Task 1: Add PyOpenOrder pyclass to python/src/lib.rs

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 0]
**Parallelizable:** no
**Description:** Add `PyOpenOrder` struct with `#[pyclass(name = "OpenOrder")]` and `#[pyo3(get)]` fields matching the Rust `OpenOrder` struct. Implement `#[new]` constructor with all fields as keyword arguments with defaults (so `OpenOrder(order_id=1, symbol="SPY")` works). Implement `From<ibcore::OpenOrder>` and `__repr__`. Add Rust-side unit tests in the `#[cfg(test)] mod tests` block: construction via `#[new]`, field access via GIL, repr, and conversion from Rust type. Follow the exact pattern of `PyStockSnapshot` (lines 20-53 of lib.rs), but with `#[new]`.

Key detail: `#[new]` signature must use `#[pyo3(signature = (...))]` with all 9 fields having defaults. See API proposal above for exact signature.

---

### Task 2: Add PyOrderStatusEvent pyclass to python/src/lib.rs

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 1]
**Parallelizable:** no
**Description:** Add `PyOrderStatusEvent` struct with `#[pyclass(name = "OrderStatusEvent")]`. Flatten the Rust enum into a single struct with a `kind: String` discriminator field. All variant-specific fields are present but zero/empty for non-matching kinds. Implement `#[new]` with all fields as keyword arguments with defaults. Implement `From<ibcore::OrderStatusEvent>` that maps each variant to the flat struct (including the `CommissionReport` → `Filled { order_id: 0, ... commission: Some(x.xx) }` case). Implement `__repr__`. Add Rust-side unit tests for each variant mapping (Submitted, Filled, Cancelled, Inactive, Rejected, Other, CommissionReport) and repr output.

Key detail: The `From<OrderStatusEvent>` impl for the `CommissionReport` arm must produce `kind: "Filled"`, `order_id: 0`, `commission: Some(x.xx)` — matching the orders.rs:139-144 behavior. Users identify commission-only events by checking `order_id == 0 && commission is not None`.

---

### Task 3: Add PyOrderUpdateReceiver pyclass to python/src/lib.rs

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 2]
**Parallelizable:** no
**Description:** Add `PyOrderUpdateReceiver` struct with `#[pyclass(name = "OrderUpdateReceiver")]`. Internal fields: `rx: std::sync::mpsc::Receiver<PyOrderStatusEvent>`, `_task: Option<tokio::task::JoinHandle<()>>`. Implement:
- `__iter__` (returns `slf`)
- `__next__` → `PyResult<Option<PyOrderStatusEvent>>` — calls `self.rx.recv()` inside `py.allow_threads()`, returns `Ok(Some(event))` on event, `Ok(None)` on disconnect (→ `StopIteration`). Include the `// GIL_FALLBACK:` comment marker before the allow_threads block.
- `impl Drop` — `self._task.take().map(|h| h.abort());`

Add Rust-side unit tests: receiver construction, `__iter__` returns self, `__next__` returns events, `Drop` aborts task (verify JoinHandle is taken).

Key detail: `__next__` return type MUST be `PyResult<Option<PyOrderStatusEvent>>` — matching `PyDiagnosticEventReceiver::__next__` return type at line 586. Do NOT use bare `Option<PyOrderStatusEvent>`.

---

### Task 4: Add open_orders() method to PyIbClient

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 1]
**Parallelizable:** no
**Description:** Add `open_orders(&self, py: Python<'_>) -> PyResult<PyObject>` method to `PyIbClient`'s `#[pymethods]` block. Calls `self.rt.block_on(client.open_orders())` inside `with_client`, converts each `OpenOrder` to a Python dict using the same pattern as `positions()` (lines 435-454 of lib.rs). Dict keys: `order_id`, `symbol`, `action`, `quantity`, `order_type`, `limit_price`, `status`, `filled_qty`, `remaining_qty`. Include `limit_price` as `None` (Python `py.None()`) when `Option::None`. Add Rust-side test: method exists, returns correct dict shape from mock data.

---

### Task 5: Add order_updates() method to PyIbClient

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 3]
**Parallelizable:** no
**Description:** Add `order_updates(&self) -> PyResult<PyOrderUpdateReceiver>` method to `PyIbClient`'s `#[pymethods]` block. Implementation:
1. `with_client` locks mutex, calls `self.rt.block_on(client.order_updates())` → `OrderStatusStream`
2. Creates `std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1024)`
3. Spawns `tokio::task::spawn` bridging task (see Background task in API proposal):
   - Uses `try_send` (not `send`) to respect bounded channel backpressure
   - On `TrySendError::Full`: `tracing::warn!("order update channel full, dropping event")` + continue
   - On `TrySendError::Disconnected`: break
   - On `Some(Err(e))` from stream: `tracing::warn!(%e, "order update stream error, continuing")` + continue
   - On `None` from stream: break
4. Returns `PyOrderUpdateReceiver { rx, _task: Some(handle) }`

Add Rust-side test: method exists, returns receiver with correct type.

---

### Task 6: Register new classes in pymodule init

**Files:** `python/src/lib.rs`
**Dependencies:** [Task 1, Task 2, Task 3, Task 4, Task 5]
**Parallelizable:** no
**Description:** In the `#[pymodule] fn _ibcore` function (lines 616-627), add:
- `m.add_class::<PyOpenOrder>()?;`
- `m.add_class::<PyOrderStatusEvent>()?;`
- `m.add_class::<PyOrderUpdateReceiver>()?;`

Verify compilation with `cargo build -p ibcore-python --features python 2>&1`.

---

### Task 7: Update python/ibcore/__init__.py exports

**Files:** `python/ibcore/__init__.py`
**Dependencies:** [Task 6]
**Parallelizable:** yes
**Description:** Add `OpenOrder`, `OrderStatusEvent`, `OrderUpdateReceiver` to the import from `ibcore._ibcore` block. Add them to `__all__` list. Follow existing pattern (lines 14-36).

---

### Task 8: Create python/tests/test_order_types.py

**Files:** `python/tests/test_order_types.py` (NEW)
**Dependencies:** [Task 6]
**Parallelizable:** yes
**Description:** Create Python tests for `OpenOrder` and `OrderStatusEvent` types. Tests must include:
- Construction via `#[new]` keyword arguments: `OpenOrder(order_id=1, symbol="SPY")`
- Construction via `#[new]` with partial args: `OpenOrder()` (all defaults)
- Field access (all fields readable)
- `repr()` output contains type name
- `OrderStatusEvent` kind field distinguishes between variants
- Commission-only event: `OrderStatusEvent(kind="Filled", order_id=0, commission=1.50)` — verify `order_id == 0`
- Follow the pattern from `test_snapshots.py` — use `from _ibcore import OpenOrder, OrderStatusEvent` and plain `pytest` assertions.

---

### Task 9: Create python/tests/test_order_receiver.py

**Files:** `python/tests/test_order_receiver.py` (NEW)
**Dependencies:** [Task 6]
**Parallelizable:** yes
**Description:** Create Python tests for `OrderUpdateReceiver`. Test:
- Receiver is iterable: `iter(receiver)` returns itself
- `next()` on empty/closed channel propagates `StopIteration`
- `__next__` return type is `Option` (not raw event)
- Since this depends on a live Gateway for real events, use the same structural pattern as `test_client.py` — test class/method existence, not live IB data.

---

### Task 10: Update python/tests/test_client.py method list

**Files:** `python/tests/test_client.py`
**Dependencies:** [Task 6]
**Parallelizable:** yes
**Description:** In `TestIbClientClass.test_has_methods`, add `"open_orders"` and `"order_updates"` to the methods list (line 17-31). Also add assertion in `test_ib_client_has_correct_attrs` in the Rust tests (around line 970-989). Run `cd python && pytest tests/test_client.py -v` to verify.

---

## Documentation Impact

**README: YES — Section "Python Bindings" needs updating.**

Current state (line 426-427): "Order management (OpenOrder, OrderStatusEvent) is deferred to a follow-up release."

After implementation, change that line to reference `open_orders()` and `order_updates()` with brief usage example, including the `order_id == 0` commission-event check. Additionally, add `open_orders()` and `order_updates()` to the API Overview table (lines 224-240).

---

## Panel Split Recommendation

This feature has 11 tasks (Task 0 + Tasks 1–10) — within the single-panel threshold (≤20). No split needed. Tasks 0–6 are sequential (same files); Tasks 7–10 run in parallel after Task 6. Total 8 sequential "steps" × 5–15 min each = 40–120 min total panel time.

---

## Security Considerations

- **No new auth surface.** All data flows through existing IB Gateway connection — no new network endpoints.
- **No data persistence.** mpsc channel is in-memory only. Background task is dropped when receiver is dropped.
- **GIL safety.** `py.allow_threads()` releases GIL during blocking `recv()` so Python threads aren't starved.
- **Backpressure safety.** Bounded channel (1024) prevents memory exhaustion from fast producers.
- **Error isolation.** Stream `Err` variants are logged, not forwarded — Python never sees internal IB errors from the stream layer.
- **No injection.** String fields (symbol, status, reason) are opaque pass-through from IB — no Python-side interpolation or eval.
