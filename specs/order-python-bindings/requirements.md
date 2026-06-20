# Requirements: Expose order_updates & open_orders in Python bindings

**Date:** 2026-06-21
**Status:** Draft — Updated 2026-06-21 (orchestrator review: 12 gaps addressed)
**Confidence:** MEDIUM — one unverified GIL-release assumption
**Impact:** MEDIUM — new public Python API surface (2 methods, 2 types, 1 receiver class)

---

## 1. What This Does

Expose the Rust `IbClient::order_updates()` and `IbClient::open_orders()` methods
to Python via PyO3, so Python users can (a) fetch a snapshot of all currently open
orders, and (b) subscribe to a live stream of order status change events.

The Rust layer already has full implementations:

| Rust type/method | Location | Purpose |
|---|---|---|
| `OpenOrder` | `src/orders.rs:27` | One-shot open order snapshot struct |
| `OrderStatusEvent` | `src/orders.rs:54` | Typed order status enum |
| `OrderStatusStream` | `src/orders.rs:103` | Async stream wrapper over ibapi subscription |
| `IbClient::open_orders()` | `src/orders.rs:264` | Fetch Vec<OpenOrder> |
| `IbClient::order_updates()` | `src/orders.rs:251` | Subscribe → OrderStatusStream |

The README (line 426-427) notes: "Order management (OpenOrder, OrderStatusEvent) is deferred to a follow-up release."
This feature fulfills that deferred work.

---

## 2. Scope

### In scope

1. **`PyOpenOrder`** — PyO3 `#[pyclass]` wrapping `OpenOrder`, with `#[pyo3(get)]` fields,
   `#[new]` constructor (all fields as keyword arguments with defaults), and `__repr__`.
2. **`PyOrderStatusEvent`** — PyO3 `#[pyclass]` wrapping `OrderStatusEvent` as a flat
   object (not an enum — Python doesn't have Rust-style enums). Use a `kind: str`
   field + variant-specific fields (some nullable). Has `#[new]` constructor with
   keyword arguments and defaults.
3. **`PyOrderUpdateReceiver`** — PyO3 `#[pyclass]` implementing `__iter__` / `__next__`
   via a background tokio task that reads from `OrderStatusStream` and pushes into
   a `std::sync::mpsc::SyncSender` (bounded channel, capacity 1024).
4. **`PyIbClient::open_orders()`** — calls Rust `open_orders()`, returns `list[dict]`
   (same pattern as `positions()` — dict-per-item).
5. **`PyIbClient::order_updates()`** — calls Rust `order_updates()`, bridges the
   async stream to a bounded mpsc channel, returns `PyOrderUpdateReceiver`.
6. **Re-export `OpenOrder`, `OrderStatusEvent`, `OrderStatusStream`** from `src/lib.rs`
   so the Python bindings can reference `ibcore::OpenOrder` without reaching into
   `ibcore::orders` directly.
7. **Update `__init__.py`** and `__all__` to export the new types.
8. **Python tests** — unit tests for new types (construct, access fields, repr),
   method presence tests on `IbClient`.

### Out of scope

- Async Python API — no `asyncio` / `await` support. The entire Python binding is synchronous.
- Exposing `OrderStatusStream` directly — the stream is internal; Python sees only the receiver.
- `place_order()` / `cancel_order()` — these already exist in Python bindings via `src/lib.rs:488-529`.

---

## 3. User Story

```python
from ibcore import IbClient, OrderStatusEvent

ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")

# (A) Fetch current open orders
open_orders = ib.open_orders()
for o in open_orders:
    print(f"Order #{o['order_id']}: {o['action']} {o['quantity']} {o['symbol']} @ {o['limit_price']} [{o['status']}]")

# (B) Subscribe to live order updates
receiver = ib.order_updates()
for event in receiver:
    if event.order_id == 0 and event.commission is not None:
        print(f"💰 Commission report: ${event.commission:.2f}")
    elif event.kind == "Filled":
        print(f"✅ Order #{event.order_id} filled: {event.filled_qty} @ ${event.avg_price:.2f}")
    elif event.kind == "Rejected":
        print(f"❌ Order #{event.order_id} rejected: {event.reason}")
    elif event.kind == "Cancelled":
        print(f"✕ Order #{event.order_id} cancelled: {event.reason}")
    # loop exits when receiver is dropped or stream ends (disconnect)

ib.disconnect()
```

---

## 4. Constraints

- **No async.** The Python API is synchronous (matches existing `connect`, `stock_snapshot`, etc.).
  `order_updates()` must return an iterable that blocks on `next()`.
- **No new dependencies.** Use `std::sync::mpsc` (stdlib) for the channel bridge — no crossbeam.
- **Follow existing patterns.** Dict-per-item for `open_orders()` (like `positions()`).
  Iterable receiver for `order_updates()` (like `diagnostic_events()` but blocking).
  `__next__` returns `PyResult<Option<PyOrderStatusEvent>>` — matching `PyDiagnosticEventReceiver`'s
  signature.
- **GIL safety.** The blocking `recv()` in `__next__` must call `py.allow_threads()` to release
  the GIL so other Python threads can run while waiting for IB events.
- **Drop safety.** When `PyOrderUpdateReceiver` is dropped (Python GC or explicit), the
  background tokio task must be cancelled and the mpsc sender dropped so `recv()` unblocks.
  Requires an explicit `impl Drop` calling `self._task.take().map(|h| h.abort())`.
- **Bounded channel.** Use `std::sync::mpsc::sync_channel(1024)` to cap memory. If IB emits
  events faster than Python consumes, `try_send` returns `Full` → log `tracing::warn!` and
  skip the event (oldest events preserved, newest dropped on backpressure).
- **Stream error handling.** `OrderStatusStream::next()` returns `Option<Result<OrderStatusEvent, IbError>>`.
  `Err(IbError)` variants must be logged via `tracing::warn!` and the background task must
  continue (do NOT forward errors to Python — the mpsc carries only `PyOrderStatusEvent`).
- **Re-exports.** `src/lib.rs` must add `pub use orders::{OpenOrder, OrderStatusEvent, OrderStatusStream};`
  so the Python crate can import `ibcore::OpenOrder` etc. without private-module path tricks.
- **`#[new]` constructors.** Both `PyOpenOrder` and `PyOrderStatusEvent` must have `#[new]`
  with all fields as keyword arguments (using `#[pyo3(signature = (...))]`). All fields
  have sensible defaults (0, "", None) so `OrderStatusEvent(kind="Filled", order_id=42)`
  compiles. This is required for validation C2 and C3 which construct from Python.

---

## 5. Edge Cases

| Scenario | Expected behaviour |
|---|---|
| `order_updates()` called when not connected | `IbError` raised ("not connected") |
| `open_orders()` called when not connected | `IbError` raised ("not connected") |
| IB Gateway has no open orders | `open_orders()` returns empty list `[]` |
| Stream ends (Gateway disconnect) | `__next__` raises `StopIteration` |
| Receiver dropped mid-stream | Background task cancelled via `Drop::abort()`, tokio subscription dropped |
| Multiple concurrent receivers | Only one `order_update_stream` subscription per IB Gateway. Second call returns `IbError` from IB. No deadlock: the mutex is released between calls (see §6 assumption 2). |
| `open_orders()` called after disconnect | `IbError` raised |
| `order_updates()` on IB Gateway without "Download open orders" enabled | May return empty or partial data — IB limitation, not ibcore's |
| Stream yields `Err(IbError)` | Logged via `tracing::warn!`; background task continues. Python never sees it. |
| mpsc channel full (1024 pending events) | `try_send` returns `Full` → logged via `tracing::warn!`; event dropped (backpressure). Python keeps newest-before-overflow events. |
| `PyIbClient` dropped while `PyOrderUpdateReceiver` alive | Runtime `Arc` refcount drops → tokio runtime shuts down → spawned tasks cancelled → mpsc sender dropped → `recv()` unblocks with `RecvError` → `StopIteration`. Transition is clean. |
| CommissionReport event from IB (order_id not available) | Python sees `PyOrderStatusEvent { kind: "Filled", order_id: 0, filled_qty: 0.0, avg_price: 0.0, commission: Some(x.xx) }`. Users should check `order_id == 0 && commission is not None` to identify commission-only events. |
| `KeyboardInterrupt` (Ctrl+C) during `next()` | Signal is delivered while GIL is released inside `allow_threads`. However, `recv()` is an uninterruptible OS-level blocking call — `KeyboardInterrupt` will NOT interrupt it. The exception is raised only AFTER the next event arrives and the GIL is re-acquired. This means Ctrl+C has a delayed effect (user must wait for an event, or disconnect the Gateway to unblock). |
| `next()` with active stream but no events yet | Blocks indefinitely. This is by design — the receiver is a long-lived blocking iterator for polling loops, not a time-bounded operation. Users who need timeout behaviour should iterate in a thread and signal externally. |

---

## 6. Assumptions (unverified)

1. **GIL release with blocking mpsc:** `py.allow_threads(|| rx.recv())` works correctly inside a PyO3 `#[pymethods]` `__next__` when a tokio runtime is running in another thread. **(MEDIUM — standard PyO3 pattern but untested in this codebase)**

2. **No mutex deadlock on second `order_updates()` call.** `order_updates()` calls `block_on(client.order_updates())` inside `with_client` which holds `std::sync::Mutex`. The mutex is released BEFORE `order_updates()` returns to Python. Two sequential calls: first acquires+releases, second acquires+releases — no deadlock. The second call reaches IB which rejects duplicate subscriptions → returns `IbError`. **(HIGH — std::sync::Mutex release semantics guarantee this)**

3. **OrderStatusStream survives move to `tokio::spawn`:** The inner `BoxStream<'static, ...>` is `Send + 'static`. The ibapi subscription it wraps survives across tasks — same pattern as the diagnostic notice stream. **(HIGH — verified by existing diagnostic task pattern in client.rs:135-158)**

4. **Bounded channel capacity 1024 is sufficient.** If IB emits >1024 order events before Python consumer catches up, oldest events are preserved; new events are dropped with a warning. For paper trading at typical rates (tens of events per second), 1024 provides ~30-60 seconds of buffer. **(MEDIUM — untuned; adjust if production shows warnings)**

---

## 7. GIL-Release Fallback Plan

The primary implementation uses `py.allow_threads(|| rx.recv())` in `__next__`. If this proves broken in production (panics, deadlocks, or runtime conflicts), a manual fallback exists:

```rust
// GIL_FALLBACK: If allow_threads causes issues, replace the __next__ body with:
//   loop {
//       match self.rx.try_recv() {
//           Ok(event) => return Ok(Some(event)),
//           Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(10)),
//           Err(TryRecvError::Disconnected) => return Ok(None),
//       }
//   }
```

This fallback is a **manual code change** — no compile-time feature flag or runtime detection. The marker comment `// GIL_FALLBACK:` in the source is the trigger for a future maintainer. It is not automated because the failure mode is unknown and may manifest differently across platforms.

---

## 8. Security Considerations

- **No order data exposure beyond the API.** The binding returns only data that already exists in the Rust layer — no new information channels.
- **No credentials.** Order data flows through in-memory mpsc channel only — no disk, no network.
- **No injection vectors.** `symbol` and other string fields are opaque pass-through from IB — no Python-side interpolation.
