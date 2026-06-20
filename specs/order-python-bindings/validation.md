# Validation: Expose order_updates & open_orders in Python bindings

**Date:** 2026-06-21
**Status:** Draft — Updated 2026-06-21 (orchestrator review: 12 gaps addressed)

---

## Success Criteria

These are testable, binary (pass/fail) criteria. All must pass before considering the feature complete.

### C1: Types exist and are importable (from both _ibcore and ibcore)

```python
from ibcore._ibcore import OpenOrder, OrderStatusEvent, OrderUpdateReceiver
```

**Verify:** `cd python && python -c "from ibcore._ibcore import OpenOrder, OrderStatusEvent, OrderUpdateReceiver; print('OK')"`

Also verify the public re-export:
```python
from ibcore import OpenOrder, OrderStatusEvent, OrderUpdateReceiver
```
**Verify:** `cd python && python -c "from ibcore import OpenOrder, OrderStatusEvent, OrderUpdateReceiver; print('OK')"`

### C2: OpenOrder constructed from Python with keyword arguments (tests #[new])

```python
from ibcore._ibcore import OpenOrder

# Full construction
o = OpenOrder(order_id=1, symbol="SPY", action="BUY", quantity=100.0,
              order_type="LMT", limit_price=450.0, status="Submitted",
              filled_qty=0.0, remaining_qty=100.0)
assert o.order_id == 1
assert o.symbol == "SPY"
assert o.limit_price == 450.0
assert "OpenOrder" in repr(o)

# Default construction (all fields defaulted)
o2 = OpenOrder()
assert o2.order_id == 0
assert o2.symbol == ""
assert o2.limit_price is None

# Partial construction
o3 = OpenOrder(order_id=42, symbol="QQQ")
assert o3.order_id == 42
assert o3.symbol == "QQQ"
```

### C3: OrderStatusEvent constructed from Python with keyword arguments (tests #[new])

```python
from ibcore._ibcore import OrderStatusEvent

# Filled variant
e = OrderStatusEvent(kind="Filled", order_id=42, filled_qty=50.0,
                      avg_price=450.25, commission=1.50,
                      reason="", status="")
assert e.kind == "Filled"
assert e.filled_qty == 50.0
assert "OrderStatusEvent" in repr(e)

# Default construction
e2 = OrderStatusEvent()
assert e2.kind == ""
assert e2.order_id == 0
assert e2.filled_qty == 0.0
assert e2.commission is None

# Partial construction
e3 = OrderStatusEvent(kind="Submitted", order_id=99)
assert e3.kind == "Submitted"
assert e3.order_id == 99
```

### C4: IbClient has open_orders and order_updates methods

```python
from ibcore._ibcore import IbClient
assert hasattr(IbClient, "open_orders")
assert hasattr(IbClient, "order_updates")
```

### C5: open_orders() returns list of dicts (with live Gateway)

```python
from ibcore import IbClient
ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
orders = ib.open_orders()
assert isinstance(orders, list)
if orders:
    o = orders[0]
    assert "order_id" in o
    assert "symbol" in o
    assert "action" in o
    assert "limit_price" in o  # may be None
ib.disconnect()
```

### C6: order_updates() returns iterable receiver (with live Gateway)

```python
from ibcore import IbClient
ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
receiver = ib.order_updates()
assert hasattr(receiver, "__iter__")
assert hasattr(receiver, "__next__")
# Verify iter() returns self
assert iter(receiver) is receiver
ib.disconnect()
```

### C7: Rust tests pass

```bash
cargo test -p ibcore-python --lib
```

Expected: all existing tests pass + new Rust-side tests pass.

### C8: Python tests pass

```bash
cd python && pytest tests/ -v
```

Expected: all existing tests pass + new test files pass.

### C9: Build succeeds

```bash
cargo build -p ibcore-python
PYO3_PYTHON=$(which python3) cargo build -p ibcore-python --release
```

Expected: clean compilation, no warnings.

### C10: Exports in __init__.py

```python
from ibcore import OpenOrder, OrderStatusEvent, OrderUpdateReceiver
```

Expected: no ImportError.

### C11: Re-exports in src/lib.rs compile

```bash
cargo check
```

Verify that `use ibcore::OpenOrder` and `use ibcore::OrderStatusEvent` compile from the Python crate (no `pub(crate)` visibility errors). This validates Task 0.

### C12: Commission-only events have order_id=0

```python
from ibcore._ibcore import OrderStatusEvent
# Simulate the CommissionReport mapping: kind="Filled", order_id=0, commission set
e = OrderStatusEvent(kind="Filled", order_id=0, commission=1.50)
assert e.order_id == 0
assert e.commission == 1.50
assert e.filled_qty == 0.0
# User pattern for detecting commission events
is_commission = e.order_id == 0 and e.commission is not None
assert is_commission is True
```

### C13: __next__ returns PyResult wrapped Option (type check)

```python
from ibcore._ibcore import OrderUpdateReceiver
import inspect
sig = inspect.signature(OrderUpdateReceiver.__next__)
# Return annotation should exist (PyResult<Option<PyOrderStatusEvent>>)
# We can't introspect the Rust return type from Python, but we can verify
# that calling next() on a disconnected receiver returns None (not raises)
# This is validated in C15 below.
```

### C14: GIL_FALLBACK comment exists in source

```bash
grep -n "GIL_FALLBACK" python/src/lib.rs
```

Expected: at least one match near the `__next__` implementation, documenting the manual fallback path.

### C15: Drop behaviour — receiver.next() returns None after client disconnect

```python
from ibcore import IbClient
ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
receiver = ib.order_updates()
ib.disconnect()
# After disconnect, the stream ends → channel disconnects → next() returns None
import _ibcore
# StopIteration should be raised when None is returned
try:
    # This may block briefly until the background task notices the disconnection
    # and drops the sender — typically within a few seconds
    event = receiver.__next__()
    # If it returns None, Python's iteration protocol raises StopIteration
    # If it hasn't disconnected yet, event will be Some — loop and retry
except StopIteration:
    pass  # expected
```

---

## Manual Verification (not automatable without Gateway)

| Check | How |
|-------|-----|
| open_orders returns actual IB data | Run against live paper Gateway with open orders |
| order_updates receives real events | Place order → verify receiver yields Submitted → Filled |
| Commission-only event has order_id=0 | Place order → wait for fill → verify commission event has order_id=0 |
| Receiver stops on disconnect | Disconnect Gateway → verify StopIteration within ~5s |
| Multiple receivers rejected | Call order_updates() twice → verify IbError (no deadlock) |
| KeyboardInterrupt during next() is delayed | Ctrl+C while waiting → verify exception only after next event arrives |
| Bounded channel backpressure | (Stress test) Submit many orders rapidly → verify no memory growth, warn log appears |
| Drop aborts background task | Create receiver, drop it, verify tokio task count decreases |

---

## Regression Checks

- [ ] `cargo test --lib` — all 124+ Rust tests still pass
- [ ] `cd python && pytest tests/ -v` — all existing Python tests still pass
- [ ] `cargo clippy -- -D warnings` — no new warnings
- [ ] `cargo build` — no new compilation errors
- [ ] `grep -c "pub use orders" src/lib.rs` — re-export line exists (Task 0)
- [ ] `grep -c "#\[new\]" python/src/lib.rs` — at least 2 new #[new] blocks (for PyOpenOrder and PyOrderStatusEvent)
- [ ] `grep -c "sync_channel" python/src/lib.rs` — bounded channel used (not `channel()`)
- [ ] `grep -c "impl Drop for PyOrderUpdateReceiver" python/src/lib.rs` — explicit Drop impl exists
- [ ] `grep -c "tracing::warn" python/src/lib.rs` — error logging exists (for stream errors and channel-full)
