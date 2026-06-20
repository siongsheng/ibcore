# Reddit Post Draft — r/algotrading

**Title:** I open-sourced our IBKR API wrapper after 6 months of finding undocumented quirks

**Body:**

Built an automated options trading system on Interactive Brokers over the past 6 months. Along the way we hit every undocumented IBKR API quirk you can imagine — paper accounts getting stub data, contract_details failing for wing strikes, Gateway disconnects mid-tick, corrupt prices for commodity ETFs.

Extracted the API layer into a standalone crate: **ibcore**

[https://crates.io/crates/ibcore](https://crates.io/crates/ibcore) | `pip install ibcore-py`

[https://github.com/siongsheng/ibcore](https://github.com/siongsheng/ibcore)

**What it does:**

- Typed errors instead of raw numeric codes — `IbError::FarmDisconnect`, `IbError::CompetingSession`, etc.
- Diagnostic events broadcast via Tokio — subscribe to Gateway health in real-time
- Option chain resolution, market snapshots with Greeks, order placement, streaming data
- Rust + Python (PyO3 bindings)

**Why this exists:**

The raw IB API gives you numeric error codes with inconsistent docs. ib_insync was the standard Python library (3,300 GitHub stars) but the author passed away in 2024 and the repo is archived. ibcore fills that gap — and adds structured diagnostics that ib_insync never had.

**Python quick start:**

```python
from ibcore import IbClient

ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
snap = ib.stock_snapshot("SPY")
print(f"SPY: ${snap.last:.2f}")
```

128 tests, MIT licensed. Would love feedback from anyone building on IBKR.
