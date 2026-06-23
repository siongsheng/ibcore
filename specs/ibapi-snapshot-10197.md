# Bug Report: ibapi `snapshot()` returns 10197 "competing session" on all Gateway versions

## Summary

`market_data().snapshot().subscribe().await` returns error 10197 ("competing live session") on every call, across all Gateway versions and account types. The TWS API itself works — `ib_insync` (Python) successfully retrieves snapshot data through the same Gateway on the same port and contract. The bug is isolated to the `ibapi` Rust crate.

## Versions Affected

| Component | Version | Result |
|-----------|---------|--------|
| ibapi crate | 3.0 (default) | 10197 |
| ibapi crate | 3.1.0 (latest) | 10197 |
| Gateway (Gonzoz docker) | 10.45.1g | 10197 |
| Gateway (Gonzoz docker) | 10.47.1d | 10197 |
| Account type | Paper | 10197 |
| Account type | Live (READ_ONLY_API=yes) | 10197 |

**No combination works.** The error is deterministic — it appears on the first `snapshot()` call, not intermittent.

## Minimal Reproduction (Rust)

```rust
use ibapi::Client;

#[tokio::main]
async fn main() {
    let client = Client::connect("127.0.0.1:4002", 999).await.unwrap();
    let contract = Contract::stock("SPY").build();
    let sub = client
        .market_data(&contract)
        .snapshot()
        .subscribe()
        .await;  // ERR: 10197 "competing session"
}
```

## Isolation Proof: ib_insync Works

Same Gateway, same port, same contract, same machine:

```python
from ib_insync import *
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=999)
contract = Stock('SPY', 'SMART', 'USD')
ib.reqMktData(contract, '', True)  # snapshot=True
ib.sleep(3)
print(ib.ticker(contract))
# Output: bid=743.71, ask=743.73, last=743.72, close=746.74
ib.disconnect()
```

The TWS API protocol supports snapshots. The Gateway serves snapshots correctly. The Rust ibapi client fails to request them without triggering a competing-session error.

## Call Chain

```
ibcore::client::stock_snapshot_inner()   ← thin wrapper
  → self.inner.market_data(&contract)    ← ibapi
    .snapshot()                          ← ibapi (generates request)
    .subscribe()                         ← ibapi
    .await                               ← ibapi async
```

All three method calls on that chain are ibapi crate internals. ibcore only builds a `Contract` struct and decodes the resulting tick stream.

## Error Details

- **Error code:** 10197
- **Message:** "competing session" or "competing live session"
- **Timing:** Immediate — no timeout, no retry
- **Gateway state:** Market Data Farm displays as connected/healthy (green "usfarm") while the error occurs
- **Concurrent sessions:** No other client connected. Single clientId. Single Gateway instance.

## Environment

- Gateway: Gnzsnz/ib-gateway Docker image (latest stable + latest)
- TWS_ACCEPT_INCOMING=accept
- BYPASS_WARNING=yes
- Rust: edition 2024, tokio async runtime
- Also reproduced with bare `ibapi` 3.0 and 3.1.0 outside ibcore

## Previous Related Fixes (for reference)

Two snapshot-related commits exist in the repo but do not resolve this:

- `7eed1cd` (May 2025): Fix subscription cancellation for snapshots after SnapshotEnd (error 300)
- `590c587` (Aug 2025): Fix SnapshotEnd message routing in MockGateway (wire protocol ID 17→57)

Neither addresses the 10197 competing-session error on subscription initiation.

## Impact

This blocks all market data snapshot functionality in ibcore and any downstream crate using ibapi for snapshot market data. The Python ib_insync library provides a working reference implementation.
