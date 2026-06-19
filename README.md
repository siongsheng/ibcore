# ibcore

Standalone Rust crate for integrating with Interactive Brokers TWS and IB Gateway.

Wraps [`ibapi`] v3.0 with a clean async API, typed errors, market data
snapshots, option chain resolution, and diagnostic event broadcasting.

[`ibapi`]: https://crates.io/crates/ibapi

## Why ibcore?

The IB API has quirks. ibcore smooths them over:

| Raw ibapi | ibcore |
|---|---|
| Numeric error codes with inconsistent docs | Typed `IbError::FarmDisconnect` |
| Error codes in notice stream | `tokio::sync::broadcast` of `DiagnosticEvent` |
| Manual contract construction | `build_option_contract`, `get_primary_exchange` |
| No structured disconnect detection | `is_connection_dead()` for reconnect logic |
| Raw `TickType` decoding | `StockSnapshot` / `OptionSnapshot` with Greeks |

ibcore is built from production experience running automated options trading
against IBKR. Every reconnect edge case, every data farm quirk, every
undocumented behavior discovered in live trading is handled here.

## Quick Start

### Prerequisites

- Rust 1.85+ (edition 2024)
- IB Gateway or TWS running with API enabled

### Installation

```bash
cargo add ibcore
```

### Connect and get a stock snapshot

```rust
use ibcore::{IbClient, AccountType};

#[tokio::main]
async fn main() -> Result<(), ibcore::IbError> {
    // Connect to paper trading Gateway on port 4002
    let ib = IbClient::connect(
        "127.0.0.1",
        4002,
        1,                    // client_id
        "delayed",            // market_data_type: "delayed" or "realtime"
        AccountType::Paper,
    )
    .await?;

    // Get a stock snapshot
    let snap = ib.stock_snapshot("SPY").await?;
    println!("SPY last: ${:.2}, bid: ${:.2}, ask: ${:.2}",
        snap.last, snap.bid, snap.ask);

    ib.disconnect().await;
    Ok(())
}
```

### Get an option chain

```rust
use ibcore::IbClient;

let chain = ib.fetch_option_chain("SPY").await?;
println!("SPY expirations: {:?}", chain.expirations);
println!("SPY strikes: {:?}", chain.strikes);
```

### Get an option snapshot with Greeks

```rust
let snap = ib.option_snapshot("SPY", "20260717", 570.0, "C").await?;
println!(
    "SPY 570C: bid={:.2}, ask={:.2}, delta={:.4}, theta={:.4}, iv={:.4}",
    snap.bid, snap.ask, snap.delta, snap.theta, snap.iv
);
```

### Monitor diagnostic events

```rust
use tokio::task;

let mut rx = ib.diagnostic_events();

let watcher = task::spawn(async move {
    while let Ok(event) = rx.recv().await {
        match event.farm_status {
            ibcore::FarmState::Inactive => {
                eprintln!("⚠️  Data farm inactive: code {}", event.error_code);
            }
            ibcore::FarmState::Warning => {
                eprintln!("⚠️  Data farm warning: {}", event.error_message);
            }
            _ => {}
        }
    }
});
```

## API Overview

### `IbClient`

| Method | Returns | Description |
|---|---|---|
| `connect(host, port, client_id, market_data_type, account_type)` | `Result<IbClient, IbError>` | Connect to IB Gateway with typed config |
| `disconnect()` | `()` | Graceful disconnect |
| `stock_snapshot(symbol)` | `Result<StockSnapshot, IbError>` | One-shot bid/ask/last/volume |
| `option_snapshot(symbol, expiry, strike, right)` | `Result<OptionSnapshot, IbError>` | Bid/ask/last + Greeks (delta, gamma, theta, vega, iv) |
| `fetch_option_chain(symbol)` | `Result<OptionChainData, IbError>` | All expirations + all strikes |
| `positions()` | `Result<Vec<Position>, IbError>` | Current positions |
| `account_summary(tag, currency)` | `Result<Vec<AccountSummaryResult>, IbError>` | NetLiq, GrossPosValue, etc. |
| `pnl(account_id, model_code)` | `Result<PnL, IbError>` | Realized + unrealized P&L |
| `net_liquidation(account_id, currency)` | `Result<f64, IbError>` | Single net liquidation value |
| `diagnostic_events()` | `broadcast::Receiver<DiagnosticEvent>` | Subscribe to structured diagnostic events |
| `server_version()` | `i32` | Gateway server version |

### `DiagnosticEvent`

Emitted on every IB error/warning notice:

```rust
pub struct DiagnosticEvent {
    pub gateway_version: i32,
    pub error_code: i32,
    pub error_message: String,
    pub error_time: Option<time::OffsetDateTime>,
    pub farm_status: FarmState,          // Ok | Warning | Inactive | Unknown(i32)
    pub connection_state: ConnectionState, // Connected | Disconnected | Reconnecting
    pub account_type: AccountType,       // Live | Paper
    pub os: &'static str,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}
```

Subscribe with `ib.diagnostic_events()` and process asynchronously.
Buffer size is 256 events; slow subscribers will miss old events.

### `IbError`

Typed error variants — no raw error codes in your business logic:

| Variant | Trigger | Typical IB codes |
|---|---|---|
| `ConnectionFailed` | Can't connect | 502, 504, 506 |
| `ConnectionReset` | TCP reset | 507, 100 |
| `Timeout` | IO timeout | — |
| `MarketData` | Data subscription error | 10000–10999 |
| `FarmDisconnect` | Data farm offline | 2107 |
| `CompetingSession` | Live session blocks paper | 10197 |
| `OrderRejected` | Order invalid | 200–299 |
| `ContractResolution` | Contract not found | — |
| `Other` | Unclassified | everything else |

### Helper functions

| Function | Description |
|---|---|
| `build_option_contract(symbol, expiry, strike, right)` | Construct IB `Contract` for options |
| `get_primary_exchange(symbol)` | Map symbol to exchange (SPY→CBOE, TLT→SMART) |
| `parse_expiry(date_str)` | Parse "20260717" to `NaiveDate` |
| `is_connection_dead(&IbError)` | Check if error means the connection is gone |
| `classify_farm(error_code)` | Map IB error code to `FarmState` |

## Reconnection

ibcore does NOT auto-reconnect. It gives you the tools to decide:

```rust
loop {
    match IbClient::connect(host, port, id, md_type, acct_type).await {
        Ok(ib) => {
            // ... run your strategy ...
            // When the connection dies:
            break;
        }
        Err(e) if is_connection_dead(&e) => {
            tracing::warn!("Gateway unreachable, retrying in 5s: {e}");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        Err(e) => return Err(e), // non-connection error
    }
}
```

This is deliberate. Your reconnect strategy depends on your trading logic.
ibcore tells you WHAT went wrong; you decide HOW to recover.

## Setup

### IB Gateway (Docker)

```bash
docker run -d \
  --name ib-gateway \
  -p 4002:4002 \
  -e TWS_USERID=your_username \
  -e TWS_PASSWORD=your_password \
  -e TRADING_MODE=paper \
  ghcr.io/unusualalpha/ib-gateway-docker:latest
```

### IB Gateway (manual)

1. Download IB Gateway from [IBKR](https://www.interactivebrokers.com/en/trading/ib-api.php)
2. Configure: API → Enable ActiveX and Socket Clients → Port 4002
3. Check "Download open orders on connection"
4. Start Gateway and log in

### Verify connectivity

```bash
# Check Gateway is listening
nc -zv 127.0.0.1 4002

# Run ibcore tests (requires Gateway)
cargo test --lib
```

## Design Philosophy

- **Wrap, don't re-implement.** ibcore wraps `ibapi` v3.0, adding ergonomics
  and safety on top. All wire-protocol details stay in `ibapi`.
- **Type errors, don't propagate codes.** `IbError` is exhaustive. If you match
  on it, you handle every failure mode the Gateway can produce.
- **Observe, don't guess.** The `DiagnosticEvent` broadcast channel lets you
  monitor Gateway health without polling. Multiple consumers can subscribe
  independently.
- **No business logic.** ibcore knows about IBKR, not about trading strategies.
  Position tracking, portfolio allocation, risk management — those belong in
  your application layer.

## Related

- [ibquirk](https://ibquirk.com) — AI bot that diagnoses IBKR API problems
  using ibcore's DiagnosticEvents (launching soon)

## License

MIT — see [LICENSE](LICENSE).

ibcore is not affiliated with Interactive Brokers Group, Inc.
