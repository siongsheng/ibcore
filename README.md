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

### Rust

**Prerequisites**

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
use ibcore::IbClient;

let snap = ib.option_snapshot(
    "SPY",
    (2025, 7, 17),  // expiry_ymd: (year, month, day)
    570.0,           // strike
    true,            // is_call (true = Call, false = Put)
    0.0,             // _implied_vol (reserved)
    0.0,             // _underlying_price (reserved)
    "CBOE",          // exchange
).await?;
println!(
    "SPY 570C: bid={:.2}, ask={:.2}, delta={:.4}, theta={:.4}, iv={:.4}",
    snap.bid, snap.ask, snap.option_delta, snap.option_theta, snap.option_iv
);
```

### Subscribe to live market data ticks

```rust
use futures::StreamExt;

let contract = ibcore::Contract::stock("SPY").build();
let mut stream = ib.tick_stream(&contract).await?;
while let Some(event) = stream.next().await {
    match event? {
        ibcore::TickEvent::Price { tick_type, price } => {
            println!("{tick_type:?}: ${price:.2}");
        }
        ibcore::TickEvent::Greeks { delta, theta, implied_volatility, .. } => {
            println!("delta={delta:.4}, theta={theta:.4}, iv={implied_volatility:.4}");
        }
        _ => {} // ignore Size, String, Generic, etc.
    }
}
```

### Fetch historical OHLCV bars

```rust
let contract = ibcore::Contract::stock("SPY").build();
let data = ib.historical_data(
    &contract,
    ibcore::BarSize::Hour,
    ibcore::Duration::days(5),
    ibcore::WhatToShow::Trades,
    ibcore::TradingHours::Regular,
).await?;
println!("Period: {} to {} — {} bars", data.start, data.end, data.bars.len());
for bar in &data.bars {
    println!("O={:.2}, H={:.2}, L={:.2}, C={:.2}, V={:.0}",
        bar.open, bar.high, bar.low, bar.close, bar.volume);
}
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

### Python

ibcore is also available as a Python package via PyO3 bindings.

**Prerequisites**

- Python 3.9+
- Rust toolchain (for maturin build)

**Installation**

```bash
pip install ibcore
```

**Connect and get a stock snapshot**

```python
from ibcore import IbClient

ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
snap = ib.stock_snapshot("SPY")
print(f"SPY last: ${snap.last:.2f}, bid: ${snap.bid:.2f}, ask: ${snap.ask:.2f}")
ib.disconnect()
```

**Get an option snapshot with Greeks**

```python
snap = ib.option_snapshot("SPY", 2025, 7, 17, 570.0, True, "CBOE")
print(f"delta={snap.option_delta:.4f}, theta={snap.option_theta:.4f}, iv={snap.option_iv:.4f}")
```

**Place an order**

```python
order_id = ib.place_order("SPY", "BUY", 100.0, "MKT", None, "SMART")
print(f"Order placed: {order_id}")
```

**Build from source**

```bash
cd python && pip install maturin && maturin develop --release
```

## CLI Tool

ibcore ships a diagnostic CLI tool for Gateway health checks:

```bash
# Build from source
cargo build -p ibkr-diag --release

# Check versions
./target/release/ibkr-diag version

# Run a 10-second diagnostic against local Gateway
./target/release/ibkr-diag diagnose --duration 10

# JSON output for scripting
./target/release/ibkr-diag diagnose --json | jq '.farm_state_counts'
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
| `tick_stream(contract)` | `Result<TickStream, IbError>` | Subscribe to live market data ticks |
| `historical_data(contract, bar_size, duration, what_to_show, trading_hours)` | `Result<HistoricalData, IbError>` | One-shot historical OHLCV bars |

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

## Python Bindings

ibcore ships PyO3 Python bindings in the `python/` workspace crate (`ibcore-python`).
Build the shared library with:

```bash
# Requires Python 3.7+ and Rust 1.85+
PYO3_PYTHON=$(which python3) cargo build -p ibcore-python --release
```

The resulting `.so` file is at `target/release/libibcore_python.so`.

### Usage

```python
import asyncio
from ibcore._ibcore import (
    IbClient, StockSnapshot, OptionSnapshot,
    DiagnosticEvent, FarmState, ConnectionState,
    AccountType, DiagnosticEventReceiver,
)

async def main():
    # Connect to paper trading Gateway
    client = await IbClient.connect(
        "127.0.0.1", 4002, 1, "delayed", "paper",
    )

    # Stock snapshot
    snap = await client.stock_snapshot("SPY")
    print(f"SPY: last={snap.last}, bid={snap.bid}, ask={snap.ask}")

    # Option snapshot
    opt = await client.option_snapshot(
        "SPY", 2026, 7, 17, 570.0, True, "CBOE",
    )
    print(f"SPY 570C: delta={opt.option_delta}, iv={opt.option_iv}")

    # Account data
    nl = await client.net_liquidation("")
    print(f"Net Liq: ${nl:.2f}")

    # Positions
    positions = await client.positions()
    for p in positions:
        print(f"  {p['symbol']}: {p['quantity']} @ {p['avg_cost']}")

    # Diagnostics
    receiver = client.diagnostic_events()
    for event in receiver:
        if event.farm_status == FarmState.WARNING:
            print(f"⚠️  {event.error_message}")

    await client.disconnect()

asyncio.run(main())
```

### Async context manager

```python
async with await IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper") as ib:
    snap = await ib.stock_snapshot("SPY")
    print(snap)
```

### Deferred types (Phase 2)

Order management (`OpenOrder`, `OrderStatusEvent`) is deferred to a
follow-up release.

## Related

- [ibquirk](https://ibquirk.com) — AI bot that diagnoses IBKR API problems
  using ibcore's DiagnosticEvents (launching soon)

## License

MIT — see [LICENSE](LICENSE).

ibcore is not affiliated with Interactive Brokers Group, Inc.
