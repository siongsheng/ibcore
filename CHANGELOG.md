# Changelog

All notable changes to ibcore will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0, a breaking change bumps the MINOR version.

## [0.3.0] — 2026-07-14

### Changed
- **BREAKING:** `IbError`, `OrderOutcome`, `OrderStatusEvent`, and `TickEvent`
  are now `#[non_exhaustive]` (#28). Downstream `match` expressions over these
  types must include a wildcard `_` arm; new variants are now additive rather
  than breaking.
- **BREAKING:** a priceless `Filled` order status is treated as non-terminal
  (#28). A fill event that carries no price no longer resolves an order as
  terminally filled — the outcome loop waits for a priced fill (or another
  terminal status) instead.
- Order-failure errors now preserve IB's original numeric error code instead of
  normalizing it, so callers can distinguish rejection causes (#17).
- `option_strikes_for_expiry()` no longer pins the option multiplier to `100`.
  Strike enumeration now returns every listed strike for the expiry regardless
  of multiplier, instead of silently excluding non-standard-multiplier listings
  (#18).

### Added
- `OrderOutcome::Inactive` — a distinct terminal outcome for an order IB
  accepted but is not working (e.g. an invalid order that errored, or an order
  submitted while the market is closed), separate from a hard `Rejected` or an
  explicit `Cancelled` (#17).
- `IbClient::option_strikes_for_expiry()` — enumerate the strikes that actually
  trade for a specific expiry via `contract_details`, so callers pick a
  resolvable strike on the first try instead of walking the union of strikes
  returned by the option chain (#16, #45).
- `IbClient::account_type()` getter, and `IbClient::place_order_await()` which
  places an order and awaits its terminal `OrderOutcome`.
- `contract::DEFAULT_OPTION_MULTIPLIER` — named constant for the standard `"100"`
  option multiplier, documenting that non-standard multipliers (e.g. 50/1000
  index options, post-split contracts) are not currently supported (#18).

### Fixed
- Market-data collectors: stock snapshots use a one-shot snapshot collector
  while option snapshots use a streaming collector, matching each instrument's
  reliable data path.

### Internal
- Extracted the duplicated exchange-fallback + `contract_details` retry loop
  shared by `option_snapshot` and `option_strikes_for_expiry` into one private
  helper (#19).

## [0.2.1] — 2026-06

### Fixed
- Streaming workaround for empty snapshot data on IB Gateway 10.45+ (ibapi
  snapshot error 10197): `stock_snapshot` and option snapshots now use a
  streaming market-data subscription with a timeout instead of the ibapi
  snapshot API. See https://github.com/wboayue/rust-ibapi/issues/683.

### Changed
- Packaging/metadata: added the `readme` field to `Cargo.toml`, pointed
  `homepage` at ibquirk, and added the Discord invite and PyPI project URLs.

## [0.2.0] — 2026-06

### Added
- Trading capabilities: order placement and status streaming, live market-data
  tick streaming, and historical OHLCV bars — `IbClient::tick_stream()`,
  `IbClient::historical_data()`, plus `OrderStatusEvent`, `OrderStatusStream`,
  `OpenOrder`, `TickEvent`, and `TickStream` (#2).
- `ibkr-diag` CLI tool (#1).
- PyO3 Python bindings (the `ibcore` Python package) with maturin packaging,
  exposing the client, snapshots, diagnostics, enums, and order types —
  including `open_orders()`, `order_updates()`, `PyOrderStatusEvent`, and
  `PyOpenOrder` (#4).
- Remote diagnostics behind the opt-in `remote-diagnostics` feature:
  `IbClient::with_remote_diagnostics()` streams diagnostic events to the ibquirk
  API via a batcher/poller with ring-buffer backoff and critical-event flush
  (#7).
- `contract_details` caching on `IbClient` — an in-memory cache keyed by
  contract identity, cleared on reconnect (#5).
- Commission correlation on order fills: `OrderStatusEvent::Filled` gained
  `commission: Option<f64>` and `execution_id: Option<String>` fields to
  correlate fills with their commission reports (#23, #24).
- Zero-price retry on `stock_snapshot()` for competing-session recovery.
- `Serialize`/`Deserialize` derives for `FarmState` and the remote-diagnostics
  types.

### Changed
- **BREAKING (Python):** replaced the flat error type with a 9-class exception
  subclass hierarchy; the `e.category` attribute was removed (#8).
- `DIAGNOSTIC_BUFFER` raised from 256 to 1024 events.

## [0.1.0] — 2026-06-19

### Added
- `IbClient` — persistent IB Gateway client with typed `connect()` and `disconnect()`
- `StockSnapshot` and `OptionSnapshot` — one-shot market data with Greeks (delta, gamma, theta, vega, iv)
- `OptionChainData` — expiry and strike resolution via `fetch_option_chain()`
- `DiagnosticEvent` — structured error/farm/connection events broadcast via `tokio::sync::broadcast`
- `FarmState`, `ConnectionState`, `AccountType` — typed enums with Display impls
- `classify_farm()` — map IB error codes (2104–2108) to `FarmState`
- `IbError` — typed error variants (ConnectionFailed, ConnectionReset, MarketData, FarmDisconnect, CompetingSession, OrderRejected, ContractResolution, Timeout, Other)
- `build_option_contract()` — construct IB `Contract` for options
- `get_primary_exchange()` — symbol→exchange mapping (SPY→CBOE, TLT→SMART, etc.)
- `parse_expiry()` — parse "YYYYMMDD" date strings
- `is_connection_dead()` — detect fatal connection errors for reconnect logic
- Full Rustdoc on all public items
- 80 unit tests

[0.3.0]: https://github.com/siongsheng/ibcore/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/siongsheng/ibcore/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/siongsheng/ibcore/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/siongsheng/ibcore/releases/tag/v0.1.0
