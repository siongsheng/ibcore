# Changelog

All notable changes to ibcore will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0, a breaking change bumps the MINOR version.

## [0.3.1] — 2026-07-15

Error-fidelity pass (#32, #27). No public signature changes; all changes below
are behavioral and require a wildcard `_` arm on `IbError` (already
`#[non_exhaustive]` since 0.3.0).

### Changed
- **BREAKING (behavioral):** order submit/cancel failures (`place_order`, the
  initial submit in `place_order_await`, `cancel_order`) are now classified like
  the stream path (#20). A transport-level failure — a mid-flight connection
  drop, or a Notice with a connection code (502/504/506/507/100) — surfaces as a
  connection-dead variant (`is_connection_dead` true) instead of a false
  `OrderRejected`, so callers reconcile rather than assuming the order died. A
  genuine broker rejection Notice (any non-transport code, including out-of-range
  codes like 110/434, not just 200–299) still maps to `OrderRejected` preserving
  its code and advanced-reject JSON (#32).
- **BREAKING (behavioral):** market-data snapshot exhaustion now distinguishes a
  plain timeout from a competing session — a snapshot that received no ticks
  returns `IbError::Timeout`, and `IbError::CompetingSession` is reserved for the
  true 10197 signature (ticks arrived but all prices zero). A stream error
  recorded during collection is surfaced instead of a fabricated timeout (#27).
- **BREAKING (behavioral):** `positions()` and `account_summary()` now return
  `Err` when the terminating End sentinel was not observed or a stream error
  occurred, instead of returning truncated data as `Ok`; a clean-but-empty result
  is still `Ok` (#27).
- **BREAKING (behavioral):** `net_liquidation()` returns `Err` when the
  `NetLiquidation` tag is absent from a cleanly-completed summary or present but
  unparseable, instead of a phantom `Ok(0.0)` that could mis-size orders (#27).
- Call-site context (e.g. the symbol) is now preserved in classified
  market-data errors, not just the unclassifiable fallback (#32).

### Fixed
- Option greeks/IV are now sourced with model-tick precedence: only a MODEL
  computation tick (`ModelOption`/`DelayedModelOption`) sets the greeks a
  downstream consumer expects, and a later bid/ask computation can no longer
  overwrite it.
  Placeholder computation ticks (IB's `f64::MAX` "not yet computed", decoded to
  `None`) are skipped so they neither lock out a real tick nor zero previously
  populated greeks. Greeks remain `f64` (`Option<f64>` deferred to a follow-up)
  (#27).

### Internal
- Snapshot collectors return a `SnapshotAttempt { snap, saw_tick, last_error }`
  and use a `select!` deadline so the timeout/competing/error decision survives
  the timeout boundary; the decision is a pure, unit-tested classifier.
- Snapshot-timeout tests use `tokio::time` virtual time (`start_paused`) instead
  of real sleeps.

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
- **BREAKING (behavioral):** `place_order_await` now returns `OrderOutcome::Pending`
  (not `Rejected`) when the order subscription errors from a transport failure
  (e.g. a mid-flight connection drop); only a proven IB `OrderRejected` maps to
  `Rejected`. Callers reconcile a `Pending` via the order-status stream instead
  of assuming the order was refused (#20).
- Market-data subscribe/fetch failures are now classified through `IbError::from`
  (preserving `ConnectionReset`/`ConnectionFailed` so `is_connection_dead` works)
  instead of being flattened to `MarketData { code: 0 }` (#21).
- `CommissionReport` now maps to the new `OrderStatusEvent::Commission` variant;
  consequently `OrderStatusEvent::Filled`'s `commission`/`execution_id` fields
  are now always `None` (correlate via `Execution`/`Commission` instead) (#23).
- Option snapshots now require a usable price **and** greeks before an early
  return, eliminating a spurious `CompetingSession` error when greeks arrive
  before price ticks (#24).

### Added
- `OrderStatusEvent::Execution` and `OrderStatusEvent::Commission` — surface IB
  execution reports (carrying `order_id` + `execution_id`) and commission
  reports (`execution_id` + `commission`) so a consumer can join a commission
  back to the order that generated it (#23).
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

[0.3.1]: https://github.com/siongsheng/ibcore/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/siongsheng/ibcore/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/siongsheng/ibcore/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/siongsheng/ibcore/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/siongsheng/ibcore/releases/tag/v0.1.0
