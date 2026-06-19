# Changelog

All notable changes to ibcore will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/siongsheng/ibcore/releases/tag/v0.1.0
