# Mission

ibcore is a standalone Rust crate that wraps the `ibapi` v3.0 IB Gateway
integration layer with a clean async API, typed errors, market data
snapshots, option chain resolution, and diagnostic event broadcasting.

## Purpose

Provide a production-grade IB Gateway integration layer for Rust and Python
users that:

- Smooths over ibapi's raw error codes with typed `IbError` variants
- Exposes structured `DiagnosticEvent`s via broadcast channels
- Provides ergonomic market data snapshots (`StockSnapshot`, `OptionSnapshot`)
- Supports option chain resolution and contract building
- Is extensible with PyO3 Python bindings for broader ecosystem access

## Non-goals

- Re-implementing the IB wire protocol (ibapi handles that)
- Business logic, trading strategies, or position management
- Auto-reconnect logic (consumers decide reconnect strategy)
