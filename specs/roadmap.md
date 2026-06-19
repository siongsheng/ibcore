# Roadmap

## Phase 1 (current) — Core PyO3 bindings

- [x] Workspace crate `python/` with Cargo.toml
- [x] StockSnapshot, OptionSnapshot pyclasses with `#[getter]`
- [x] DiagnosticEvent pyclass with all 9 fields
- [x] FarmState, ConnectionState, AccountType as string-constant classes
- [x] IbError as Python exception with category, code, message
- [x] IbClient with async connect/disconnect/snapshot/account/order methods
- [x] DiagnosticEventReceiver iterator
- [x] README documentation

## Phase 2 — Streaming, historical, orders

- [ ] TickEvent, TickStream with async iter protocol (`__aiter__` + `__anext__`)
- [ ] Bar, HistoricalData pyclasses
- [ ] OpenOrder, OrderStatusEvent, OrderStatusStream pyclasses
- [ ] `tick_stream()`, `historical_data()`, `open_orders()`, `order_updates()` on IbClient
- [ ] Additional Python unit tests

## Phase 3 — Packaging and distribution

- [ ] maturin project (pyproject.toml) for PyPI publishing
- [ ] CI/CD pipeline for building wheels
- [ ] Python package on PyPI (`ibcore`)
