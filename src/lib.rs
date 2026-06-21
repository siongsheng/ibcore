//! `ibcore` — Standalone IB Gateway integration layer.
//!
//! Wraps [`ibapi::Client`] in a newtype [`IbClient`] that provides:
//!
//! - **Market data snapshots** — [`StockSnapshot`], [`OptionSnapshot`] for one-shot
//!   bid/ask/last/Greeks data via [`IbClient::stock_snapshot`] and
//!   [`IbClient::option_snapshot`].
//! - **Order-agnostic account data** — [`IbClient::positions`], [`IbClient::account_summary`],
//!   [`IbClient::pnl`], [`IbClient::net_liquidation`].
//! - **Option chain resolution** — [`IbClient::fetch_option_chain`] returns
//!   [`OptionChainData`] (expirations + strikes).
//! - **Market data streaming** — [`TickEvent`], [`TickStream`] for real-time
//!   market data ticks via [`IbClient::tick_stream`].
//! - **Historical data** — [`IbClient::historical_data`] returns [`HistoricalData`]
//!   with OHLCV [`Bar`]s. Types re-exported from [`historical`] module.
//! - **Structured errors** — [`IbError`] translates raw IB error codes into typed
//!   variants instead of letting raw `Notice` codes escape.
//! - **Diagnostic events** — [`DiagnosticEvent`] emitted via `tokio::sync::broadcast`
//!   on the underlying [`ibapi::Client`]'s notice stream, carrying gateway version,
//!   farm status, connection state, and structured error context.
//!
//! # Design
//! `ibcore` wraps [`ibapi`] rather than re-implementing it. All IB wire-protocol
//! details are handled by the underlying `ibapi` crate; `ibcore` adds ergonomics,
//! structured errors, and observability on top.
//!
//! # Re-exports
//! Commonly-used `ibapi` types are re-exported so consumers need only depend on
//! `ibcore`, never on `ibapi` directly:
//!
//! - Contracts: [`Contract`], [`OptionRight`], [`SecurityType`], [`LegAction`]
//! - Account data: [`Position`](ibapi::accounts::Position), [`PnL`](ibapi::accounts::PnL)
//! - Orders: [`Action`](ibapi::orders::Action),
//!   [`combo_limit_order`](ibapi::orders::order_builder::combo_limit_order),
//!   [`combo_market_order`](ibapi::orders::order_builder::combo_market_order)
//! - Market data: [`MarketDataType`]
//! - Prelude: [`ibapi::prelude::*`]

pub mod client;
pub mod snapshots;
pub mod chain;
pub mod diagnostics;
pub mod errors;
pub mod exchange;
pub mod contract;
pub mod orders;
pub mod streaming;
pub mod historical;

#[cfg(feature = "remote-diagnostics")]
pub mod remote;

// ── Re-exports ──────────────────────────────────────────────────────────────

pub use client::{IbClient, is_connection_dead};
pub use snapshots::{StockSnapshot, OptionSnapshot};
pub use chain::OptionChainData;
pub use diagnostics::{DiagnosticEvent, FarmState, ConnectionState, AccountType};
pub use errors::IbError;
pub use exchange::get_primary_exchange;
pub use contract::{build_option_contract, parse_expiry};
pub use streaming::{TickEvent, TickStream};
pub use orders::{OpenOrder, OrderStatusEvent, OrderStatusStream};
pub use historical::{Bar, BarSize, Duration, HistoricalData, WhatToShow, TradingHours};

// ibapi re-exports — so Huat only needs ibcore
pub use ibapi::contracts::{Contract, OptionRight, SecurityType, LegAction, Symbol};
pub use ibapi::accounts::{Position, PnL};
pub use ibapi::orders::{Action, OrderStatusKind, Orders, order_builder::{combo_limit_order, combo_market_order}};
pub use ibapi::subscriptions::SubscriptionItemStreamExt;
pub use ibapi::market_data::MarketDataType;
pub use ibapi::prelude::*;

// ── Version information ─────────────────────────────────────────────────────

/// Crate version string, from `CARGO_PKG_VERSION` at compile time.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Version of the `ibapi` dependency this crate was built against.
pub const IBAPI_VERSION: &str = "3.0";
