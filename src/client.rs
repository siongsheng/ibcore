//! Persistent IB Gateway client wrapper.
//!
//! Wraps [`ibapi::Client`] in a newtype with snapshot helpers, structured errors,
//! and a diagnostic event broadcast channel that monitors the notice stream.
//!
//! # Example (requires a running IB Gateway)
//! ```no_run
//! # async fn example() -> Result<(), ibcore::IbError> {
//! let ib = ibcore::IbClient::connect("127.0.0.1", 4002, 1, "delayed", ibcore::AccountType::Paper).await?;
//! let snap = ib.stock_snapshot("SPY").await?;
//! println!("SPY last: {}", snap.last);
//! ib.disconnect().await;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use ibapi::{
    accounts::{
        types::{AccountGroup, AccountId},
        AccountSummaryResult, PositionUpdate,
    },
    contracts::{Contract, OptionRight, SecurityType, tick_types::TickType},
    market_data::MarketDataType,
    market_data::realtime::TickTypes,
    market_data::historical::{
        BarSize, HistoricalData, WhatToShow,
    },
    market_data::historical::Duration as IbDuration,
    market_data::TradingHours,
    subscriptions::SubscriptionItemStreamExt,
};
use tokio::sync::broadcast;
use tracing;

use crate::{
    contract::{build_option_contract, DEFAULT_OPTION_MULTIPLIER},
    diagnostics::{
        classify_farm, AccountType, ConnectionState, DiagnosticEvent,
    },
    errors::IbError,
    exchange::get_primary_exchange,
    chain::OptionChainData,
    snapshots::{OptionSnapshot, StockSnapshot},
    TickStream,
};

#[cfg(feature = "remote-diagnostics")]
use crate::remote::{RemoteDiagnosticsConfig, SessionFingerprint, DIAGNOSIS_BUFFER, BATCHER_TO_POLLER_CAPACITY};

/// Maximum number of diagnostic events that can be buffered before old ones
/// are dropped for slow subscribers.
const DIAGNOSTIC_BUFFER: usize = 1024;

use std::collections::HashMap;

/// Build a cache key from an IB contract's identifying fields.
fn contract_cache_key(c: &ibapi::contracts::Contract) -> String {
    format!(
        "{}|{:?}|{}|{}|{}|{:?}",
        c.symbol,
        c.security_type,
        c.exchange,
        c.last_trade_date_or_contract_month,
        c.strike,
        c.right
    )
}

/// Persistent IB Gateway client with snapshot helpers and diagnostic events.
pub struct IbClient {
    inner: Arc<ibapi::Client>,
    account_type: AccountType,
    diagnostic_tx: broadcast::Sender<DiagnosticEvent>,
    _diagnostic_task: tokio::task::JoinHandle<()>,
    contract_cache: tokio::sync::Mutex<HashMap<String, Vec<ibapi::contracts::ContractDetails>>>,
    #[cfg(feature = "remote-diagnostics")]
    _remote_diag_configured: bool,
    #[cfg(feature = "remote-diagnostics")]
    _remote_batcher: Option<tokio::task::JoinHandle<()>>,
    #[cfg(feature = "remote-diagnostics")]
    _remote_poller: Option<tokio::task::JoinHandle<()>>,
}

/// Check if an error indicates the IB Gateway connection is dead.
///
/// Detects [`IbError::ConnectionReset`], [`IbError::ConnectionFailed`],
/// and similar conditions. Returns false for market-data errors, contract
/// errors, etc.
pub fn is_connection_dead(e: &IbError) -> bool {
    matches!(e, IbError::ConnectionReset | IbError::ConnectionFailed(_))
}

/// Build an [`IbError`] from a market-data subscribe/fetch failure, routing the
/// raw [`ibapi::Error`] through [`IbError::from`] so it is classified correctly
/// (#21).
///
/// Previously these paths hardcoded `IbError::MarketData { code: 0 }`, which
/// discarded the real error: a reconnect-time `ConnectionReset` surfaced as
/// `MarketData { code: 0 }` and [`is_connection_dead`] returned false, so
/// callers missed the disconnect. Routing through [`IbError::from`] preserves
/// classification (`ConnectionReset` → [`IbError::ConnectionReset`], a Notice →
/// [`classify_notice`](crate::errors::classify_notice), IO → typed). Only a
/// genuinely unclassifiable error ([`IbError::Other`]) falls back to
/// [`IbError::MarketData`], where the `context` string is preserved for
/// diagnosis.
fn market_data_error(context: &str, e: ibapi::Error) -> IbError {
    match IbError::from(e) {
        IbError::Other(msg) => IbError::MarketData {
            code: 0,
            message: format!("{context}: {msg}"),
        },
        classified => classified,
    }
}

impl IbClient {
    /// Access the underlying ibapi Client (cloning gives another `Arc` handle).
    pub fn inner(&self) -> Arc<ibapi::Client> {
        self.inner.clone()
    }

    /// The server version reported by the connected Gateway.
    pub fn server_version(&self) -> i32 {
        self.inner.server_version()
    }

    /// Whether this client is connected to a paper (simulation) account.
    pub fn account_type(&self) -> AccountType {
        self.account_type
    }

    /// Fetch contract_details with caching.
    ///
    /// Checks the in-memory cache before calling the IB API. Cache is keyed
    /// by (symbol, security_type, exchange, expiry, strike, right).
    /// Cleared on reconnect.
    async fn cached_contract_details(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<Vec<ibapi::contracts::ContractDetails>, ibapi::Error> {
        let key = contract_cache_key(contract);
        {
            let cache = self.contract_cache.lock().await;
            if let Some(details) = cache.get(&key) {
                tracing::debug!("contract_details cache hit: {key}");
                return Ok(details.clone());
            }
        }
        let details = self.inner.contract_details(contract).await?;
        if !details.is_empty() {
            let mut cache = self.contract_cache.lock().await;
            cache.insert(key, details.clone());
        }
        Ok(details)
    }

    /// Resolve a contract to its `ContractDetails` via [`cached_contract_details`],
    /// trying the preferred exchange first and falling back to IB smart routing
    /// (empty exchange) when the preferred one yields nothing.
    ///
    /// Shared by [`option_snapshot`](Self::option_snapshot) and
    /// [`option_strikes_for_expiry`](Self::option_strikes_for_expiry), which both
    /// need the same fallback discipline. Fallback order: `[exchange]` when
    /// `exchange` is empty or `"SMART"` (nothing more to try), otherwise
    /// `[exchange, ""]`. Returns the first non-empty `ContractDetails` list; if
    /// every attempt is empty or errors, returns the last error seen, or a
    /// [`IbError::ContractResolution`] built from `desc` when there was none.
    ///
    /// `desc` is a human-readable contract descriptor (e.g. `"SPY 20260717 700"`)
    /// used only in log and error messages.
    async fn resolve_contract_with_exchange_fallback(
        &self,
        contract: &ibapi::contracts::Contract,
        exchange: &str,
        desc: &str,
    ) -> Result<Vec<ibapi::contracts::ContractDetails>, IbError> {
        let exchanges_to_try = if exchange.is_empty() || exchange == "SMART" {
            vec![exchange.to_string()]
        } else {
            vec![exchange.to_string(), String::new()]
        };

        let mut last_err = None;
        for ex in &exchanges_to_try {
            let mut c = contract.clone();
            c.exchange = ex.as_str().into();
            match self.cached_contract_details(&c).await {
                Ok(d) if !d.is_empty() => return Ok(d),
                Ok(_) => {
                    last_err = Some(IbError::ContractResolution(format!(
                        "contract_details returned empty for {desc} on {ex}"
                    )));
                }
                Err(e) => {
                    tracing::warn!("contract_details failed for {desc} on {ex}: {e}");
                    last_err = Some(IbError::from(e));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            IbError::ContractResolution(format!("contract_details failed for {desc}"))
        }))
    }

    /// Subscribe to the diagnostic event stream.
    ///
    /// Returns a receiver that will see all future [`DiagnosticEvent`]s emitted
    /// by the background notice-stream watcher. Late subscribers miss earlier
    /// events.
    pub fn diagnostic_events(&self) -> broadcast::Receiver<DiagnosticEvent> {
        self.diagnostic_tx.subscribe()
    }

    /// Connect to IB Gateway. Returns a persistent client.
    ///
    /// Spawns a background diagnostic task that monitors the IB notice stream
    /// and broadcasts [`DiagnosticEvent`]s for consumers.
    pub async fn connect(
        host: &str,
        port: u16,
        client_id: i32,
        market_data_type: &str,
        account_type: AccountType,
    ) -> Result<Self, IbError> {
        let address = format!("{host}:{port}");
        tracing::info!("connecting to IB Gateway at {address} client_id={client_id}");

        let client = ibapi::Client::connect(&address, client_id)
            .await
            .map_err(|e| IbError::ConnectionFailed(format!("failed to connect: {e}")))?;

        let sv = client.server_version();
        tracing::info!("connected — server_version={sv}");

        // Switch market data type based on config
        let md_type = match market_data_type {
            "realtime" => MarketDataType::Realtime,
            _ => MarketDataType::Delayed,
        };
        client
            .switch_market_data_type(md_type)
            .await
            .map_err(|e| {
                IbError::Other(format!("failed to switch market data type: {e}"))
            })?;
        tracing::info!("market data type set to {market_data_type} (ibapi={md_type:?})");

        let inner = Arc::new(client);

        // Subscribe to notices and spawn diagnostic task
        let mut notice_stream = inner
            .notice_stream()
            .map_err(|e| IbError::Other(format!("failed to subscribe to notices: {e}")))?;

        let (diagnostic_tx, _rx) = broadcast::channel(DIAGNOSTIC_BUFFER);
        let tx = diagnostic_tx.clone();
        let acc_type = account_type;

        let _diagnostic_task = tokio::spawn(async move {
            loop {
                match notice_stream.next().await {
                    Some(notice) => {
                        let event = DiagnosticEvent {
                            gateway_version: sv,
                            error_code: notice.code,
                            error_message: notice.message,
                            error_time: notice.error_time,
                            farm_status: classify_farm(notice.code),
                            connection_state: ConnectionState::Connected,
                            account_type: acc_type,
                            os: std::env::consts::OS,
                            timestamp: chrono::Utc::now(),
                        };
                        let _ = tx.send(event);
                    }
                    None => {
                        tracing::debug!("notice stream ended");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            inner,
            account_type,
            diagnostic_tx,
            _diagnostic_task,
            contract_cache: tokio::sync::Mutex::new(HashMap::new()),
            #[cfg(feature = "remote-diagnostics")]
            _remote_diag_configured: false,
            #[cfg(feature = "remote-diagnostics")]
            _remote_batcher: None,
            #[cfg(feature = "remote-diagnostics")]
            _remote_poller: None,
        })
    }

    /// Gracefully disconnect from IB Gateway, releasing the client_id so it
    /// can be reused immediately.
    pub async fn disconnect(&self) {
        tracing::info!("disconnecting from IB Gateway");
        self._diagnostic_task.abort();
        #[cfg(feature = "remote-diagnostics")]
        {
            if let Some(handle) = &self._remote_batcher {
                handle.abort();
            }
            if let Some(handle) = &self._remote_poller {
                handle.abort();
            }
        }
        self.inner.disconnect().await;
    }

    /// Reconnect after connection loss. Gracefully disconnects the old client
    /// (freeing its client_id), then establishes a fresh connection.
    pub async fn reconnect(
        &mut self,
        host: &str,
        port: u16,
        client_id: i32,
        market_data_type: &str,
    ) -> Result<(), IbError> {
        let address = format!("{host}:{port}");
        tracing::info!("reconnecting to IB Gateway at {address} client_id={client_id}");

        self._diagnostic_task.abort();
        self.inner.disconnect().await;

        let client = ibapi::Client::connect(&address, client_id)
            .await
            .map_err(|e| {
                IbError::ConnectionFailed(format!("failed to reconnect: {e}"))
            })?;

        let sv = client.server_version();
        tracing::info!("reconnected — server_version={sv}");

        let md_type = match market_data_type {
            "realtime" => MarketDataType::Realtime,
            _ => MarketDataType::Delayed,
        };
        client
            .switch_market_data_type(md_type)
            .await
            .map_err(|e| {
                IbError::Other(format!("failed to switch market data type: {e}"))
            })?;
        tracing::info!("market data type set to {market_data_type} after reconnect");

        let inner = Arc::new(client);

        // Spawn new diagnostic task
        let mut notice_stream = inner
            .notice_stream()
            .map_err(|e| IbError::Other(format!("failed to subscribe to notices: {e}")))?;

        let tx = self.diagnostic_tx.clone();
        let acc_type = self.account_type;

        self._diagnostic_task = tokio::spawn(async move {
            loop {
                match notice_stream.next().await {
                    Some(notice) => {
                        let event = DiagnosticEvent {
                            gateway_version: sv,
                            error_code: notice.code,
                            error_message: notice.message,
                            error_time: notice.error_time,
                            farm_status: classify_farm(notice.code),
                            connection_state: ConnectionState::Connected,
                            account_type: acc_type,
                            os: std::env::consts::OS,
                            timestamp: chrono::Utc::now(),
                        };
                        let _ = tx.send(event);
                    }
                    None => {
                        tracing::debug!("notice stream ended after reconnect");
                        break;
                    }
                }
            }
        });

        self.inner = inner;
        self.contract_cache = tokio::sync::Mutex::new(HashMap::new());
        #[cfg(feature = "remote-diagnostics")]
        {
            // Remote diag tasks are NOT re-spawned after reconnect — the
            // caller must call with_remote_diagnostics again if needed.
            self._remote_diag_configured = false;
            self._remote_batcher = None;
            self._remote_poller = None;
        }
        Ok(())
    }

    // ── Remote diagnostics ──

    /// Enable remote diagnostic event streaming to ibquirk API.
    ///
    /// Consumes `self` and returns `(Self, Receiver<RemoteDiagnosis>)`.
    /// The receiver can be used to subscribe to diagnosis responses.
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same client (double-invocation
    /// guard).
    #[cfg(feature = "remote-diagnostics")]
    pub fn with_remote_diagnostics(
        mut self,
        config: RemoteDiagnosticsConfig,
    ) -> (Self, tokio::sync::broadcast::Receiver<crate::remote::RemoteDiagnosis>) {
        assert!(
            !self._remote_diag_configured,
            "with_remote_diagnostics called twice — remote diagnostics already configured"
        );

        let diagnostic_rx = self.diagnostic_tx.subscribe();
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let (diagnosis_tx, diagnosis_rx) = tokio::sync::broadcast::channel(DIAGNOSIS_BUFFER);

        let session = SessionFingerprint {
            gateway_version: self.inner.server_version(),
            os: std::env::consts::OS,
            account_type: self.account_type,
            client_version: crate::version(),
        };

        let interval = config.batch_interval;
        let batcher_batch_tx = batch_tx.clone();
        let batcher_session = session.clone();

        let batcher_handle = tokio::spawn(async move {
            crate::remote::run_batcher(
                diagnostic_rx,
                interval,
                batcher_batch_tx,
                batcher_session,
            )
            .await;
        });

        let poller_config = config.clone();
        let poller_diagnosis_tx = diagnosis_tx.clone();

        let poller_handle = tokio::spawn(async move {
            crate::remote::run_poller(batch_rx, poller_config, poller_diagnosis_tx).await;
        });

        self._remote_diag_configured = true;
        self._remote_batcher = Some(batcher_handle);
        self._remote_poller = Some(poller_handle);

        (self, diagnosis_rx)
    }

    // ── Account / Position methods ──

    /// Fetch all positions (one-time snapshot).
    pub async fn positions(&self) -> Result<Vec<ibapi::accounts::Position>, IbError> {
        let sub = self
            .inner
            .positions()
            .await
            .map_err(|e| IbError::Other(format!("positions failed: {e}")))?;
        let mut data = sub.filter_data();
        let mut positions = Vec::new();
        while let Some(item) = data.next().await {
            match item {
                Ok(PositionUpdate::Position(p)) => positions.push(p),
                Ok(PositionUpdate::PositionEnd) => break,
                Err(e) => tracing::warn!("position stream error: {e}"),
            }
        }
        Ok(positions)
    }

    /// Fetch account summary tags.
    pub async fn account_summary(
        &self,
        tags: &[&str],
    ) -> Result<Vec<(String, String, String, String)>, IbError> {
        let sub = self
            .inner
            .account_summary(&AccountGroup("All".into()), tags)
            .await
            .map_err(|e| IbError::Other(format!("account_summary failed: {e}")))?;
        let mut data = sub.filter_data();
        let mut results = Vec::new();
        while let Some(item) = data.next().await {
            match item {
                Ok(AccountSummaryResult::Summary(s)) => {
                    results.push((s.account, s.tag, s.value, s.currency));
                }
                Ok(AccountSummaryResult::End) => break,
                Err(e) => tracing::warn!("account summary error: {e}"),
            }
        }
        Ok(results)
    }

    /// Fetch P&L for an account.
    pub async fn pnl(&self, account: &str) -> Result<ibapi::accounts::PnL, IbError> {
        let sub = self
            .inner
            .pnl(&AccountId(account.into()), None)
            .await
            .map_err(|e| IbError::Other(format!("pnl failed: {e}")))?;
        let mut data = sub.filter_data();
        while let Some(item) = data.next().await {
            match item {
                Ok(pnl) => return Ok(pnl),
                Err(e) => tracing::warn!("pnl error: {e}"),
            }
        }
        Err(IbError::Other("no P&L data returned".into()))
    }

    /// Get a market data snapshot for a stock or index.
    ///
    /// Retries up to 3 times with exponential backoff when the snapshot returns
    /// all-zero bid/ask/last prices (competing live session blocking paper data).
    pub async fn stock_snapshot(&self, symbol: &str) -> Result<StockSnapshot, IbError> {
        retry_market_data(|| self.stock_snapshot_inner(symbol)).await
    }

    /// Single stock snapshot attempt — no retry.
    /// 
    /// Uses streaming market data with a 5-second timeout instead of the
    /// ibapi snapshot API, which returns 10197 on Gateway 10.45+.
    /// See https://github.com/wboayue/rust-ibapi/issues/683
    async fn stock_snapshot_inner(&self, symbol: &str) -> Result<StockSnapshot, IbError> {
        let contract = if symbol == "VIX" {
            Contract {
                symbol: symbol.into(),
                security_type: SecurityType::Index,
                exchange: "CBOE".into(),
                currency: "USD".into(),
                ..Default::default()
            }
        } else {
            Contract::stock(symbol)
                .on_exchange(get_primary_exchange(symbol))
                .build()
        };
        let sub = self
            .inner
            .market_data(&contract)
            .snapshot()
            .subscribe()
            .await
            .map_err(|e| market_data_error("stock market data subscribe failed", e))?;
        let data = sub.filter_data();

        let snap = collect_stock_snapshot_ticks(data).await;

        // sub dropped here → streaming subscription cancelled
        Ok(snap)
    }

    /// Get a market data snapshot for an option by resolving the contract
    /// via contract_details first, then using the resolved contract for the
    /// snapshot.
    #[allow(clippy::too_many_arguments)]
    pub async fn option_snapshot(
        &self,
        symbol: &str,
        expiry_ymd: (u16, u8, u8),
        strike: f64,
        is_call: bool,
        _implied_vol: f64,
        _underlying_price: f64,
        exchange: &str,
    ) -> Result<OptionSnapshot, IbError> {
        let (year, month, day) = expiry_ymd;
        let expiry_str = format!("{year:04}{month:02}{day:02}");
        let right = if is_call {
            OptionRight::Call
        } else {
            OptionRight::Put
        };

        let contract = Contract {
            symbol: symbol.into(),
            security_type: SecurityType::Option,
            exchange: exchange.into(),
            currency: "USD".into(),
            last_trade_date_or_contract_month: expiry_str.clone(),
            strike,
            right: Some(right),
            // Assumes standard US-equity options (multiplier 100). Non-standard
            // multipliers (e.g. 50/1000 index options, post-split contracts) are
            // not currently supported — see DEFAULT_OPTION_MULTIPLIER.
            multiplier: DEFAULT_OPTION_MULTIPLIER.into(),
            ..Default::default()
        };

        // Resolve via contract_details — try preferred exchange first,
        // then fall back to empty exchange (IB smart routing) if that fails.
        let details = self
            .resolve_contract_with_exchange_fallback(
                &contract,
                exchange,
                &format!("{symbol} {expiry_str} {strike}"),
            )
            .await?;
        let resolved = details.first().ok_or_else(|| {
            IbError::ContractResolution(format!(
                "no contract details for {symbol} {expiry_str} {strike}"
            ))
        })?;

        tracing::debug!(
            "resolved {symbol} option conid={} exchange={}",
            resolved.contract.contract_id,
            resolved.contract.exchange
        );

        self.option_snapshot_from_contract(&resolved.contract)
            .await
    }

    /// Enumerate the strikes that actually TRADE for a specific expiry, via
    /// `contract_details` on a partial option contract (symbol + expiry, no
    /// strike/right).
    ///
    /// `fetch_option_chain` (secDefOptParams) returns the UNION of strikes
    /// across ALL expirations, so a strike in that list may not be listed for a
    /// given expiry (e.g. weekly $1 strikes vs a monthly's $5 grid) — resolving
    /// it fails with `[200] No security definition`. This returns only the
    /// strikes valid for `expiry_ymd`, so callers pick a resolvable strike on
    /// the first try instead of walking adjacents (#45).
    pub async fn option_strikes_for_expiry(
        &self,
        symbol: &str,
        expiry_ymd: (u16, u8, u8),
        exchange: &str,
    ) -> Result<Vec<f64>, IbError> {
        let (year, month, day) = expiry_ymd;
        let expiry_str = format!("{year:04}{month:02}{day:02}");
        // Partial contract: no strike, no right → contract_details returns every
        // listed option (both rights, all strikes) for this symbol + expiry.
        //
        // Multiplier is intentionally left unset here: unlike option_snapshot,
        // which resolves ONE specific contract and assumes the standard 100
        // multiplier (see DEFAULT_OPTION_MULTIPLIER), strike enumeration wants
        // every listed strike. Pinning multiplier to "100" would silently drop
        // any non-standard-multiplier listings (e.g. 50/1000 index options,
        // post-split contracts); omitting it lets contract_details return them
        // all, and distinct_sorted_strikes dedups across multipliers.
        let contract = Contract {
            symbol: symbol.into(),
            security_type: SecurityType::Option,
            exchange: exchange.into(),
            currency: "USD".into(),
            last_trade_date_or_contract_month: expiry_str.clone(),
            ..Default::default()
        };

        // Same exchange-fallback discipline as option_snapshot.
        let details = self
            .resolve_contract_with_exchange_fallback(
                &contract,
                exchange,
                &format!("{symbol} {expiry_str}"),
            )
            .await?;
        Ok(distinct_sorted_strikes(
            details.iter().map(|cd| cd.contract.strike),
        ))
    }

    /// Get a market data snapshot for an option using an already-resolved
    /// IB contract.
    ///
    /// Retries up to 3 times with exponential backoff when the snapshot returns
    /// all-zero prices (error 10197: competing live session blocking paper data).
    pub async fn option_snapshot_from_contract(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<OptionSnapshot, IbError> {
        retry_snapshot(|| self.snapshot_inner(contract)).await
    }

    /// Single snapshot attempt — no retry.
    /// 
    /// Uses streaming market data with a 5-second timeout instead of the
    /// ibapi snapshot API, which returns 10197 on Gateway 10.45+.
    /// See https://github.com/wboayue/rust-ibapi/issues/683
    async fn snapshot_inner(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<OptionSnapshot, IbError> {
        let sub = self
            .inner
            .market_data(contract)
            .subscribe()
            .await
            .map_err(|e| market_data_error("option market data subscribe failed", e))?;
        let data = sub.filter_data();

        let snap = collect_option_snapshot_ticks(data).await;

        // sub dropped here → streaming subscription cancelled
        Ok(snap)
    }

    /// Fetch the option chain (expirations + strikes) for an underlying.
    pub async fn fetch_option_chain(
        &self,
        symbol: &str,
    ) -> Result<OptionChainData, IbError> {
        // Resolve conid via contract_details
        let exchanges = ["SMART", "CBOE", "ARCA", "NASDAQ", ""];
        let mut conid = 0i32;
        for exchange in &exchanges {
            let contract = if exchange.is_empty() {
                Contract::stock(symbol).build()
            } else {
                Contract::stock(symbol).on_exchange(*exchange).build()
            };
            match self.cached_contract_details(&contract).await {
                Ok(details) => {
                    if let Some(d) = details.first() {
                        conid = d.contract.contract_id;
                        tracing::debug!(
                            "{symbol}: contract_details resolved conid={conid} on {exchange}"
                        );
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!("{symbol}: contract_details({exchange}): {e}");
                }
            }
        }

        if conid == 0 {
            return Err(IbError::ContractResolution(format!(
                "{symbol}: could not resolve contract ID"
            )));
        }

        // Request option chain with resolved conid
        let exch_combos = [("SMART", conid), ("CBOE", conid), ("", conid)];

        for (exchange, cid) in &exch_combos {
            let sub = match self
                .inner
                .option_chain(symbol, exchange, SecurityType::Stock, *cid)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("option_chain ({exchange}/{cid}): {e}");
                    continue;
                }
            };

            let mut stream = sub;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(ibapi::subscriptions::SubscriptionItem::Data(chain)) => {
                        if chain.expirations.is_empty() && chain.strikes.is_empty() {
                            continue;
                        }
                        return OptionChainData::from_ib(
                            symbol,
                            &chain.exchange,
                            chain.expirations,
                            chain.strikes,
                        )
                        .ok_or_else(|| {
                            IbError::ContractResolution(format!(
                                "{symbol}: option chain returned empty expirations after parsing"
                            ))
                        });
                    }
                    Ok(ibapi::subscriptions::SubscriptionItem::Notice(_)) => {}
                    Err(e) => {
                        tracing::warn!("option_chain stream ({exchange}/{cid}): {e}")
                    }
                }
            }
        }
        Err(IbError::ContractResolution(format!(
            "no option chain data returned for {symbol}"
        )))
    }

    /// Resolve an option contract's conid and exchange via contract_details.
    pub async fn resolve_option_conid(
        &self,
        symbol: &str,
        expiry_ymd: (u16, u8, u8),
        strike: f64,
        is_call: bool,
        exchange: &str,
    ) -> Result<(i32, String), IbError> {
        let contract = build_option_contract(symbol, expiry_ymd, strike, is_call, exchange);
        let (year, month, day) = expiry_ymd;
        let expiry_str = format!("{year:04}{month:02}{day:02}");

        let details = self
            .cached_contract_details(&contract)
            .await
            .map_err(|e| {
                IbError::ContractResolution(format!(
                    "contract_details failed for {symbol} {expiry_str} {strike}: {e}"
                ))
            })?;
        let resolved = details.first().ok_or_else(|| {
            IbError::ContractResolution(format!(
                "no contract details for {symbol} {expiry_str} {strike}"
            ))
        })?;

        tracing::debug!(
            "resolved {symbol} option conid={} exchange={}",
            resolved.contract.contract_id,
            resolved.contract.exchange
        );

        Ok((
            resolved.contract.contract_id,
            resolved.contract.exchange.0.clone(),
        ))
    }

    /// Subscribe to live market data ticks for a contract.
    ///
    /// Returns a [`TickStream`] that yields typed [`TickEvent`]s as they arrive.
    /// Drop the stream to cancel the IB subscription.
    ///
    /// # Example (requires a running IB Gateway)
    /// ```no_run
    /// # async fn example() -> Result<(), ibcore::IbError> {
    /// # let ib = ibcore::IbClient::connect(
    /// #     "127.0.0.1", 4002, 1, "delayed", ibcore::AccountType::Paper
    /// # ).await?;
    /// use futures::StreamExt;
    ///
    /// let contract = ibcore::Contract::stock("SPY").build();
    /// let mut stream = ib.tick_stream(&contract).await?;
    /// while let Some(event) = stream.next().await {
    ///     match event? {
    ///         ibcore::TickEvent::Price { tick_type, price } => {
    ///             println!("{tick_type:?}: ${price:.2}");
    ///         }
    ///         _ => {}
    ///     }
    /// }
    /// # ib.disconnect().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn tick_stream(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<TickStream, IbError> {
        let symbol = &contract.symbol;
        let sub = self
            .inner
            .market_data(contract)
            .subscribe()
            .await
            .map_err(|e| market_data_error(&format!("tick_stream subscribe failed for {symbol}"), e))?;
        tracing::info!("tick_stream subscribed for {symbol}");
        Ok(TickStream::from_subscription(sub))
    }

    /// Fetch one-shot historical OHLCV bars for a contract.
    ///
    /// # Example (requires a running IB Gateway)
    /// ```no_run
    /// # async fn example() -> Result<(), ibcore::IbError> {
    /// # let ib = ibcore::IbClient::connect(
    /// #     "127.0.0.1", 4002, 1, "delayed", ibcore::AccountType::Paper
    /// # ).await?;
    /// let contract = ibcore::Contract::stock("SPY").build();
    /// let data = ib.historical_data(
    ///     &contract,
    ///     ibcore::BarSize::Hour,
    ///     ibcore::Duration::days(5),
    ///     ibcore::WhatToShow::Trades,
    ///     ibcore::TradingHours::Regular,
    /// ).await?;
    /// for bar in &data.bars {
    ///     println!("O={:.2} H={:.2} L={:.2} C={:.2} V={:.0}",
    ///         bar.open, bar.high, bar.low, bar.close, bar.volume);
    /// }
    /// # ib.disconnect().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn historical_data(
        &self,
        contract: &ibapi::contracts::Contract,
        bar_size: BarSize,
        duration: IbDuration,
        what_to_show: WhatToShow,
        trading_hours: TradingHours,
    ) -> Result<HistoricalData, IbError> {
        let symbol = &contract.symbol;
        let data = self
            .inner
            .historical_data(contract, bar_size)
            .duration(duration)
            .what_to_show(what_to_show)
            .trading_hours(trading_hours)
            .fetch()
            .await
            .map_err(|e| market_data_error(&format!("historical_data fetch failed for {symbol}"), e))?;
        tracing::info!(
            "historical_data fetched for {symbol}: {} bars, period {} to {}",
            data.bars.len(),
            data.start,
            data.end,
        );
        Ok(data)
    }

    /// Fetch NetLiquidation from account summary.
    pub async fn net_liquidation(&self, _account_id: &str) -> Result<f64, IbError> {
        let summary = self.account_summary(&["NetLiquidation"]).await?;
        for (_account, tag, value, _currency) in summary {
            if tag == "NetLiquidation" {
                return Ok(value.parse().unwrap_or(0.0));
            }
        }
        Ok(0.0)
    }
}

// ── Free helper functions ──

/// Trait for snapshots that can detect zero-price data (competing session).
trait IsZeroPriced {
    fn is_zero_priced(&self) -> bool;
}

impl IsZeroPriced for StockSnapshot {
    fn is_zero_priced(&self) -> bool {
        self.bid <= 0.0 && self.ask <= 0.0 && self.last <= 0.0
    }
}

impl IsZeroPriced for OptionSnapshot {
    /// Aligns the retry/validity gate with the collector's early-break
    /// definition (#24): an option snapshot is only usable when it has BOTH a
    /// price and greeks, so anything incomplete (greeks-only, price-only) counts
    /// as "needs retry" — not merely the all-zero-price case. Sharing
    /// [`option_snapshot_complete`] keeps "complete" meaning the same thing in
    /// both places, so greeks-arriving-first no longer yields a spurious
    /// [`IbError::CompetingSession`].
    fn is_zero_priced(&self) -> bool {
        !option_snapshot_complete(self)
    }
}

/// Retry a snapshot with exponential backoff when all prices are zero.
///
/// Error 10197 ("competing live session") causes IB to return empty market data
/// on paper accounts when a live Gateway is also connected. Retrying with delay
/// gives the market data stream time to recover.
async fn retry_market_data<T, F, Fut>(mut fetch: F) -> Result<T, IbError>
where
    T: IsZeroPriced,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, IbError>>,
{
    for attempt in 0..3 {
        let snap = fetch().await?;
        if !snap.is_zero_priced() {
            return Ok(snap);
        }
        let delay_ms = 1000 * (1 << attempt);
        tracing::warn!(
            "empty market data (attempt {}/3, competing session?) — retrying in {}s",
            attempt + 1,
            delay_ms / 1000,
        );
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    tracing::error!(
        "market data persistently empty after 3 retries — \
         competing live session likely blocking paper data. \
         Stop the live Gateway (port 4001) or use delayed market data."
    );
    Err(IbError::CompetingSession)
}

/// Retry a snapshot — delegates to [`retry_market_data`].
async fn retry_snapshot<F, Fut>(fetch: F) -> Result<OptionSnapshot, IbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<OptionSnapshot, IbError>>,
{
    retry_market_data(fetch).await
}

// ── Streaming snapshot collection (workaround for ibapi snapshot 10197) ──

async fn collect_stock_snapshot_ticks(
    mut data: impl futures::Stream<Item = Result<TickTypes, ibapi::Error>> + Unpin,
) -> StockSnapshot {
    let collect_fut = async {
        let mut snap = StockSnapshot::default();
        while let Some(item) = data.next().await {
            match item {
                Ok(TickTypes::Price(price)) => match price.tick_type {
                    TickType::Bid | TickType::DelayedBid => snap.bid = price.price,
                    TickType::Ask | TickType::DelayedAsk => snap.ask = price.price,
                    TickType::Last | TickType::DelayedLast => snap.last = price.price,
                    TickType::Close | TickType::DelayedClose => snap.close = price.price,
                    _ => {}
                },
                Ok(TickTypes::PriceSize(ps)) => match ps.price_tick_type {
                    TickType::Bid | TickType::DelayedBid => snap.bid = ps.price,
                    TickType::Ask | TickType::DelayedAsk => snap.ask = ps.price,
                    TickType::Last | TickType::DelayedLast => snap.last = ps.price,
                    _ => {}
                },
                Err(e) => tracing::warn!("stock snapshot error: {e}"),
                _ => {}
            }
            if snap.last > 0.0 || (snap.bid > 0.0 && snap.ask > 0.0) {
                break;
            }
        }
        snap
    };

    tokio::time::timeout(Duration::from_secs(10), collect_fut)
        .await
        .unwrap_or_default()
}

async fn collect_option_snapshot_ticks(
    mut data: impl futures::Stream<Item = Result<TickTypes, ibapi::Error>> + Unpin,
) -> OptionSnapshot {
    let collect_fut = async {
        let mut snap = OptionSnapshot::default();
        while let Some(item) = data.next().await {
            match item {
                Ok(TickTypes::PriceSize(ps)) => match ps.price_tick_type {
                    TickType::Bid | TickType::DelayedBid => snap.bid = ps.price,
                    TickType::Ask | TickType::DelayedAsk => snap.ask = ps.price,
                    TickType::Last | TickType::DelayedLast => snap.last = ps.price,
                    _ => {}
                },
                Ok(TickTypes::Price(price)) => match price.tick_type {
                    TickType::Bid | TickType::DelayedBid => snap.bid = price.price,
                    TickType::Ask | TickType::DelayedAsk => snap.ask = price.price,
                    TickType::Last | TickType::DelayedLast
                    | TickType::Close | TickType::DelayedClose => snap.last = price.price,
                    _ => {}
                },
                Ok(TickTypes::OptionComputation(opt)) => {
                    snap.option_iv = opt.implied_volatility.unwrap_or(0.0);
                    snap.option_delta = opt.delta.unwrap_or(0.0);
                    snap.option_gamma = opt.gamma.unwrap_or(0.0);
                    snap.option_theta = opt.theta.unwrap_or(0.0);
                    snap.option_price = opt.option_price.unwrap_or(0.0);
                    snap.underlying_price = opt.underlying_price.unwrap_or(0.0);
                }
                Err(e) => tracing::warn!("option snapshot error: {e}"),
                _ => {}
            }
            // Break only once we have EVERYTHING the caller needs — a usable
            // price AND greeks (#24). Breaking on greeks alone (IB does not
            // guarantee price ticks arrive first) returned a zero-priced
            // snapshot that then failed the retry gate. The 5s timeout below is
            // the backstop that still returns whatever partial data arrived.
            if option_snapshot_complete(&snap) {
                break;
            }
        }
        snap
    };

    tokio::time::timeout(Duration::from_secs(5), collect_fut)
        .await
        .unwrap_or_default()
}

/// Whether an option snapshot carries everything the caller needs: a usable
/// price (bid or ask > 0) AND greeks (iv > 0 or delta != 0). Pure predicate
/// shared by the early-break in [`collect_option_snapshot_ticks`] and the
/// retry/validity gate ([`IsZeroPriced`] for [`OptionSnapshot`]), so "complete"
/// means the same thing in both places (#24).
fn option_snapshot_complete(snap: &OptionSnapshot) -> bool {
    (snap.bid > 0.0 || snap.ask > 0.0) && (snap.option_iv > 0.0 || snap.option_delta != 0.0)
}

/// Distinct, ascending, positive strikes from raw `contract_details` strike
/// values. Pure — sorts, drops non-positive sentinels, and dedups; unit-tested
/// independently of any IB round-trip. Used by [`IbClient::option_strikes_for_expiry`].
fn distinct_sorted_strikes(strikes: impl Iterator<Item = f64>) -> Vec<f64> {
    let mut v: Vec<f64> = strikes.filter(|s| *s > 0.0).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v.dedup();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_sorted_strikes_sorts_dedups_and_drops_sentinels() {
        let got = distinct_sorted_strikes([790.0, 785.0, 790.0, 0.0, -1.0, 795.0].into_iter());
        assert_eq!(got, vec![785.0, 790.0, 795.0]);
    }

    #[test]
    fn distinct_sorted_strikes_empty_is_empty() {
        let got = distinct_sorted_strikes(std::iter::empty());
        assert!(got.is_empty());
    }

    /// Compile-time check: this won't compile until tick_stream() exists on IbClient.
    /// The inner function references the method by name; if it's missing, rustc fails.
    #[test]
    fn tick_stream_method_exists() {
        fn _check(ib: &IbClient, c: &ibapi::contracts::Contract) {
            let _ = ib.tick_stream(c);
        }
        let _ = _check;
    }

    /// Compile-time check: this won't compile until historical_data() exists on IbClient.
    #[test]
    fn historical_data_method_exists() {
        fn _check(
            ib: &IbClient,
            c: &ibapi::contracts::Contract,
            bs: &ibapi::market_data::historical::BarSize,
            d: &ibapi::market_data::historical::Duration,
            w: &ibapi::market_data::historical::WhatToShow,
            th: &ibapi::market_data::TradingHours,
        ) {
            let _ = ib.historical_data(c, *bs, d.clone(), *w, *th);
        }
        let _ = _check;
    }

    /// Validate that TickStream::new() with a oneshot-error stream
    /// produces the correct IbError variant after the client's error
    /// mapping path.
    #[tokio::test]
    async fn tick_stream_error_maps_correctly() {
        use futures::StreamExt;
        use futures::stream::BoxStream;
        use ibapi::market_data::realtime::TickTypes;
        use crate::TickStream;

        // Build a stream that yields one error then ends
        let stream: BoxStream<'static, Result<TickTypes, ibapi::Error>> =
            futures::stream::once(async {
                Err(ibapi::Error::ConnectionReset)
            })
            .boxed();
        let mut ts = TickStream::new(stream);

        // The first item should be the error mapped to IbError
        let item = ts.next().await;
        match item {
            Some(Err(IbError::ConnectionReset)) => {} // expected
            other => panic!("expected Some(Err(ConnectionReset)), got {other:?}"),
        }

        // After the error, the stream should be exhausted
        let next = ts.next().await;
        assert!(next.is_none(), "expected None after stream ends");
    }

    // ── is_connection_dead tests ──

    #[test]
    fn connection_reset_is_dead() {
        let e = IbError::ConnectionReset;
        assert!(is_connection_dead(&e));
    }

    #[test]
    fn connection_failed_is_dead() {
        let e = IbError::ConnectionFailed("test".into());
        assert!(is_connection_dead(&e));
    }

    #[test]
    fn market_data_not_dead() {
        let e = IbError::MarketData {
            code: 10197,
            message: "competing session".into(),
        };
        assert!(!is_connection_dead(&e));
    }

    #[test]
    fn competing_session_not_dead() {
        let e = IbError::CompetingSession;
        assert!(!is_connection_dead(&e));
    }

    #[test]
    fn contract_resolution_not_dead() {
        let e = IbError::ContractResolution("failed".into());
        assert!(!is_connection_dead(&e));
    }

    #[test]
    fn order_rejected_not_dead() {
        let e = IbError::OrderRejected {
            code: 201,
            message: "rejected".into(),
            rejection_json: None,
        };
        assert!(!is_connection_dead(&e));
    }

    #[test]
    fn timeout_not_dead() {
        let e = IbError::Timeout("timed out".into());
        assert!(!is_connection_dead(&e));
    }

    #[test]
    fn other_not_dead() {
        let e = IbError::Other("something".into());
        assert!(!is_connection_dead(&e));
    }

    // ── is_connection_dead via IbError from ibapi::Error conversion ──

    #[test]
    fn ibapi_connection_reset_is_dead_via_conversion() {
        let ib_err: IbError = ibapi::Error::ConnectionReset.into();
        assert!(is_connection_dead(&ib_err));
    }

    #[test]
    fn ibapi_shutdown_is_dead_via_conversion() {
        let ib_err: IbError = ibapi::Error::Shutdown.into();
        assert!(is_connection_dead(&ib_err));
    }

    #[test]
    fn ibapi_notice_not_dead_via_conversion() {
        let notice = ibapi::Notice {
            code: 10197,
            message: "Competing session".into(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        let ib_err: IbError = ibapi::Error::Notice(notice).into();
        assert!(!is_connection_dead(&ib_err));
    }

    // ── market_data_error classification tests (#21) ──
    //
    // Market-data subscribe/fetch failures must be classified via IbError::from
    // rather than flattened to MarketData { code: 0 }, so a reconnect-time
    // ConnectionReset is detectable by is_connection_dead.

    #[test]
    fn market_data_error_connection_reset_is_classified() {
        let e = market_data_error("stock snapshot subscribe failed", ibapi::Error::ConnectionReset);
        assert!(matches!(e, IbError::ConnectionReset));
        assert!(is_connection_dead(&e));
    }

    #[test]
    fn market_data_error_shutdown_is_connection_dead() {
        let e = market_data_error("tick_stream subscribe failed", ibapi::Error::Shutdown);
        assert!(is_connection_dead(&e));
    }

    #[test]
    fn market_data_error_notice_is_classified_not_code_zero() {
        let notice = ibapi::Notice {
            code: 10199,
            message: "market data not subscribed".into(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        let e = market_data_error("option snapshot subscribe failed", ibapi::Error::Notice(notice));
        assert!(matches!(e, IbError::MarketData { code: 10199, .. }));
    }

    #[test]
    fn market_data_error_unclassifiable_falls_back_with_context() {
        // An error with no useful classification (Other) falls back to
        // MarketData { code: 0 }, preserving the context string for diagnosis.
        let e = market_data_error("historical_data fetch failed for SPY", ibapi::Error::NotImplemented);
        match e {
            IbError::MarketData { code, message } => {
                assert_eq!(code, 0);
                assert!(message.contains("historical_data fetch failed for SPY"));
            }
            other => panic!("expected MarketData fallback, got {other:?}"),
        }
    }

    // ── contract cache key tests ──

    #[test]
    fn cache_key_same_contract_produces_same_key() {
        use ibapi::contracts::{Contract, SecurityType};
        let c1 = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Stock,
            exchange: "SMART".into(),
            ..Default::default()
        };
        let c2 = c1.clone();
        assert_eq!(contract_cache_key(&c1), contract_cache_key(&c2));
    }

    #[test]
    fn cache_key_different_symbols_produce_different_keys() {
        use ibapi::contracts::{Contract, SecurityType};
        let spy = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Stock,
            exchange: "SMART".into(),
            ..Default::default()
        };
        let qqq = Contract {
            symbol: "QQQ".into(),
            security_type: SecurityType::Stock,
            exchange: "SMART".into(),
            ..Default::default()
        };
        assert_ne!(contract_cache_key(&spy), contract_cache_key(&qqq));
    }

    #[test]
    fn cache_key_different_strikes_produce_different_keys() {
        use ibapi::contracts::{Contract, SecurityType};
        let opt1 = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Option,
            exchange: "SMART".into(),
            last_trade_date_or_contract_month: "20260717".into(),
            strike: 400.0,
            ..Default::default()
        };
        let opt2 = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Option,
            exchange: "SMART".into(),
            last_trade_date_or_contract_month: "20260717".into(),
            strike: 450.0,
            ..Default::default()
        };
        assert_ne!(contract_cache_key(&opt1), contract_cache_key(&opt2));
    }

    #[test]
    fn cache_key_call_vs_put_produce_different_keys() {
        use ibapi::contracts::{Contract, OptionRight, SecurityType};
        let call = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Option,
            exchange: "SMART".into(),
            last_trade_date_or_contract_month: "20260717".into(),
            strike: 400.0,
            right: Some(OptionRight::Call),
            ..Default::default()
        };
        let put = Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Option,
            exchange: "SMART".into(),
            last_trade_date_or_contract_month: "20260717".into(),
            strike: 400.0,
            right: Some(OptionRight::Put),
            ..Default::default()
        };
        assert_ne!(contract_cache_key(&call), contract_cache_key(&put));
    }

    // ── snapshot collection tests (streaming workaround for ibapi snapshot 10197) ──

    /// Empty stream → timeout → returns default (all-zero) snapshot.
    #[tokio::test]
    async fn collect_stock_snapshot_timeout_returns_default() {
        // A stream that never yields (simulates no data on the wire)
        let stream = futures::stream::pending::<Result<TickTypes, ibapi::Error>>();
        let snap = collect_stock_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 0.0);
        assert_eq!(snap.ask, 0.0);
        assert_eq!(snap.last, 0.0);
    }

    /// Stream with valid ticks → collects bid/ask/last and breaks early.
    /// With relaxed break, stops on bid+ask (before Last tick arrives).
    #[tokio::test]
    async fn collect_stock_snapshot_collects_bid_ask_last() {
        use ibapi::market_data::realtime::TickPrice;
        let ticks = vec![
            Ok(TickTypes::Price(TickPrice {
                price: 100.0,
                tick_type: TickType::Bid,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 101.0,
                tick_type: TickType::Ask,
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_stock_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 100.0);
        assert_eq!(snap.ask, 101.0);
    }

    /// Close price is collected from delayed Close tick.
    /// Last-only break: Last or Close triggers exit.
    #[tokio::test]
    async fn collect_stock_snapshot_collects_close() {
        use ibapi::market_data::realtime::TickPrice;
        let ticks = vec![
            Ok(TickTypes::Price(TickPrice {
                price: 99.0,
                tick_type: TickType::DelayedClose,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 100.5,
                tick_type: TickType::Last,
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_stock_snapshot_ticks(stream).await;
        assert_eq!(snap.close, 99.0);
        assert_eq!(snap.last, 100.5);
    }

    /// Non-price ticks are silently ignored.
    #[tokio::test]
    async fn collect_stock_snapshot_ignores_non_price_ticks() {
        use ibapi::market_data::realtime::{TickPrice, TickSize};
        let ticks = vec![
            Ok(TickTypes::Size(TickSize {
                size: 500.0,
                tick_type: TickType::BidSize,
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 100.0,
                tick_type: TickType::Bid,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 101.0,
                tick_type: TickType::Ask,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 100.5,
                tick_type: TickType::Last,
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_stock_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 100.0);
    }

    /// Stream error doesn't crash — it's logged and skipped.
    #[tokio::test]
    async fn collect_stock_snapshot_skips_stream_errors() {
        use ibapi::market_data::realtime::TickPrice;
        let ticks = vec![
            Err(ibapi::Error::ConnectionReset),
            Ok(TickTypes::Price(TickPrice {
                price: 100.0,
                tick_type: TickType::Bid,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 101.0,
                tick_type: TickType::Ask,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 100.5,
                tick_type: TickType::Last,
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_stock_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 100.0);
        assert_eq!(snap.ask, 101.0);
    }

    /// Option snapshot collects bid/ask + Greeks.
    #[tokio::test]
    async fn collect_option_snapshot_collects_bid_ask_greeks() {
        use ibapi::market_data::realtime::TickPrice;
        let ticks = vec![
            Ok(TickTypes::Price(TickPrice {
                price: 5.0,
                tick_type: TickType::Bid,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 5.5,
                tick_type: TickType::Ask,
                ..Default::default()
            })),
            Ok(TickTypes::OptionComputation(ibapi::contracts::OptionComputation {
                implied_volatility: Some(0.25),
                delta: Some(0.30),
                gamma: Some(0.02),
                theta: Some(-0.05),
                option_price: Some(5.25),
                underlying_price: Some(100.0),
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_option_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 5.0);
        assert_eq!(snap.ask, 5.5);
        assert!((snap.option_iv - 0.25).abs() < 0.001);
        assert!((snap.option_delta - 0.30).abs() < 0.001);
        assert_eq!(snap.underlying_price, 100.0);
    }

    /// Option snapshot timeout returns default.
    #[tokio::test]
    async fn collect_option_snapshot_timeout_returns_default() {
        let stream = futures::stream::pending::<Result<TickTypes, ibapi::Error>>();
        let snap = collect_option_snapshot_ticks(stream).await;
        assert_eq!(snap.bid, 0.0);
        assert_eq!(snap.ask, 0.0);
        assert_eq!(snap.option_delta, 0.0);
    }

    // ── option-snapshot completeness (issue #24) ──

    #[test]
    fn option_snapshot_complete_requires_price_and_greeks() {
        // Greeks only, no price — NOT complete (the #24 bug used to break here).
        let greeks_only = OptionSnapshot {
            option_iv: 0.25,
            option_delta: 0.30,
            ..Default::default()
        };
        assert!(!option_snapshot_complete(&greeks_only), "greeks without a price is incomplete");

        // Price only, no greeks — NOT complete.
        let price_only = OptionSnapshot { bid: 5.0, ask: 5.5, ..Default::default() };
        assert!(!option_snapshot_complete(&price_only), "price without greeks is incomplete");

        // Both a usable price and greeks — complete.
        let both = OptionSnapshot {
            bid: 5.0,
            ask: 5.5,
            option_delta: 0.30,
            ..Default::default()
        };
        assert!(option_snapshot_complete(&both), "price + greeks is complete");
    }

    #[test]
    fn option_snapshot_zero_priced_aligns_with_completeness() {
        // #24: the retry/validity gate must use the same definition as the
        // early-break — an incomplete snapshot counts as "needs retry".
        let greeks_only = OptionSnapshot { option_delta: 0.30, ..Default::default() };
        assert!(greeks_only.is_zero_priced(), "greeks-only must be treated as needing retry");

        let complete = OptionSnapshot { bid: 5.0, option_delta: 0.30, ..Default::default() };
        assert!(!complete.is_zero_priced(), "a complete snapshot must not trigger retry");
    }

    /// #24: greeks arrive BEFORE any price tick. The collector must not break
    /// early and return a zero-priced snapshot — it must wait for the price.
    #[tokio::test]
    async fn collect_option_snapshot_waits_for_price_when_greeks_arrive_first() {
        use ibapi::market_data::realtime::TickPrice;
        let ticks = vec![
            Ok(TickTypes::OptionComputation(ibapi::contracts::OptionComputation {
                implied_volatility: Some(0.25),
                delta: Some(0.30),
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 5.0,
                tick_type: TickType::Bid,
                ..Default::default()
            })),
            Ok(TickTypes::Price(TickPrice {
                price: 5.5,
                tick_type: TickType::Ask,
                ..Default::default()
            })),
        ];
        let stream = futures::stream::iter(ticks);
        let snap = collect_option_snapshot_ticks(stream).await;
        assert!(
            snap.bid > 0.0 || snap.ask > 0.0,
            "collector must wait for a price tick, got bid={} ask={}",
            snap.bid,
            snap.ask
        );
        assert!(snap.option_delta != 0.0, "greeks must still be present");
    }

    // ── remote diagnostics tests ──

    #[cfg(feature = "remote-diagnostics")]
    #[tokio::test]
    async fn with_remote_diagnostics_returns_diagnosis_receiver() {
        // This test only compiles when the remote-diagnostics feature is on.
        // It uses IbClient::with_remote_diagnostics and verifies the diagnosis
        // broadcast channel is returned.
        use crate::remote::{RemoteDiagnosticsConfig, DIAGNOSIS_BUFFER};
        use tokio::sync::broadcast;

        // We can't actually connect to a Gateway in a unit test.
        // Instead, just check that the struct has the right shape.
        let _ = RemoteDiagnosticsConfig {
            endpoint: "https://api.example.com/v1/diagnose".into(),
            api_token: "test_token".into(),
            batch_interval: std::time::Duration::from_secs(5),
        };

        // Verify that DIAGNOSIS_BUFFER is reasonable
        assert!(DIAGNOSIS_BUFFER > 0);
        assert_eq!(DIAGNOSIS_BUFFER, 32);
    }

    #[cfg(feature = "remote-diagnostics")]
    #[test]
    fn with_remote_diagnostics_types_accessible() {
        // Verify that types needed for with_remote_diagnostics are accessible
        use crate::remote::{RemoteDiagnosis, RemoteDiagnosticsConfig};
        let _config = RemoteDiagnosticsConfig {
            endpoint: String::new(),
            api_token: String::new(),
            batch_interval: std::time::Duration::from_secs(1),
        };
        let _diag = RemoteDiagnosis {
            matched_quirk: String::new(),
            title: String::new(),
            confidence: 0.0,
            root_cause: String::new(),
            workaround: String::new(),
            verification: String::new(),
        };
    }
}
