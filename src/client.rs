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

use futures::StreamExt;
use ibapi::{
    accounts::{
        types::{AccountGroup, AccountId},
        AccountSummaryResult, PositionUpdate,
    },
    contracts::{Contract, OptionRight, SecurityType, tick_types::TickType},
    market_data::MarketDataType,
    market_data::realtime::TickTypes,
    subscriptions::SubscriptionItemStreamExt,
};
use tokio::sync::broadcast;
use tracing;

use crate::{
    contract::build_option_contract,
    diagnostics::{
        classify_farm, AccountType, ConnectionState, DiagnosticEvent,
    },
    errors::IbError,
    exchange::get_primary_exchange,
    chain::OptionChainData,
    snapshots::{OptionSnapshot, StockSnapshot},
    TickStream,
};

/// Maximum number of diagnostic events that can be buffered before old ones
/// are dropped for slow subscribers.
const DIAGNOSTIC_BUFFER: usize = 256;

/// Persistent IB Gateway client with snapshot helpers and diagnostic events.
pub struct IbClient {
    inner: Arc<ibapi::Client>,
    account_type: AccountType,
    diagnostic_tx: broadcast::Sender<DiagnosticEvent>,
    _diagnostic_task: tokio::task::JoinHandle<()>,
}

/// Check if an error indicates the IB Gateway connection is dead.
///
/// Detects [`IbError::ConnectionReset`], [`IbError::ConnectionFailed`],
/// and similar conditions. Returns false for market-data errors, contract
/// errors, etc.
pub fn is_connection_dead(e: &IbError) -> bool {
    matches!(e, IbError::ConnectionReset | IbError::ConnectionFailed(_))
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
        })
    }

    /// Gracefully disconnect from IB Gateway, releasing the client_id so it
    /// can be reused immediately.
    pub async fn disconnect(&self) {
        tracing::info!("disconnecting from IB Gateway");
        self._diagnostic_task.abort();
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
        Ok(())
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
    pub async fn stock_snapshot(&self, symbol: &str) -> Result<StockSnapshot, IbError> {
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
            .map_err(|e| IbError::MarketData {
                code: 0,
                message: format!("stock snapshot subscribe failed: {e}"),
            })?;
        let mut data = sub.filter_data();
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
                Ok(TickTypes::SnapshotEnd) => break,
                Err(e) => tracing::warn!("stock snapshot error: {e}"),
                _ => {}
            }
        }
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
            multiplier: "100".into(),
            ..Default::default()
        };

        // Resolve via contract_details — try preferred exchange first,
        // then fall back to empty exchange (IB smart routing) if that fails.
        let exchanges_to_try = if exchange.is_empty() || exchange == "SMART" {
            vec![exchange.to_string()]
        } else {
            vec![exchange.to_string(), String::new()]
        };

        let mut last_err = None;
        let mut details = None;
        for ex in &exchanges_to_try {
            let mut c = contract.clone();
            c.exchange = ex.as_str().into();
            match self.inner.contract_details(&c).await {
                Ok(d) if !d.is_empty() => {
                    details = Some(d);
                    break;
                }
                Ok(_) => {
                    last_err = Some(IbError::ContractResolution(format!(
                        "contract_details returned empty for {symbol} {expiry_str} {strike} on {ex}"
                    )));
                }
                Err(e) => {
                    tracing::warn!(
                        "contract_details failed for {symbol} {expiry_str} {strike} on {ex}: {e}"
                    );
                    last_err = Some(IbError::from(e));
                }
            }
        }

        let details = details.ok_or_else(|| {
            last_err.unwrap_or_else(|| {
                IbError::ContractResolution(format!(
                    "contract_details failed for {symbol} {expiry_str} {strike}"
                ))
            })
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

        self.option_snapshot_from_contract(&resolved.contract)
            .await
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
    async fn snapshot_inner(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<OptionSnapshot, IbError> {
        let sub = self
            .inner
            .market_data(contract)
            .snapshot()
            .subscribe()
            .await
            .map_err(|e| IbError::MarketData {
                code: 0,
                message: format!("option snapshot subscribe failed: {e}"),
            })?;
        let mut data = sub.filter_data();
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
                    TickType::Last | TickType::DelayedLast => snap.last = price.price,
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
                Ok(TickTypes::SnapshotEnd) => break,
                Err(e) => tracing::warn!("option snapshot error: {e}"),
                _ => {}
            }
        }
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
            match self.inner.contract_details(&contract).await {
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
            .inner
            .contract_details(&contract)
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
            .map_err(|e| IbError::MarketData {
                code: 0,
                message: format!("tick_stream subscribe failed for {symbol}: {e}"),
            })?;
        tracing::info!("tick_stream subscribed for {symbol}");
        Ok(TickStream::from_subscription(sub))
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

/// Retry a snapshot with exponential backoff when all prices are zero.
///
/// Error 10197 ("competing live session") causes IB to return empty market data
/// on paper accounts when a live Gateway is also connected. Retrying with delay
/// gives the market data stream time to recover.
async fn retry_snapshot<F, Fut>(mut fetch: F) -> Result<OptionSnapshot, IbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<OptionSnapshot, IbError>>,
{
    for attempt in 0..3 {
        let snap = fetch().await?;
        let is_zero = snap.bid <= 0.0 && snap.ask <= 0.0 && snap.last <= 0.0;

        if !is_zero {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
