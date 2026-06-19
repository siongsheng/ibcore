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
