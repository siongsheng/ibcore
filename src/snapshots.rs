//! Market data snapshot types for stocks and options.
//!
//! These structs capture one-shot bid/ask/last prices (and Greeks for options)
//! returned by IB's snapshot market data API.

/// Market data snapshot for a stock or index.
///
/// Fields are populated from IB's `TickType::Bid` / `Ask` / `Last` / `Close`
/// price ticks. Zero-valued fields mean the data was not received.
///
/// # Example
/// ```
/// use ibcore::StockSnapshot;
///
/// let snap = StockSnapshot { last: 450.0, ..Default::default() };
/// assert_eq!(snap.last, 450.0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct StockSnapshot {
    /// Last traded price.
    pub last: f64,
    /// Current bid price.
    pub bid: f64,
    /// Current ask price.
    pub ask: f64,
    /// Previous day's close price.
    pub close: f64,
}

/// Market data snapshot for an option (includes Greeks).
///
/// Fields are populated from IB's `PriceSize`, `Price`, and
/// `OptionComputation` ticks. Zero-valued fields mean the data was not
/// received or the option is deeply out-of-the-money.
///
/// # Example
/// ```
/// use ibcore::OptionSnapshot;
///
/// let snap = OptionSnapshot { bid: 2.50, ask: 2.60, ..Default::default() };
/// assert!(snap.ask > snap.bid);
/// ```
#[derive(Debug, Clone, Default)]
pub struct OptionSnapshot {
    /// Current bid price.
    pub bid: f64,
    /// Current ask price.
    pub ask: f64,
    /// Last traded price.
    pub last: f64,
    /// Implied volatility from the option computation tick.
    pub option_iv: f64,
    /// Delta from the option computation tick.
    pub option_delta: f64,
    /// Gamma from the option computation tick.
    pub option_gamma: f64,
    /// Theta from the option computation tick.
    pub option_theta: f64,
    /// Option mid/calculated price from the computation tick.
    pub option_price: f64,
    /// Underlying price at the time of the snapshot.
    pub underlying_price: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stock_snapshot_default_is_zero() {
        let snap = StockSnapshot::default();
        assert_eq!(snap.last, 0.0);
        assert_eq!(snap.bid, 0.0);
        assert_eq!(snap.ask, 0.0);
        assert_eq!(snap.close, 0.0);
    }

    #[test]
    fn stock_snapshot_fields_are_accessible() {
        let snap = StockSnapshot {
            last: 450.25,
            bid: 450.20,
            ask: 450.30,
            close: 448.00,
        };
        assert!((snap.bid - snap.last).abs() < 0.10);
        assert!(snap.ask > snap.bid);
    }

    #[test]
    fn option_snapshot_default_is_zero() {
        let snap = OptionSnapshot::default();
        assert_eq!(snap.bid, 0.0);
        assert_eq!(snap.ask, 0.0);
        assert_eq!(snap.option_iv, 0.0);
        assert_eq!(snap.option_delta, 0.0);
        assert_eq!(snap.option_gamma, 0.0);
        assert_eq!(snap.option_theta, 0.0);
        assert_eq!(snap.option_price, 0.0);
        assert_eq!(snap.underlying_price, 0.0);
    }

    #[test]
    fn option_snapshot_with_greeks() {
        let snap = OptionSnapshot {
            bid: 2.50,
            ask: 2.60,
            last: 2.55,
            option_iv: 0.25,
            option_delta: 0.45,
            option_gamma: 0.05,
            option_theta: -0.03,
            option_price: 2.55,
            underlying_price: 450.0,
        };
        assert!(snap.ask > snap.bid);
        assert!((snap.option_delta - 0.45).abs() < 0.01);
        assert!(snap.option_theta < 0.0);
    }
}
