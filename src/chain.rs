//! Option chain data — expirations and strikes for an underlying.
//!
//! [`OptionChainData`] provides structured access to the expiration dates and
//! strike prices returned by IB's option chain API.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// Structured option chain data for a single underlying.
///
/// Contains the resolved expiration dates and strike prices from IB's option
/// chain response.
///
/// # Example
/// ```
/// use ibcore::OptionChainData;
/// use chrono::NaiveDate;
///
/// let chain = OptionChainData::from_ib(
///     "SPY", "SMART",
///     vec!["20260717".into(), "20260821".into()],
///     vec![500.0, 550.0, 600.0],
/// ).unwrap();
///
/// assert_eq!(chain.symbol, "SPY");
/// assert_eq!(chain.expirations.len(), 2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionChainData {
    /// Ticker symbol.
    pub symbol: String,
    /// Exchange where the chain was resolved.
    pub exchange: String,
    /// Available expiration dates, sorted ascending.
    pub expirations: Vec<NaiveDate>,
    /// Available strike prices, sorted ascending.
    pub strikes: Vec<f64>,
}

impl OptionChainData {
    /// Build from raw IB [`ibapi::OptionChain`] fields.
    ///
    /// IB provides expirations as `Vec<String>` in `YYYYMMDD` format and strikes
    /// as `Vec<f64>`. This constructor parses the date strings and sorts strikes.
    ///
    /// Returns `None` if no expirations could be parsed (e.g. the chain is empty).
    pub fn from_ib(
        symbol: &str,
        exchange: &str,
        expirations: Vec<String>,
        strikes: Vec<f64>,
    ) -> Option<Self> {
        let expirations: Vec<NaiveDate> = expirations
            .iter()
            .filter_map(|e| {
                if e.len() == 8 {
                    NaiveDate::parse_from_str(e, "%Y%m%d").ok()
                } else {
                    None
                }
            })
            .collect();

        if expirations.is_empty() {
            return None;
        }

        let mut strikes = strikes;
        strikes.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        Some(Self {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            expirations,
            strikes,
        })
    }

    /// Find all expirations within ±3 days of a target DTE (days-to-expiration).
    ///
    /// Useful for filtering option chains to a specific DTE target like 45 or 21.
    ///
    /// # Example
    /// ```
    /// use ibcore::OptionChainData;
    /// use chrono::NaiveDate;
    ///
    /// let chain = OptionChainData::from_ib(
    ///     "SPY", "SMART",
    ///     vec!["20260731".into(), "20260807".into(), "20260918".into()],
    ///     vec![500.0],
    /// ).unwrap();
    ///
    /// let today = NaiveDate::from_ymd_opt(2026, 6, 16).unwrap();
    /// let matches = chain.find_expirations_near_dte(45, today);
    /// assert_eq!(matches.len(), 1);
    /// ```
    pub fn find_expirations_near_dte(&self, target_dte: i64, today: NaiveDate) -> Vec<NaiveDate> {
        self.expirations
            .iter()
            .filter(|exp| {
                let dte = (**exp - today).num_days();
                (dte - target_dte).abs() <= 3
            })
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ib_parses_valid_expirations() {
        let chain = OptionChainData::from_ib(
            "SPY",
            "SMART",
            vec!["20260717".into(), "20260821".into()],
            vec![500.0, 550.0, 600.0],
        )
        .unwrap();

        assert_eq!(chain.symbol, "SPY");
        assert_eq!(chain.expirations.len(), 2);
        assert_eq!(
            chain.expirations[0],
            NaiveDate::from_ymd_opt(2026, 7, 17).unwrap()
        );
        assert_eq!(chain.strikes, vec![500.0, 550.0, 600.0]);
    }

    #[test]
    fn from_ib_rejects_empty_expirations() {
        let chain = OptionChainData::from_ib("SPY", "SMART", vec![], vec![500.0]);
        assert!(chain.is_none());
    }

    #[test]
    fn from_ib_skips_invalid_expiry_format() {
        let chain = OptionChainData::from_ib(
            "SPY",
            "SMART",
            vec!["bad".into(), "20260717".into()],
            vec![500.0],
        )
        .unwrap();

        assert_eq!(chain.expirations.len(), 1);
    }

    #[test]
    fn find_near_dte_matches_45_dte() {
        let chain = OptionChainData::from_ib(
            "SPY",
            "SMART",
            vec!["20260731".into(), "20260807".into(), "20260918".into()],
            vec![500.0],
        )
        .unwrap();

        let today = NaiveDate::from_ymd_opt(2026, 6, 16).unwrap();
        let matches = chain.find_expirations_near_dte(45, today);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0],
            NaiveDate::from_ymd_opt(2026, 7, 31).unwrap()
        );
    }

    #[test]
    fn find_near_dte_empty_when_none_match() {
        let chain = OptionChainData::from_ib(
            "SPY",
            "SMART",
            vec!["20261218".into()],
            vec![500.0],
        )
        .unwrap();

        let today = NaiveDate::from_ymd_opt(2026, 6, 16).unwrap();
        let matches = chain.find_expirations_near_dte(45, today);
        assert!(matches.is_empty());
    }
}
