//! Option contract construction helpers.
//!
//! Pure functions for building [`Contract`] structs used with IB's
//! `contract_details` API. No side effects — these just assemble fields.

use ibapi::contracts::{Contract, OptionRight, SecurityType};

/// Build an IB option contract for `contract_details` resolution.
///
/// Constructs a [`Contract`] for the specified symbol, expiry, strike, and
/// right (call/put). The returned contract is suitable for passing to
/// [`ibapi::Client::contract_details`] which resolves the conid and exchange.
///
/// # Example
/// ```
/// use ibcore::contract::build_option_contract;
///
/// let contract = build_option_contract("SPY", (2026, 7, 17), 700.0, false, "SMART");
/// assert_eq!(contract.symbol.0, "SPY");
/// ```
pub fn build_option_contract(
    symbol: &str,
    expiry_ymd: (u16, u8, u8),
    strike: f64,
    is_call: bool,
    exchange: &str,
) -> Contract {
    let (year, month, day) = expiry_ymd;
    let expiry_str = format!("{year:04}{month:02}{day:02}");
    let right = if is_call {
        OptionRight::Call
    } else {
        OptionRight::Put
    };

    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Option,
        exchange: exchange.into(),
        currency: "USD".into(),
        last_trade_date_or_contract_month: expiry_str,
        strike,
        right: Some(right),
        multiplier: "100".into(),
        ..Default::default()
    }
}

/// Parse a YYYYMMDD expiry string into a `(year, month, day)` tuple.
///
/// # Example
/// ```
/// use ibcore::contract::parse_expiry;
///
/// let (y, m, d) = parse_expiry("20260731").unwrap();
/// assert_eq!(y, 2026);
/// assert_eq!(m, 7);
/// assert_eq!(d, 31);
/// ```
pub fn parse_expiry(expiry: &str) -> Result<(u16, u8, u8), ParseExpiryError> {
    if expiry.len() != 8 {
        return Err(ParseExpiryError(format!(
            "invalid expiry format: {expiry}"
        )));
    }
    let year: u16 = expiry[0..4]
        .parse()
        .map_err(|e| ParseExpiryError(format!("invalid year in {expiry}: {e}")))?;
    let month: u8 = expiry[4..6]
        .parse()
        .map_err(|e| ParseExpiryError(format!("invalid month in {expiry}: {e}")))?;
    let day: u8 = expiry[6..8]
        .parse()
        .map_err(|e| ParseExpiryError(format!("invalid day in {expiry}: {e}")))?;
    Ok((year, month, day))
}

/// Error returned by [`parse_expiry`] when the input is not a valid YYYYMMDD string.
#[derive(Debug)]
pub struct ParseExpiryError(pub String);

impl std::fmt::Display for ParseExpiryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseExpiryError {}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_expiry tests ──

    #[test]
    fn parse_expiry_valid() {
        let (y, m, d) = parse_expiry("20260731").unwrap();
        assert_eq!(y, 2026);
        assert_eq!(m, 7);
        assert_eq!(d, 31);
    }

    #[test]
    fn parse_expiry_invalid_short() {
        assert!(parse_expiry("bad").is_err());
    }

    #[test]
    fn parse_expiry_invalid_chars() {
        assert!(parse_expiry("2026abcd").is_err());
    }

    #[test]
    fn parse_expiry_error_display() {
        let e = parse_expiry("bad").unwrap_err();
        assert!(!e.to_string().is_empty());
    }

    #[test]
    fn parse_expiry_january() {
        let (y, m, d) = parse_expiry("20260103").unwrap();
        assert_eq!(y, 2026);
        assert_eq!(m, 1);
        assert_eq!(d, 3);
    }

    // ── build_option_contract tests ──

    #[test]
    fn build_put_contract_shape() {
        let contract = build_option_contract("SPY", (2026, 7, 17), 700.0, false, "SMART");
        assert_eq!(contract.symbol.0, "SPY");
        assert_eq!(contract.security_type, SecurityType::Option);
        assert_eq!(contract.exchange, "SMART");
        assert_eq!(contract.currency, "USD");
        assert_eq!(contract.last_trade_date_or_contract_month, "20260717");
        assert!((contract.strike - 700.0).abs() < 0.01);
        assert_eq!(contract.right, Some(OptionRight::Put));
        assert_eq!(contract.multiplier, "100");
    }

    #[test]
    fn build_call_contract_shape() {
        let contract = build_option_contract("QQQ", (2026, 8, 21), 380.0, true, "CBOE");
        assert_eq!(contract.symbol.0, "QQQ");
        assert_eq!(contract.exchange, "CBOE");
        assert_eq!(contract.last_trade_date_or_contract_month, "20260821");
        assert_eq!(contract.right, Some(OptionRight::Call));
    }

    #[test]
    fn build_contract_defaults_rest() {
        let contract = build_option_contract("AAPL", (2027, 1, 15), 200.0, false, "SMART");
        // Verify unspecified fields are default
        assert_eq!(contract.contract_id, 0);
    }
}
