//! Primary options exchange resolution for stock symbols.
//!
//! Maps common ticker symbols to their primary options exchange for IB
//! contract resolution. Unrecognized symbols default to `SMART` routing.

/// Map a stock symbol to its primary options exchange.
///
/// IB requires an explicit exchange for option snapshot requests. This
/// function provides sensible defaults for common ETFs and indices:
///
/// | Symbol(s)              | Exchange |
/// |------------------------|----------|
/// | SPY, QQQ, IWM, DIA    | CBOE     |
/// | GLD, SLV, FXE, FXY    | SMART    |
/// | TLT, IEF               | SMART    |
/// | VIX                    | CBOE     |
/// | Everything else        | SMART    |
pub fn get_primary_exchange(symbol: &str) -> &'static str {
    match symbol {
        "SPY" | "QQQ" | "IWM" | "DIA" => "CBOE",
        "GLD" | "SLV" => "SMART", // metals — SMART works for these
        "TLT" | "IEF" => "SMART", // bonds
        "FXE" | "FXY" => "SMART", // currencies
        "VIX" => "CBOE",          // volatility index
        _ => "SMART",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spy_goes_to_cboe() {
        assert_eq!(get_primary_exchange("SPY"), "CBOE");
    }

    #[test]
    fn qqq_goes_to_cboe() {
        assert_eq!(get_primary_exchange("QQQ"), "CBOE");
    }

    #[test]
    fn iwm_goes_to_cboe() {
        assert_eq!(get_primary_exchange("IWM"), "CBOE");
    }

    #[test]
    fn dia_goes_to_cboe() {
        assert_eq!(get_primary_exchange("DIA"), "CBOE");
    }

    #[test]
    fn vix_goes_to_cboe() {
        assert_eq!(get_primary_exchange("VIX"), "CBOE");
    }

    #[test]
    fn gld_goes_to_smart() {
        assert_eq!(get_primary_exchange("GLD"), "SMART");
    }

    #[test]
    fn tlt_goes_to_smart() {
        assert_eq!(get_primary_exchange("TLT"), "SMART");
    }

    #[test]
    fn unknown_symbol_goes_to_smart() {
        assert_eq!(get_primary_exchange("AAPL"), "SMART");
    }
}
