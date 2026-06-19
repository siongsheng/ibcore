//! Historical market data.
//!
//! Re-exports ibapi historical data types so consumers don't need ibapi
//! in their Cargo.toml.

pub use ibapi::market_data::historical::{
    Bar, BarSize, Duration, HistoricalData, WhatToShow,
};
pub use ibapi::market_data::TradingHours;

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    // Compile-time checks that re-exports exist
    use ibapi::market_data::historical::BarTimestamp;
    use time::Date;

    #[test]
    fn historical_bar_type_exists() {
        let bar = Bar {
            date: BarTimestamp::Date(Date::MIN),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            wap: 0.0,
            count: 0,
        };
        let _ = bar;
    }

    #[test]
    fn historical_bar_size_exists() {
        let _ = BarSize::Day;
    }

    #[test]
    fn historical_duration_exists() {
        let _ = Duration::days(30);
    }

    #[test]
    fn what_to_show_exists() {
        let _ = WhatToShow::Trades;
    }

    #[test]
    fn trading_hours_exists() {
        assert_eq!(TradingHours::Regular, TradingHours::Regular);
    }

    /// Construct HistoricalData with bars and verify field access.
    #[test]
    fn historical_data_struct_construction() {
        todo!("implement HistoricalData construction test");
    }
}
