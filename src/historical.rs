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
        use ibapi::market_data::historical::BarTimestamp;
        use time::OffsetDateTime;

        let bar1 = Bar {
            date: BarTimestamp::Date(
                Date::from_calendar_date(2025, time::Month::January, 1).unwrap(),
            ),
            open: 100.0,
            high: 101.0,
            low: 99.5,
            close: 100.5,
            volume: 10000.0,
            wap: 100.2,
            count: 500,
        };
        let bar2 = Bar {
            date: BarTimestamp::Date(
                Date::from_calendar_date(2025, time::Month::January, 2).unwrap(),
            ),
            open: 100.5,
            high: 102.0,
            low: 100.0,
            close: 101.5,
            volume: 12000.0,
            wap: 101.0,
            count: 600,
        };
        let data = HistoricalData {
            start: OffsetDateTime::UNIX_EPOCH,
            end: OffsetDateTime::UNIX_EPOCH,
            bars: vec![bar1, bar2],
        };
        assert_eq!(data.bars.len(), 2);
        assert!((data.bars[0].open - 100.0).abs() < 1e-10);
        assert!((data.bars[1].close - 101.5).abs() < 1e-10);
    }
}
