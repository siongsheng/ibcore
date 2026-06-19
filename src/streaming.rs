//! Real-time market data streaming.
//!
//! Provides typed market data tick events ([`TickEvent`]) and the
//! [`TickStream`] wrapper that manages the IB subscription lifecycle.

use ibapi::contracts::tick_types::TickType;
use ibapi::market_data::realtime::TickTypes;
use futures::StreamExt;
use futures::stream::BoxStream;
use ibapi::subscriptions::SubscriptionItemStreamExt;

use crate::errors::IbError;

/// A typed tick event from a real-time market data stream.
///
/// Maps ibapi's heterogeneous [`TickTypes`] into a flat, consumer-friendly
/// enum. Each variant carries the data relevant to that kind of tick.
#[derive(Debug, PartialEq)]
pub enum TickEvent {
    /// Bid, ask, or last price update.
    Price {
        tick_type: TickType,
        price: f64,
    },
    /// Size update (bid size, ask size, last size, volume).
    Size {
        tick_type: TickType,
        size: f64,
    },
    /// Combined price + size (common for option bid/ask).
    PriceSize {
        price_tick_type: TickType,
        price: f64,
        size: f64,
    },
    /// Option Greeks + IV computation.
    Greeks {
        computation_type: i32,
        implied_volatility: f64,
        delta: f64,
        gamma: f64,
        theta: f64,
        vega: f64,
        option_price: f64,
        underlying_price: f64,
    },
    /// Generic numeric tick (e.g. option IV, historical vol, shortable).
    Generic {
        tick_type: TickType,
        value: f64,
    },
    /// Textual market data string (e.g. last timestamp, RT volume, halted).
    String {
        tick_type: TickType,
        value: String,
    },
    /// Snapshot completed (only for snapshot subscriptions).
    SnapshotEnd,
    /// Active market data type announced for this subscription.
    MarketDataType(ibapi::market_data::MarketDataType),
}

/// Map ibapi's [`TickTypes`] into our domain [`TickEvent`].
impl From<TickTypes> for TickEvent {
    fn from(tick: TickTypes) -> Self {
        match tick {
            TickTypes::Price(price) => TickEvent::Price {
                tick_type: price.tick_type,
                price: price.price,
            },
            TickTypes::Size(size) => TickEvent::Size {
                tick_type: size.tick_type,
                size: size.size,
            },
            TickTypes::String(s) => TickEvent::String {
                tick_type: s.tick_type,
                value: s.value,
            },
            TickTypes::Generic(g) => TickEvent::Generic {
                tick_type: g.tick_type,
                value: g.value,
            },
            TickTypes::OptionComputation(opt) => TickEvent::Greeks {
                computation_type: opt.tick_attribute.unwrap_or(0),
                implied_volatility: opt.implied_volatility.unwrap_or(0.0),
                delta: opt.delta.unwrap_or(0.0),
                gamma: opt.gamma.unwrap_or(0.0),
                theta: opt.theta.unwrap_or(0.0),
                vega: opt.vega.unwrap_or(0.0),
                option_price: opt.option_price.unwrap_or(0.0),
                underlying_price: opt.underlying_price.unwrap_or(0.0),
            },
            TickTypes::SnapshotEnd => TickEvent::SnapshotEnd,
            TickTypes::MarketDataType(md) => TickEvent::MarketDataType(md),
            TickTypes::PriceSize(ps) => TickEvent::PriceSize {
                price_tick_type: ps.price_tick_type,
                price: ps.price,
                size: ps.size,
            },
            TickTypes::RequestParameters(_) => {
                // Request parameters are not user-facing — skip
                TickEvent::Generic {
                    tick_type: TickType::Unknown,
                    value: 0.0,
                }
            }
        }
    }
}

/// A live market data tick stream for a single contract.
///
/// Wraps an ibapi subscription and maps each item into a typed [`TickEvent`].
/// Dropping the stream cancels the IB subscription.
pub struct TickStream {
    /// Inner boxed stream to hide the concrete ibapi subscription type.
    inner: BoxStream<'static, Result<ibapi::market_data::realtime::TickTypes, ibapi::Error>>,
}

impl TickStream {
    /// Create a new stream from any boxed stream of TickTypes items.
    ///
    /// Primarily for testing. For production use, construct via
    /// [`IbClient::tick_stream`](crate::IbClient::tick_stream).
    pub fn new(
        inner: BoxStream<'static, Result<ibapi::market_data::realtime::TickTypes, ibapi::Error>>,
    ) -> Self {
        Self { inner }
    }

    /// Create from an ibapi market data subscription (used internally).
    pub fn from_subscription(
        sub: ibapi::subscriptions::Subscription<ibapi::market_data::realtime::TickTypes>,
    ) -> Self {
        Self {
            inner: sub.filter_data().boxed(),
        }
    }

    /// Receive the next tick event (or error / stream-end).
    ///
    /// Returns `None` when the stream ends (connection closed / cancellation).
    pub async fn next(&mut self) -> Option<Result<TickEvent, IbError>> {
        loop {
            match self.inner.next().await {
                Some(Ok(tick)) => return Some(Ok(TickEvent::from(tick))),
                Some(Err(e)) => return Some(Err(IbError::from(e))),
                None => return None,
            }
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ibapi::contracts::tick_types::TickType;
    use ibapi::market_data::realtime::{
        TickGeneric, TickPrice, TickPriceSize, TickSize, TickString,
    };
    use ibapi::market_data::MarketDataType;
    use ibapi::contracts::OptionComputation;

    // ── TickEvent variant construction tests ──

    #[test]
    fn price_event_has_tick_type_and_price() {
        let e = TickEvent::Price {
            tick_type: TickType::Bid,
            price: 450.25,
        };
        assert_eq!(
            e,
            TickEvent::Price {
                tick_type: TickType::Bid,
                price: 450.25,
            }
        );
    }

    #[test]
    fn size_event_has_tick_type_and_size() {
        let e = TickEvent::Size {
            tick_type: TickType::BidSize,
            size: 100.0,
        };
        assert_eq!(
            e,
            TickEvent::Size {
                tick_type: TickType::BidSize,
                size: 100.0,
            }
        );
    }

    #[test]
    fn price_size_event_has_price_and_size() {
        let e = TickEvent::PriceSize {
            price_tick_type: TickType::Ask,
            price: 451.00,
            size: 200.0,
        };
        assert_eq!(
            e,
            TickEvent::PriceSize {
                price_tick_type: TickType::Ask,
                price: 451.00,
                size: 200.0,
            }
        );
    }

    #[test]
    fn greeks_event_has_all_fields() {
        let e = TickEvent::Greeks {
            computation_type: 10,
            implied_volatility: 0.35,
            delta: 0.5,
            gamma: 0.05,
            theta: -0.02,
            vega: 0.10,
            option_price: 5.20,
            underlying_price: 450.00,
        };
        match e {
            TickEvent::Greeks {
                computation_type,
                implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                option_price,
                underlying_price,
            } => {
                assert_eq!(computation_type, 10);
                assert!((implied_volatility - 0.35).abs() < 1e-10);
                assert!((delta - 0.5).abs() < 1e-10);
                assert!((gamma - 0.05).abs() < 1e-10);
                assert!((theta - -0.02).abs() < 1e-10);
                assert!((vega - 0.10).abs() < 1e-10);
                assert!((option_price - 5.20).abs() < 1e-10);
                assert!((underlying_price - 450.00).abs() < 1e-10);
            }
            _ => panic!("expected Greeks variant"),
        }
    }

    #[test]
    fn generic_event_has_tick_type_and_value() {
        let e = TickEvent::Generic {
            tick_type: TickType::OptionImpliedVol,
            value: 0.28,
        };
        match e {
            TickEvent::Generic { tick_type, value } => {
                assert_eq!(tick_type, TickType::OptionImpliedVol);
                assert!((value - 0.28).abs() < 1e-10);
            }
            _ => panic!("expected Generic variant"),
        }
    }

    #[test]
    fn string_event_has_tick_type_and_value() {
        let e = TickEvent::String {
            tick_type: TickType::LastTimestamp,
            value: "1234567890".into(),
        };
        match e {
            TickEvent::String { tick_type, value } => {
                assert_eq!(tick_type, TickType::LastTimestamp);
                assert_eq!(value, "1234567890");
            }
            _ => panic!("expected String variant"),
        }
    }

    #[test]
    fn snapshot_end_is_unit_variant() {
        let e = TickEvent::SnapshotEnd;
        assert_eq!(e, TickEvent::SnapshotEnd);
    }

    #[test]
    fn market_data_type_event() {
        let e = TickEvent::MarketDataType(MarketDataType::Realtime);
        match e {
            TickEvent::MarketDataType(md) => assert_eq!(md, MarketDataType::Realtime),
            _ => panic!("expected MarketDataType variant"),
        }
    }

    // ── From<TickTypes> mapping tests ──

    #[test]
    fn from_tick_price_maps_to_price_event() {
        let tick = TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.50,
            attributes: Default::default(),
        });
        let event = TickEvent::from(tick);
        assert_eq!(
            event,
            TickEvent::Price {
                tick_type: TickType::Bid,
                price: 100.50,
            }
        );
    }

    #[test]
    fn from_tick_size_maps_to_size_event() {
        let tick = TickTypes::Size(TickSize {
            tick_type: TickType::BidSize,
            size: 500.0,
        });
        let event = TickEvent::from(tick);
        assert_eq!(
            event,
            TickEvent::Size {
                tick_type: TickType::BidSize,
                size: 500.0,
            }
        );
    }

    #[test]
    fn from_tick_price_size_maps_to_price_size_event() {
        let tick = TickTypes::PriceSize(TickPriceSize {
            price_tick_type: TickType::Ask,
            price: 101.00,
            attributes: Default::default(),
            size_tick_type: TickType::AskSize,
            size: 300.0,
        });
        let event = TickEvent::from(tick);
        assert_eq!(
            event,
            TickEvent::PriceSize {
                price_tick_type: TickType::Ask,
                price: 101.00,
                size: 300.0,
            }
        );
    }

    #[test]
    fn from_tick_string_maps_to_string_event() {
        let tick = TickTypes::String(TickString {
            tick_type: TickType::LastTimestamp,
            value: "1700000000".into(),
        });
        let event = TickEvent::from(tick);
        match event {
            TickEvent::String { tick_type, value } => {
                assert_eq!(tick_type, TickType::LastTimestamp);
                assert_eq!(value, "1700000000");
            }
            _ => panic!("expected String variant"),
        }
    }

    #[test]
    fn from_tick_generic_maps_to_generic_event() {
        let tick = TickTypes::Generic(TickGeneric {
            tick_type: TickType::OptionImpliedVol,
            value: 0.35,
        });
        let event = TickEvent::from(tick);
        match event {
            TickEvent::Generic { tick_type, value } => {
                assert_eq!(tick_type, TickType::OptionImpliedVol);
                assert!((value - 0.35).abs() < 1e-10);
            }
            _ => panic!("expected Generic variant"),
        }
    }

    #[test]
    fn from_tick_option_computation_maps_to_greeks() {
        let opt = OptionComputation {
            field: TickType::Bid,
            tick_attribute: Some(10),
            implied_volatility: Some(0.35),
            delta: Some(0.5),
            gamma: Some(0.05),
            theta: Some(-0.02),
            vega: Some(0.10),
            underlying_price: Some(450.00),
            option_price: Some(5.20),
            present_value_dividend: None,
        };
        let tick = TickTypes::OptionComputation(opt);
        let event = TickEvent::from(tick);
        match event {
            TickEvent::Greeks {
                computation_type,
                implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                option_price,
                underlying_price,
            } => {
                assert_eq!(computation_type, 10);
                assert!((implied_volatility - 0.35).abs() < 1e-10);
                assert!((delta - 0.5).abs() < 1e-10);
                assert!((gamma - 0.05).abs() < 1e-10);
                assert!((theta - -0.02).abs() < 1e-10);
                assert!((vega - 0.10).abs() < 1e-10);
                assert!((option_price - 5.20).abs() < 1e-10);
                assert!((underlying_price - 450.00).abs() < 1e-10);
            }
            _ => panic!("expected Greeks variant"),
        }
    }

    #[test]
    fn from_snapshot_end_maps_to_snapshot_end() {
        let tick = TickTypes::SnapshotEnd;
        let event = TickEvent::from(tick);
        assert_eq!(event, TickEvent::SnapshotEnd);
    }

    #[test]
    fn from_market_data_type_maps_to_market_data_type() {
        let tick = TickTypes::MarketDataType(MarketDataType::Realtime);
        let event = TickEvent::from(tick);
        assert_eq!(event, TickEvent::MarketDataType(MarketDataType::Realtime));
    }

    #[test]
    fn from_request_parameters_maps_to_generic() {
        let tick =
            TickTypes::RequestParameters(ibapi::market_data::realtime::TickRequestParameters {
                min_tick: 0.01,
                bbo_exchange: "SMART".into(),
                snapshot_permissions: 1,
            });
        let event = TickEvent::from(tick);
        match event {
            TickEvent::Generic { .. } => {} // expected
            _ => panic!("expected Generic variant for RequestParameters"),
        }
    }

    // ── TickStream tests ──

    #[tokio::test]
    async fn tick_stream_next_returns_none_when_stream_ended() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let stream = futures::stream::once(async {
            let _ = tx.send(());
            std::result::Result::<TickTypes, ibapi::Error>::Err(ibapi::Error::Shutdown)
        })
        .boxed();
        let mut ts = TickStream::new(stream);
        // First item should be an error (the Shutdown)
        let first = ts.next().await;
        assert!(first.is_some(), "expected error from stream before end");
        // Second item should be None (stream ended)
        // Actually, the stream will have ended after the first poll since
        // it yields exactly one item then ends.
        // The next() method should return None after the stream yields None.
        let second = ts.next().await;
        assert!(second.is_none(), "expected None after stream ended");
    }

    #[tokio::test]
    async fn tick_stream_forwards_stream_error() {
        let stream = futures::stream::once(
            async { std::result::Result::<TickTypes, ibapi::Error>::Err(ibapi::Error::Shutdown) },
        )
        .boxed();
        let mut ts = TickStream::new(stream);
        let item = ts.next().await;
        match item {
            Some(Err(IbError::ConnectionReset)) => {} // Shutdown maps to ConnectionReset
            other => panic!("expected Some(Err(ConnectionReset)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tick_stream_forwards_tick_event() {
        let tick = TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.50,
            attributes: Default::default(),
        });
        let stream = futures::stream::once(
            async move { std::result::Result::<TickTypes, ibapi::Error>::Ok(tick) },
        )
        .boxed();
        let mut ts = TickStream::new(stream);
        let item = ts.next().await;
        match item {
            Some(Ok(TickEvent::Price { tick_type, price })) => {
                assert_eq!(tick_type, TickType::Bid);
                assert!((price - 100.50).abs() < 1e-10);
            }
            other => panic!("expected Some(Ok(Price)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tick_stream_drop_is_safe() {
        let stream = futures::stream::pending::<std::result::Result<TickTypes, ibapi::Error>>()
            .boxed();
        let ts = TickStream::new(stream);
        drop(ts); // should not panic
    }
}
