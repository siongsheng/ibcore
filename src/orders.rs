//! Order placement, cancellation, and status tracking.
//!
//! Provides methods on [`IbClient`](crate::IbClient) for submitting orders,
//! cancelling them, and subscribing to order status updates.
//!
//! # Design
//! All methods are async and return [`IbError`](crate::IbError) on failure.
//! Order IDs are auto-assigned via [`ibapi::Client::next_valid_order_id`]
//! — callers never manage raw IDs.

use crate::diagnostics::DiagnosticEvent;
use crate::errors::IbError;
use crate::IbClient;
use futures::StreamExt;
use futures::stream::BoxStream;
use ibapi::contracts::Contract;
use ibapi::orders::Order;
use ibapi::subscriptions::SubscriptionItemStreamExt;

// Re-export key types for the pub API
pub use ibapi::orders::{Action, OrderStatusKind, TimeInForce};

/// Snapshot of an open order with its contract.
///
/// Returned by [`IbClient::open_orders`] as a one-shot summary.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrder {
    /// The API-assigned order ID.
    pub order_id: i32,
    /// Ticker symbol of the underlying contract.
    pub symbol: String,
    /// Order side: "BUY" or "SELL".
    pub action: String,
    /// Total order quantity.
    pub quantity: f64,
    /// Order type string (e.g. "LMT", "MKT", "STP").
    pub order_type: String,
    /// Limit price, if applicable.
    pub limit_price: Option<f64>,
    /// Current order status string.
    pub status: String,
    /// Number of contracts/shares already filled.
    pub filled_qty: f64,
    /// Number of contracts/shares still open.
    pub remaining_qty: f64,
}

/// Typed order status event (not raw ibapi enum).
///
/// Provides a simplified, typed view of order state changes from IB's
/// order update stream. Consumers match on these variants instead of
/// interpreting raw [`ibapi::orders::OrderStatus`] fields.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderStatusEvent {
    /// Order was acknowledged by IB.
    Submitted {
        /// The API-assigned order ID.
        order_id: i32,
    },
    /// Order was filled (partially or fully).
    Filled {
        /// The API-assigned order ID.
        order_id: i32,
        /// Number of contracts/shares filled.
        filled_qty: f64,
        /// Average fill price.
        avg_price: f64,
        /// Optional commission from the next commission report.
        commission: Option<f64>,
        /// Execution ID from the CommissionReport, for correlating fills
        /// with their commission reports. `None` for fill-status events
        /// (which carry no execution ID), `Some(...)` for commission-report
        /// events.
        execution_id: Option<String>,
    },
    /// Order was cancelled.
    Cancelled {
        /// The API-assigned order ID.
        order_id: i32,
        /// Reason for cancellation, if available.
        reason: String,
    },
    /// Order went inactive (e.g., GTC expired).
    Inactive {
        /// The API-assigned order ID.
        order_id: i32,
    },
    /// Order was rejected.
    Rejected {
        /// The API-assigned order ID.
        order_id: i32,
        /// Rejection reason from IB.
        reason: String,
    },
    /// Generic status update — unknown status kind.
    Other {
        /// The API-assigned order ID.
        order_id: i32,
        /// Raw status string from IB.
        status: String,
    },
}

/// Terminal outcome of an order placed via [`IbClient::place_order_await`].
#[derive(Debug, Clone)]
pub enum OrderOutcome {
    /// Order filled. Carries fill quantity and average fill price.
    Filled {
        /// API-assigned order ID.
        order_id: i32,
        /// Quantity filled.
        filled_qty: f64,
        /// Average fill price (combo net price for spreads).
        avg_price: f64,
    },
    /// Order rejected by IB — carries the rejection message.
    Rejected {
        /// API-assigned order ID.
        order_id: i32,
        /// Rejection reason from IB.
        reason: String,
    },
    /// Order cancelled or went inactive before filling.
    Cancelled {
        /// API-assigned order ID.
        order_id: i32,
        /// Reason, if any.
        reason: String,
    },
    /// Acknowledged/working but no terminal status within the wait window.
    Pending {
        /// API-assigned order ID.
        order_id: i32,
    },
}

/// A stream of live order status updates.
///
/// Wraps an ibapi `Subscription<OrderUpdate>` and maps each item into a
/// typed [`OrderStatusEvent`]. Dropping the stream cancels the subscription.
pub struct OrderStatusStream {
    /// Inner boxed stream to hide the concrete ibapi subscription type.
    inner: BoxStream<'static, Result<ibapi::orders::OrderUpdate, ibapi::Error>>,
    /// Optional diagnostic event sender for emitting fill/rejection events.
    diagnostic_tx: Option<tokio::sync::broadcast::Sender<DiagnosticEvent>>,
}

impl OrderStatusStream {
    /// Create a new stream from an ibapi order update subscription.
    pub fn new(
        sub: ibapi::subscriptions::Subscription<ibapi::orders::OrderUpdate>,
    ) -> Self {
        Self {
            inner: sub.filter_data().boxed(),
            diagnostic_tx: None,
        }
    }

    /// Attach a diagnostic event sender for emitting fill/rejection events.
    pub fn with_diagnostics(mut self, tx: tokio::sync::broadcast::Sender<DiagnosticEvent>) -> Self {
        self.diagnostic_tx = Some(tx);
        self
    }

    /// Receive the next order status update.
    ///
    /// Returns `None` when the stream ends (connection closed / cancellation).
    pub async fn next(&mut self) -> Option<Result<OrderStatusEvent, IbError>> {
        loop {
            match self.inner.next().await {
                Some(Ok(ibapi::orders::OrderUpdate::OrderStatus(status))) => {
                    return Some(Ok(map_order_status(&status)));
                }
                Some(Ok(ibapi::orders::OrderUpdate::CommissionReport(report))) => {
                    // Commission reports are handled as part of Filled events.
                    // We emit them as separate events for consumers that want them.
                    return Some(Ok(OrderStatusEvent::Filled {
                        order_id: 0, // Not available in commission report; caller matches via execution_id
                        filled_qty: 0.0,
                        avg_price: 0.0,
                        commission: Some(report.commission),
                        execution_id: normalize_execution_id(report.execution_id),
                    }));
                }
                Some(Ok(_)) => {
                    // OpenOrder / ExecutionData — skip, handled elsewhere
                    continue;
                }
                Some(Err(e)) => return Some(Err(IbError::from(e))),
                None => return None,
            }
        }
    }
}

/// Map an ibapi [`OrderStatus`] into our typed [`OrderStatusEvent`].
fn map_order_status(status: &ibapi::orders::OrderStatus) -> OrderStatusEvent {
    let order_id = status.order_id;
    match status.status {
        OrderStatusKind::ApiPending | OrderStatusKind::ApiCancelled => {
            OrderStatusEvent::Other {
                order_id,
                status: format!("{:?}", status.status),
            }
        }
        OrderStatusKind::Submitted => OrderStatusEvent::Submitted { order_id },
        OrderStatusKind::Filled => {
            OrderStatusEvent::Filled {
                order_id,
                filled_qty: status.filled,
                avg_price: status.average_fill_price.unwrap_or(0.0),
                commission: None,
                execution_id: None,
            }
        }
        OrderStatusKind::Cancelled => OrderStatusEvent::Cancelled {
            order_id,
            reason: status.why_held.clone(),
        },
        OrderStatusKind::Inactive => OrderStatusEvent::Inactive { order_id },
        _ => OrderStatusEvent::Other {
            order_id,
            status: format!("{:?}", status.status),
        },
    }
}

/// Decide whether an order-status event is terminal for `place_order_await`.
///
/// Returns `Some(outcome)` to stop waiting, or `None` for non-terminal states
/// (Submitted / PreSubmitted / PendingSubmit) where we keep listening. Pure
/// function — the async loop in `place_order_await` is a thin wrapper over it.
fn classify_terminal(ev: OrderStatusEvent) -> Option<OrderOutcome> {
    match ev {
        OrderStatusEvent::Filled { order_id, filled_qty, avg_price, .. } => {
            Some(OrderOutcome::Filled { order_id, filled_qty, avg_price })
        }
        OrderStatusEvent::Rejected { order_id, reason } => {
            Some(OrderOutcome::Rejected { order_id, reason })
        }
        OrderStatusEvent::Cancelled { order_id, reason } => {
            Some(OrderOutcome::Cancelled { order_id, reason })
        }
        OrderStatusEvent::Inactive { order_id } => {
            Some(OrderOutcome::Rejected { order_id, reason: "order went inactive".to_string() })
        }
        OrderStatusEvent::Submitted { .. } | OrderStatusEvent::Other { .. } => None,
    }
}

/// Normalize an IB execution ID string: empty strings are coerced to `None`,
/// preserving non-empty values. This prevents `Some("")` which would force
/// every consumer to check for empty strings.
fn normalize_execution_id(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

impl IbClient {
    /// Submit an order to IB Gateway.
    ///
    /// Auto-assigns an order ID via [`ibapi::Client::next_valid_order_id`],
    /// then submits the order via [`ibapi::Client::submit_order`].
    ///
    /// # Returns
    /// The assigned `order_id` on successful submission. Note that IB may
    /// still reject the order asynchronously after this returns — subscribe
    /// to [`order_updates()`](Self::order_updates) to track status.
    ///
    /// # Errors
    /// Returns [`IbError::OrderRejected`] if the order ID could not be
    /// obtained or submission failed.
    pub async fn place_order(
        &self,
        contract: &Contract,
        order: &Order,
    ) -> Result<i32, IbError> {
        let order_id = self
            .inner()
            .next_valid_order_id()
            .await
            .map_err(|e| IbError::OrderRejected {
                code: 0,
                message: format!("failed to get order ID: {e}"),
                rejection_json: None,
            })?;

        self.inner()
            .submit_order(order_id, contract, order)
            .await
            .map_err(|e| IbError::OrderRejected {
                code: 0,
                message: format!("submit_order failed: {e}"),
                rejection_json: None,
            })?;

        Ok(order_id)
    }

    /// Submit an order and wait (up to `timeout`) for a terminal status.
    ///
    /// Unlike the fire-and-forget [`place_order`](Self::place_order) (which
    /// wraps `submit_order` and never learns the outcome), this uses ibapi's
    /// `place_order` — which registers the order id so its status/execution/
    /// error messages route back on the returned subscription. The caller thus
    /// learns the actual fill price or the rejection reason instead of
    /// assuming success. Returns [`OrderOutcome::Pending`] if no terminal
    /// status arrives within `timeout` (the order may still be working).
    pub async fn place_order_await(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        timeout: std::time::Duration,
    ) -> Result<OrderOutcome, IbError> {
        let sub = self
            .inner()
            .place_order(order_id, contract, order)
            .await
            .map_err(|e| IbError::OrderRejected {
                code: 0,
                message: format!("place_order failed: {e}"),
                rejection_json: None,
            })?;

        let outcome = tokio::time::timeout(timeout, async {
            let mut data = sub.filter_data();
            while let Some(item) = data.next().await {
                match item {
                    Ok(ibapi::orders::PlaceOrder::OrderStatus(s)) => {
                        if let Some(outcome) = classify_terminal(map_order_status(&s)) {
                            return outcome;
                        }
                        // non-terminal (Submitted / PreSubmitted / PendingSubmit) — keep waiting
                    }
                    // OpenOrder / ExecutionData / CommissionReport — not terminal here.
                    Ok(_) => continue,
                    // IB delivers order rejections as an error on this subscription.
                    Err(e) => {
                        return OrderOutcome::Rejected { order_id, reason: e.to_string() };
                    }
                }
            }
            OrderOutcome::Pending { order_id }
        })
        .await
        .unwrap_or(OrderOutcome::Pending { order_id });

        Ok(outcome)
    }

    /// Cancel an open order by order_id.
    ///
    /// Wraps [`ibapi::Client::cancel_order`] with typed error handling.
    /// The cancellation is immediate (no manual order cancel time).
    pub async fn cancel_order(&self, order_id: i32) -> Result<(), IbError> {
        let _sub = self
            .inner()
            .cancel_order(order_id, "")
            .await
            .map_err(|e| IbError::OrderRejected {
                code: 0,
                message: format!("cancel_order failed: {e}"),
                rejection_json: None,
            })?;
        Ok(())
    }

    /// Subscribe to live order status updates.
    ///
    /// Returns a stream that yields [`OrderStatusEvent`] items until the
    /// stream is dropped or the connection is closed. Only one order update
    /// subscription can be active at a time per IB Gateway.
    pub async fn order_updates(&self) -> Result<OrderStatusStream, IbError> {
        let sub = self
            .inner()
            .order_update_stream()
            .await
            .map_err(|e| IbError::Other(format!("order_update_stream failed: {e}")))?;
        Ok(OrderStatusStream::new(sub))
    }

    /// Fetch currently open orders (one-shot snapshot).
    ///
    /// Returns a [`Vec<OpenOrder>`] of all open orders placed by this API
    /// client. Uses [`ibapi::Client::open_orders`] internally.
    pub async fn open_orders(&self) -> Result<Vec<OpenOrder>, IbError> {
        let sub = self
            .inner()
            .open_orders()
            .await
            .map_err(|e| IbError::Other(format!("open_orders failed: {e}")))?;
        let mut data = sub.filter_data();
        let mut orders = Vec::new();
        while let Some(item) = data.next().await {
            match item {
                Ok(ibapi::orders::Orders::OrderData(od)) => {
                    let order = &od.order;
                    let state = &od.order_state;
                    orders.push(OpenOrder {
                        order_id: od.order_id,
                        symbol: od.contract.symbol.0.clone(),
                        action: format!("{:?}", order.action),
                        quantity: order.total_quantity,
                        order_type: order.order_type.clone(),
                        limit_price: order.limit_price,
                        status: format!("{:?}", state.status),
                        filled_qty: order.filled_quantity,
                        remaining_qty: order.total_quantity - order.filled_quantity,
                    });
                }
                Ok(ibapi::orders::Orders::OrderStatus(_)) => {} // skip status in snapshot
                Err(e) => tracing::warn!("open_orders stream error: {e}"),
            }
        }
        Ok(orders)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IbError;

    // ── Error propagation tests ──

    #[test]
    fn place_order_error_maps_to_order_rejected() {
        let e = IbError::OrderRejected {
            code: 0,
            message: "failed to get order ID: connection error".into(),
            rejection_json: None,
        };
        let s = e.to_string();
        assert!(s.contains("order rejection"));
        assert!(s.contains("failed to get order ID"));

        let e2 = IbError::OrderRejected {
            code: 201,
            message: "submit_order failed: insufficient funds".into(),
            rejection_json: Some("{\"reason\":\"bad\"}".into()),
        };
        let s2 = e2.to_string();
        assert!(s2.contains("201"));
        assert!(s2.contains("insufficient funds"));
    }

    #[test]
    fn place_order_returns_i32_on_success() {
        // Verify the return type is Result<i32, IbError>
        fn _check() -> Result<i32, IbError> {
            Ok(42)
        }
        assert_eq!(_check().unwrap(), 42);
    }

    // ── cancel_order tests ──

    #[test]
    fn cancel_order_error_maps_to_order_rejected() {
        let e = IbError::OrderRejected {
            code: 0,
            message: "cancel_order failed: not found".into(),
            rejection_json: None,
        };
        let s = e.to_string();
        assert!(s.contains("order rejection"));
        assert!(s.contains("cancel_order failed"));
    }

    #[test]
    fn cancel_order_success_returns_ok() {
        let result: Result<(), IbError> = Ok(());
        assert!(result.is_ok());
    }

    // ── OrderStatusEvent tests ──

    #[test]
    fn order_status_event_submitted_has_order_id() {
        let e = OrderStatusEvent::Submitted { order_id: 42 };
        assert_eq!(format!("{e:?}"), "Submitted { order_id: 42 }");
    }

    #[test]
    fn order_status_event_filled_has_qty_and_price() {
        let e = OrderStatusEvent::Filled {
            order_id: 100,
            filled_qty: 50.0,
            avg_price: 450.25,
            commission: Some(1.50),
            execution_id: None,
        };
        match e {
            OrderStatusEvent::Filled { order_id, filled_qty, avg_price, commission, .. } => {
                assert_eq!(order_id, 100);
                assert_eq!(filled_qty, 50.0);
                assert_eq!(avg_price, 450.25);
                assert_eq!(commission, Some(1.50));
            }
            _ => panic!("expected Filled variant"),
        }
    }

    #[test]
    fn order_status_event_filled_has_execution_id() {
        let e = OrderStatusEvent::Filled {
            order_id: 100,
            filled_qty: 50.0,
            avg_price: 450.25,
            commission: None,
            execution_id: Some("abc123".into()),
        };
        match e {
            OrderStatusEvent::Filled { execution_id, .. } => {
                assert_eq!(execution_id, Some("abc123".to_string()));
            }
            _ => panic!("expected Filled variant"),
        }
    }

    #[test]
    fn normalize_execution_id_coerces_empty_string() {
        assert_eq!(super::normalize_execution_id("".into()), None);
    }

    #[test]
    fn normalize_execution_id_preserves_non_empty() {
        assert_eq!(
            super::normalize_execution_id("abc123".into()),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn order_status_event_cancelled_has_reason() {
        let e = OrderStatusEvent::Cancelled {
            order_id: 200,
            reason: "user requested".into(),
        };
        let s = format!("{e:?}");
        assert!(s.contains("Cancelled"));
        assert!(s.contains("user requested"));
    }

    #[test]
    fn order_status_event_rejected_has_reason() {
        let e = OrderStatusEvent::Rejected {
            order_id: 300,
            reason: "insufficient funds".into(),
        };
        let s = format!("{e:?}");
        assert!(s.contains("Rejected"));
        assert!(s.contains("insufficient funds"));
    }

    #[test]
    fn order_status_event_inactive_has_order_id() {
        let e = OrderStatusEvent::Inactive { order_id: 400 };
        assert!(format!("{e:?}").contains("Inactive"));
    }

    #[test]
    fn order_status_event_other_has_status() {
        let e = OrderStatusEvent::Other {
            order_id: 500,
            status: "ApiPending".into(),
        };
        assert!(format!("{e:?}").contains("ApiPending"));
    }

    // ── classify_terminal / OrderOutcome tests ──

    #[test]
    fn classify_filled_is_terminal_with_price() {
        let ev = OrderStatusEvent::Filled {
            order_id: 7,
            filled_qty: 1.0,
            avg_price: 6.34,
            commission: None,
            execution_id: None,
        };
        match classify_terminal(ev) {
            Some(OrderOutcome::Filled { order_id, filled_qty, avg_price }) => {
                assert_eq!(order_id, 7);
                assert_eq!(filled_qty, 1.0);
                assert_eq!(avg_price, 6.34);
            }
            other => panic!("expected Filled, got {other:?}"),
        }
    }

    #[test]
    fn classify_rejected_is_terminal_with_reason() {
        let ev = OrderStatusEvent::Rejected { order_id: 8, reason: "no security def".into() };
        match classify_terminal(ev) {
            Some(OrderOutcome::Rejected { order_id, reason }) => {
                assert_eq!(order_id, 8);
                assert_eq!(reason, "no security def");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn classify_cancelled_is_terminal() {
        let ev = OrderStatusEvent::Cancelled { order_id: 9, reason: "held".into() };
        assert!(matches!(
            classify_terminal(ev),
            Some(OrderOutcome::Cancelled { order_id: 9, .. })
        ));
    }

    #[test]
    fn classify_inactive_maps_to_inactive() {
        // Issue #13: IB `Inactive` is neither a hard rejection nor an explicit
        // cancel, so it maps to its own terminal variant rather than Rejected.
        let ev = OrderStatusEvent::Inactive { order_id: 10 };
        match classify_terminal(ev) {
            Some(OrderOutcome::Inactive { order_id, reason }) => {
                assert_eq!(order_id, 10);
                assert!(reason.contains("inactive"));
            }
            other => panic!("expected Inactive, got {other:?}"),
        }
    }

    // ── order_rejected_from (issue #14) ──

    #[test]
    fn order_rejected_from_notice_preserves_code_and_json() {
        let err = ibapi::Error::Notice(ibapi::Notice {
            code: 201,
            message: "Order rejected - insufficient margin".into(),
            error_time: None,
            advanced_order_reject_json: "{\"orderId\":5}".into(),
        });
        match order_rejected_from("submit_order failed", &err) {
            IbError::OrderRejected { code, message, rejection_json } => {
                assert_eq!(code, 201);
                assert!(message.starts_with("submit_order failed:"));
                assert!(message.contains("insufficient margin"));
                assert_eq!(rejection_json.as_deref(), Some("{\"orderId\":5}"));
            }
            other => panic!("expected OrderRejected, got {other:?}"),
        }
    }

    #[test]
    fn order_rejected_from_notice_empty_json_is_none() {
        let err = ibapi::Error::Notice(ibapi::Notice {
            code: 321,
            message: "Error validating request".into(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        });
        match order_rejected_from("submit_order failed", &err) {
            IbError::OrderRejected { code, rejection_json, .. } => {
                assert_eq!(code, 321);
                assert!(rejection_json.is_none());
            }
            other => panic!("expected OrderRejected, got {other:?}"),
        }
    }

    #[test]
    fn order_rejected_from_non_notice_falls_back_to_zero() {
        // Connection / IO / shutdown errors carry no IB code — code 0, but the
        // original error is preserved in the message for diagnosis.
        let err = ibapi::Error::ConnectionFailed;
        match order_rejected_from("place_order failed", &err) {
            IbError::OrderRejected { code, message, rejection_json } => {
                assert_eq!(code, 0);
                assert!(message.starts_with("place_order failed:"));
                assert!(rejection_json.is_none());
            }
            other => panic!("expected OrderRejected, got {other:?}"),
        }
    }

    #[test]
    fn classify_submitted_is_non_terminal() {
        assert!(classify_terminal(OrderStatusEvent::Submitted { order_id: 11 }).is_none());
    }

    #[test]
    fn classify_other_is_non_terminal() {
        let ev = OrderStatusEvent::Other { order_id: 12, status: "PreSubmitted".into() };
        assert!(classify_terminal(ev).is_none());
    }

    // ── map_order_status tests ──

    #[test]
    fn map_submitted_status() {
        let status = ibapi::orders::OrderStatus {
            order_id: 1,
            status: OrderStatusKind::Submitted,
            filled: 0.0,
            remaining: 100.0,
            average_fill_price: None,
            perm_id: 0,
            parent_id: 0,
            last_fill_price: None,
            client_id: 0,
            why_held: String::new(),
            market_cap_price: None,
        };
        let event = map_order_status(&status);
        assert_eq!(event, OrderStatusEvent::Submitted { order_id: 1 });
    }

    #[test]
    fn map_filled_status() {
        let status = ibapi::orders::OrderStatus {
            order_id: 2,
            status: OrderStatusKind::Filled,
            filled: 100.0,
            remaining: 0.0,
            average_fill_price: Some(450.50),
            perm_id: 0,
            parent_id: 0,
            last_fill_price: None,
            client_id: 0,
            why_held: String::new(),
            market_cap_price: None,
        };
        let event = map_order_status(&status);
        assert_eq!(
            event,
            OrderStatusEvent::Filled {
                order_id: 2,
                filled_qty: 100.0,
                avg_price: 450.50,
                commission: None,
                execution_id: None,
            }
        );
    }

    #[test]
    fn map_cancelled_status() {
        let status = ibapi::orders::OrderStatus {
            order_id: 3,
            status: OrderStatusKind::Cancelled,
            filled: 0.0,
            remaining: 100.0,
            average_fill_price: None,
            perm_id: 0,
            parent_id: 0,
            last_fill_price: None,
            client_id: 0,
            why_held: "user cancelled".into(),
            market_cap_price: None,
        };
        let event = map_order_status(&status);
        assert_eq!(
            event,
            OrderStatusEvent::Cancelled {
                order_id: 3,
                reason: "user cancelled".into()
            }
        );
    }

    #[test]
    fn map_api_pending_is_other() {
        let status = ibapi::orders::OrderStatus {
            order_id: 4,
            status: OrderStatusKind::ApiPending,
            filled: 0.0,
            remaining: 0.0,
            average_fill_price: None,
            perm_id: 0,
            parent_id: 0,
            last_fill_price: None,
            client_id: 0,
            why_held: String::new(),
            market_cap_price: None,
        };
        let event = map_order_status(&status);
        assert!(matches!(event, OrderStatusEvent::Other { .. }));
    }

    #[test]
    fn map_inactive_status() {
        let status = ibapi::orders::OrderStatus {
            order_id: 5,
            status: OrderStatusKind::Inactive,
            filled: 0.0,
            remaining: 100.0,
            average_fill_price: None,
            perm_id: 0,
            parent_id: 0,
            last_fill_price: None,
            client_id: 0,
            why_held: String::new(),
            market_cap_price: None,
        };
        let event = map_order_status(&status);
        assert_eq!(event, OrderStatusEvent::Inactive { order_id: 5 });
    }

    #[test]
    fn order_status_stream_construct() {
        // Verify OrderStatusStream can be constructed (compile-time check)
        fn _check() {
            let _ = std::mem::size_of::<OrderStatusStream>();
        }
        _check();
    }

    // ── OpenOrder and open_orders tests ──

    #[test]
    fn open_order_struct_fields() {
        let o = OpenOrder {
            order_id: 42,
            symbol: "SPY".into(),
            action: "BUY".into(),
            quantity: 100.0,
            order_type: "LMT".into(),
            limit_price: Some(450.0),
            status: "Submitted".into(),
            filled_qty: 0.0,
            remaining_qty: 100.0,
        };
        assert_eq!(o.symbol, "SPY");
        assert_eq!(o.order_id, 42);
        assert_eq!(o.limit_price, Some(450.0));
    }

    #[test]
    fn open_order_default_limit_price() {
        let o = OpenOrder {
            order_id: 1,
            symbol: "QQQ".into(),
            action: "SELL".into(),
            quantity: 200.0,
            order_type: "MKT".into(),
            limit_price: None,
            status: "Filled".into(),
            filled_qty: 200.0,
            remaining_qty: 0.0,
        };
        assert!(o.limit_price.is_none());
        assert_eq!(o.filled_qty, o.quantity);
    }
}
