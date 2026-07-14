//! Structured IB error types — maps raw ibapi error codes to typed variants.
//!
//! [`IbError`] translates low-level IB error notifications into semantically
//! meaningful variants so consumers never handle raw error codes directly.

use std::fmt;

/// Structured IB error type with semantically meaningful variants — the
/// primary error type for ibcore.
///
/// Every IB notice, I/O error, and protocol error passes through this type
/// before reaching consumers. Raw error codes are classified into variants:
///
/// | Variant             | Typical IB Codes           |
/// |---------------------|----------------------------|
/// | `CompetingSession`  | 10197                      |
/// | `MarketData`        | 10000–10999                |
/// | `FarmDisconnect`    | 2107                       |
/// | `OrderRejected`     | 200–299                    |
/// | `ConnectionFailed`  | 502, 504, 506, IO errors   |
/// | `ConnectionReset`   | 507, 100, IO reset/refused |
/// | `Timeout`           | IO timed out               |
/// | `ContractResolution`| (derived from API calls)   |
/// | `Other`             | everything else             |
///
/// `#[non_exhaustive]`: IB's error space grows over time, so new variants must
/// stay additive for downstream consumers (same policy as
/// [`crate::OrderOutcome`]).
#[derive(Debug)]
#[non_exhaustive]
pub enum IbError {
    /// Connection could not be established or was lost.
    ConnectionFailed(String),
    /// TCP connection was reset by the peer.
    ConnectionReset,
    /// Market data subscription error.
    MarketData {
        code: i32,
        message: String,
    },
    /// An order was rejected by IB.
    OrderRejected {
        code: i32,
        message: String,
        /// Optional advanced order-reject JSON payload.
        rejection_json: Option<String>,
    },
    /// Data farm disconnection (code 2107).
    FarmDisconnect {
        code: i32,
        message: String,
    },
    /// Contract details returned no results.
    ContractResolution(String),
    /// Empty market data — competing live session blocking paper data (error 10197).
    CompetingSession,
    /// Operation timed out.
    Timeout(String),
    /// Catch-all for unrecognised errors.
    Other(String),
}

impl fmt::Display for IbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IbError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            IbError::ConnectionReset => write!(f, "connection reset"),
            IbError::MarketData { code, message } => {
                write!(f, "market data error {code}: {message}")
            }
            IbError::OrderRejected {
                code,
                message,
                rejection_json,
            } => {
                if let Some(json) = rejection_json {
                    write!(f, "order rejection {code}: {message} [{json}]")
                } else {
                    write!(f, "order rejection {code}: {message}")
                }
            }
            IbError::FarmDisconnect { code, message } => {
                write!(f, "farm disconnect {code}: {message}")
            }
            IbError::ContractResolution(msg) => write!(f, "contract resolution failed: {msg}"),
            IbError::CompetingSession => {
                write!(f, "empty market data — competing session (error 10197)")
            }
            IbError::Timeout(msg) => write!(f, "timeout: {msg}"),
            IbError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for IbError {}

/// Classify an IB notice by its error code and return the appropriate [`IbError`] variant.
///
/// This is the central dispatcher: every [`ibapi::Notice`] that reaches `ibcore`
/// passes through this function before being surfaced to consumers.
pub fn classify_notice(code: i32, message: &str, rejection_json: &str) -> IbError {
    match code {
        // Competing live session blocking paper data
        10197 => IbError::CompetingSession,
        // Market data errors (broad range: missing data, invalid request, throttling)
        c if (10000..=10999).contains(&c) => IbError::MarketData {
            code,
            message: message.to_string(),
        },
        // Data farm disconnects
        2107 => IbError::FarmDisconnect {
            code,
            message: message.to_string(),
        },
        // Order rejections: typically 200-299 range
        c if (200..=299).contains(&c) => {
            let json = if rejection_json.is_empty() {
                None
            } else {
                Some(rejection_json.to_string())
            };
            IbError::OrderRejected {
                code,
                message: message.to_string(),
                rejection_json: json,
            }
        }
        // Connection-level errors
        502 | 504 | 506 => IbError::ConnectionFailed(format!("IB error {code}: {message}")),
        507 | 100 => IbError::ConnectionReset,
        // Catch-all for everything else
        _ => IbError::Other(format!("IB error {code}: {message}")),
    }
}

impl From<ibapi::Error> for IbError {
    fn from(err: ibapi::Error) -> Self {
        match err {
            ibapi::Error::ConnectionFailed => {
                IbError::ConnectionFailed("ibapi connection failed".into())
            }
            ibapi::Error::ConnectionReset => IbError::ConnectionReset,
            ibapi::Error::Notice(notice) => classify_notice(
                notice.code,
                &notice.message,
                &notice.advanced_order_reject_json,
            ),
            ibapi::Error::Io(io) => IbError::from(io),
            ibapi::Error::Shutdown => IbError::ConnectionReset,
            other => IbError::Other(format!("{other}")),
        }
    }
}

impl From<std::io::Error> for IbError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::BrokenPipe => IbError::ConnectionReset,
            std::io::ErrorKind::TimedOut => IbError::Timeout(err.to_string()),
            other => IbError::ConnectionFailed(format!("IO error ({other:?}): {err}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Variant construction tests ──

    #[test]
    fn connection_failed_message() {
        let e = IbError::ConnectionFailed("timeout".into());
        assert_eq!(e.to_string(), "connection failed: timeout");
    }

    #[test]
    fn connection_reset_display() {
        let e = IbError::ConnectionReset;
        assert_eq!(e.to_string(), "connection reset");
    }

    #[test]
    fn market_data_contains_code_and_message() {
        let e = IbError::MarketData {
            code: 10197,
            message: "No market data".into(),
        };
        let s = e.to_string();
        assert!(s.contains("10197"));
        assert!(s.contains("No market data"));
    }

    #[test]
    fn order_rejected_contains_code() {
        let e = IbError::OrderRejected {
            code: 201,
            message: "Order rejected".into(),
            rejection_json: Some("{\"reason\":\"bad\"}".into()),
        };
        let s = e.to_string();
        assert!(s.contains("201"));
        assert!(s.contains("Order rejected"));
    }

    #[test]
    fn order_rejected_no_json() {
        let e = IbError::OrderRejected {
            code: 202,
            message: "cancelled".into(),
            rejection_json: None,
        };
        assert!(!e.to_string().is_empty());
    }

    #[test]
    fn farm_disconnect_display() {
        let e = IbError::FarmDisconnect {
            code: 2107,
            message: "HMDS data farm lost".into(),
        };
        let s = e.to_string();
        assert!(s.contains("2107"));
    }

    #[test]
    fn contract_resolution_message() {
        let e = IbError::ContractResolution("SPY: no conid".into());
        assert_eq!(
            e.to_string(),
            "contract resolution failed: SPY: no conid"
        );
    }

    #[test]
    fn competing_session_display() {
        let e = IbError::CompetingSession;
        assert_eq!(
            e.to_string(),
            "empty market data — competing session (error 10197)"
        );
    }

    #[test]
    fn timeout_message() {
        let e = IbError::Timeout("request timed out".into());
        assert_eq!(e.to_string(), "timeout: request timed out");
    }

    #[test]
    fn other_message() {
        let e = IbError::Other("something weird".into());
        assert_eq!(e.to_string(), "something weird");
    }

    // ── classify_notice tests ──

    #[test]
    fn notice_10197_is_competing_session() {
        let e = classify_notice(10197, "Competing live session", "");
        assert!(matches!(e, IbError::CompetingSession));
    }

    #[test]
    fn notice_10000_10999_is_market_data() {
        let e = classify_notice(10199, "Requested market data is not subscribed", "");
        assert!(matches!(e, IbError::MarketData { code: 10199, .. }));
    }

    #[test]
    fn notice_2107_is_farm_disconnect() {
        let e = classify_notice(2107, "HMDS data farm connection is broken", "");
        assert!(matches!(e, IbError::FarmDisconnect { .. }));
    }

    #[test]
    fn notice_200_299_is_order_rejected() {
        let e =
            classify_notice(201, "Order rejected", "{\"reason\":\"insufficient funds\"}");
        assert!(matches!(e, IbError::OrderRejected { code: 201, .. }));
        if let IbError::OrderRejected { rejection_json, .. } = &e {
            assert!(rejection_json.is_some());
        }
    }

    #[test]
    fn notice_200_299_order_rejected_no_json() {
        let e = classify_notice(202, "Order cancelled", "");
        assert!(matches!(e, IbError::OrderRejected { code: 202, .. }));
        if let IbError::OrderRejected { rejection_json, .. } = &e {
            assert!(rejection_json.is_none());
        }
    }

    #[test]
    fn notice_504_is_connection_failed() {
        let e = classify_notice(504, "Not connected", "");
        assert!(matches!(e, IbError::ConnectionFailed(_)));
    }

    #[test]
    fn notice_unknown_falls_to_other() {
        let e = classify_notice(9999, "Unknown code", "");
        assert!(matches!(e, IbError::Other(_)));
    }

    // ── From<ibapi::Error> tests ──

    #[test]
    fn ibapi_connection_failed_maps_directly() {
        let err = ibapi::Error::ConnectionFailed;
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionFailed(_)));
    }

    #[test]
    fn ibapi_connection_reset_maps_directly() {
        let err = ibapi::Error::ConnectionReset;
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionReset));
    }

    #[test]
    fn ibapi_notice_10197_maps_to_competing_session() {
        let notice = ibapi::Notice {
            code: 10197,
            message: "Competing live session".into(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        let err = ibapi::Error::Notice(notice);
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::CompetingSession));
    }

    #[test]
    fn ibapi_io_connection_reset_maps() {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        );
        let ibe: IbError = io_err.into();
        assert!(matches!(ibe, IbError::ConnectionReset));
    }

    #[test]
    fn ibapi_io_timeout_maps() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        let ibe: IbError = io_err.into();
        assert!(matches!(ibe, IbError::Timeout(_)));
    }

    #[test]
    fn ibapi_shutdown_maps_to_connection_reset() {
        let err = ibapi::Error::Shutdown;
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionReset));
    }

    #[test]
    fn ibapi_other_maps_to_other() {
        let err = ibapi::Error::NotImplemented;
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::Other(_)));
    }

    // ── Doc comment tests ──

    #[test]
    fn ib_error_enum_doc_mentions_primary_error_type() {
        let src = include_str!("errors.rs");
        let has_doc = src
            .lines()
            .any(|line| line.starts_with("///") && line.contains("error type for ibcore"));
        assert!(
            has_doc,
            "doc comment missing 'primary error type for ibcore'"
        );
    }

    // ── From<io::Error> tests ──

    #[test]
    fn io_broken_pipe_is_connection_reset() {
        let err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionReset));
    }

    #[test]
    fn io_connection_refused_is_connection_reset() {
        let err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionReset));
    }

    #[test]
    fn io_other_is_connection_failed() {
        let err =
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let ibe: IbError = err.into();
        assert!(matches!(ibe, IbError::ConnectionFailed(_)));
    }

    // ── with_context tests (#32) ──

    #[test]
    fn with_context_prepends_to_message_variants() {
        let cases = [
            with_context("ctx", IbError::MarketData { code: 10199, message: "no sub".into() }),
            with_context("ctx", IbError::ConnectionFailed("dropped".into())),
            with_context("ctx", IbError::Timeout("slow".into())),
            with_context("ctx", IbError::Other("weird".into())),
            with_context("ctx", IbError::ContractResolution("bad".into())),
        ];
        for e in &cases {
            assert!(
                e.to_string().contains("ctx"),
                "context prefix missing from {e}"
            );
        }
        // The code / variant must be preserved, not flattened.
        assert!(matches!(cases[0], IbError::MarketData { code: 10199, .. }));
    }

    #[test]
    fn with_context_leaves_messageless_variants_unchanged() {
        assert!(matches!(
            with_context("ctx", IbError::ConnectionReset),
            IbError::ConnectionReset
        ));
        assert!(matches!(
            with_context("ctx", IbError::CompetingSession),
            IbError::CompetingSession
        ));
    }
}
