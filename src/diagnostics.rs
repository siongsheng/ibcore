//! Diagnostic event types — structured notifications from the IB Gateway notice stream.
//!
//! Provides [`DiagnosticEvent`] for broadcasting structured error information,
//! plus the supporting enums [`FarmState`], [`ConnectionState`], and
//! [`AccountType`].
//!
//! # Example
//! ```
//! use ibcore::diagnostics::{DiagnosticEvent, FarmState, ConnectionState, AccountType};
//!
//! let event = DiagnosticEvent {
//!     gateway_version: 221,
//!     error_code: 2104,
//!     error_message: "Market data farm connection is OK".into(),
//!     error_time: None,
//!     farm_status: FarmState::Ok,
//!     connection_state: ConnectionState::Connected,
//!     account_type: AccountType::Paper,
//!     os: std::env::consts::OS,
//!     timestamp: chrono::Utc::now(),
//! };
//! assert_eq!(event.farm_status.to_string(), "ok");
//! ```

use serde::{Deserialize, Serialize};

/// A structured diagnostic event emitted when the IB Gateway notice stream
/// produces an error, warning, or farm-status notification.
///
/// These are broadcast via `tokio::sync::broadcast` so that consumers
/// (logging, health-check, alerting) can subscribe independently.
#[derive(Debug, Clone)]
pub struct DiagnosticEvent {
    /// IB Gateway/TWS server version number.
    pub gateway_version: i32,
    /// IB error code from the notice.
    pub error_code: i32,
    /// Human-readable error message.
    pub error_message: String,
    /// Optional timestamp when the error occurred (IB server versions >= 194).
    pub error_time: Option<time::OffsetDateTime>,
    /// Classification of farm-status codes (2104, 2105, etc.).
    pub farm_status: FarmState,
    /// Current connection state.
    pub connection_state: ConnectionState,
    /// Whether this is a live or paper account.
    pub account_type: AccountType,
    /// Operating system identifier (from `std::env::consts::OS`).
    pub os: &'static str,
    /// Wall-clock timestamp when the event was created.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Farm connection status, classified from IB notification codes.
///
/// | IB Code | FarmState   |
/// |---------|-------------|
/// | 2104    | `Ok`        |
/// | 2106    | `Ok`        |
/// | 2105    | `Warning`   |
/// | 2108    | `Warning`   |
/// | 2107    | `Inactive`  |
/// | other   | `Unknown`   |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FarmState {
    /// Data farm is operating normally (codes 2104, 2106).
    Ok,
    /// Data farm has a warning condition (codes 2105, 2108).
    Warning,
    /// Data farm is disconnected (code 2107).
    Inactive,
    /// Non-farm error code (catch-all).
    Unknown(i32),
}

impl std::fmt::Display for FarmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FarmState::Ok => write!(f, "ok"),
            FarmState::Warning => write!(f, "warning"),
            FarmState::Inactive => write!(f, "inactive"),
            FarmState::Unknown(code) => write!(f, "unknown({code})"),
        }
    }
}

/// Gateway connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connected to IB Gateway.
    Connected,
    /// Disconnected from IB Gateway.
    Disconnected,
    /// Attempting to reconnect.
    Reconnecting,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Reconnecting => write!(f, "reconnecting"),
        }
    }
}

/// IB account type — live trading or paper simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountType {
    /// Live trading account.
    Live,
    /// Paper/simulation account.
    Paper,
}

impl std::fmt::Display for AccountType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountType::Live => write!(f, "live"),
            AccountType::Paper => write!(f, "paper"),
        }
    }
}

/// Classify an IB error code into a [`FarmState`].
///
/// Only codes 2104–2108 are farm-related; everything else is `Unknown`.
///
/// # Example
/// ```
/// use ibcore::diagnostics::{classify_farm, FarmState};
///
/// assert_eq!(classify_farm(2104), FarmState::Ok);
/// assert_eq!(classify_farm(2107), FarmState::Inactive);
/// assert_eq!(classify_farm(999), FarmState::Unknown(999));
/// ```
pub fn classify_farm(code: i32) -> FarmState {
    match code {
        2104 | 2106 => FarmState::Ok,
        2105 | 2108 => FarmState::Warning,
        2107 => FarmState::Inactive,
        other => FarmState::Unknown(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // ── classify_farm tests ──

    #[test]
    fn farm_2104_is_ok() {
        assert_eq!(classify_farm(2104), FarmState::Ok);
    }

    #[test]
    fn farm_2106_is_ok() {
        assert_eq!(classify_farm(2106), FarmState::Ok);
    }

    #[test]
    fn farm_2105_is_warning() {
        assert_eq!(classify_farm(2105), FarmState::Warning);
    }

    #[test]
    fn farm_2108_is_warning() {
        assert_eq!(classify_farm(2108), FarmState::Warning);
    }

    #[test]
    fn farm_2107_is_inactive() {
        assert_eq!(classify_farm(2107), FarmState::Inactive);
    }

    #[test]
    fn farm_999_is_unknown() {
        assert_eq!(classify_farm(999), FarmState::Unknown(999));
    }

    #[test]
    fn farm_0_is_unknown() {
        assert_eq!(classify_farm(0), FarmState::Unknown(0));
    }

    // ── Display tests ──

    #[test]
    fn farm_state_display_ok() {
        assert_eq!(FarmState::Ok.to_string(), "ok");
    }

    #[test]
    fn farm_state_display_warning() {
        assert_eq!(FarmState::Warning.to_string(), "warning");
    }

    #[test]
    fn farm_state_display_inactive() {
        assert_eq!(FarmState::Inactive.to_string(), "inactive");
    }

    #[test]
    fn farm_state_display_unknown() {
        assert_eq!(FarmState::Unknown(42).to_string(), "unknown(42)");
    }

    #[test]
    fn connection_state_display_connected() {
        assert_eq!(ConnectionState::Connected.to_string(), "connected");
    }

    #[test]
    fn connection_state_display_disconnected() {
        assert_eq!(ConnectionState::Disconnected.to_string(), "disconnected");
    }

    #[test]
    fn connection_state_display_reconnecting() {
        assert_eq!(ConnectionState::Reconnecting.to_string(), "reconnecting");
    }

    #[test]
    fn account_type_display_live() {
        assert_eq!(AccountType::Live.to_string(), "live");
    }

    #[test]
    fn account_type_display_paper() {
        assert_eq!(AccountType::Paper.to_string(), "paper");
    }

    // ── DiagnosticEvent construction test ──

    #[test]
    fn diagnostic_event_construct() {
        let event = DiagnosticEvent {
            gateway_version: 221,
            error_code: 2104,
            error_message: "Market data farm connection OK".into(),
            error_time: None,
            farm_status: FarmState::Ok,
            connection_state: ConnectionState::Connected,
            account_type: AccountType::Paper,
            os: std::env::consts::OS,
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(event.gateway_version, 221);
        assert_eq!(event.error_code, 2104);
        assert_eq!(event.farm_status, FarmState::Ok);
        assert_eq!(event.connection_state, ConnectionState::Connected);
        assert_eq!(event.account_type, AccountType::Paper);
        assert_eq!(event.os, std::env::consts::OS);
    }

    // ── FarmState Serialize/Deserialize tests ──

    #[test]
    fn farm_state_ok_serializes_to_json() {
        let json = serde_json::to_string(&FarmState::Ok).unwrap();
        assert_eq!(json, "\"Ok\"");
    }

    #[test]
    fn farm_state_warning_serializes_to_json() {
        let json = serde_json::to_string(&FarmState::Warning).unwrap();
        assert_eq!(json, "\"Warning\"");
    }

    #[test]
    fn farm_state_inactive_serializes_to_json() {
        let json = serde_json::to_string(&FarmState::Inactive).unwrap();
        assert_eq!(json, "\"Inactive\"");
    }

    #[test]
    fn farm_state_unknown_serializes_to_json_object() {
        let json = serde_json::to_string(&FarmState::Unknown(10197)).unwrap();
        assert_eq!(json, "{\"Unknown\":10197}");
    }

    #[test]
    fn farm_state_ok_deserializes_from_json() {
        let state: FarmState = serde_json::from_str("\"Ok\"").unwrap();
        assert_eq!(state, FarmState::Ok);
    }

    #[test]
    fn farm_state_unknown_deserializes_from_json_object() {
        let state: FarmState = serde_json::from_str("{\"Unknown\":10197}").unwrap();
        assert_eq!(state, FarmState::Unknown(10197));
    }

    #[test]
    fn farm_state_round_trips_ok() {
        for (original, expected_json) in &[
            (FarmState::Ok, "\"Ok\""),
            (FarmState::Warning, "\"Warning\""),
            (FarmState::Inactive, "\"Inactive\""),
            (FarmState::Unknown(0), "{\"Unknown\":0}"),
            (FarmState::Unknown(2107), "{\"Unknown\":2107}"),
            (FarmState::Unknown(9999), "{\"Unknown\":9999}"),
        ] {
            let json = serde_json::to_string(original).unwrap();
            assert_eq!(&json, expected_json, "serialize failed for {original:?}");
            let deserialized: FarmState = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, *original, "deserialize failed for {original:?}");
        }
    }
}
