//! Remote diagnostics — batched HTTP POST of `DiagnosticEvent`s to ibquirk API.
//! Feature-gated behind `remote-diagnostics`.

use serde::{Deserialize, Serialize};

/// Diagnosis returned by ibquirk's AI.
#[derive(Debug, Clone)]
pub struct RemoteDiagnosis {
    pub matched_quirk: String,
    pub title: String,
    pub confidence: f64,
    pub root_cause: String,
    pub workaround: String,
    pub verification: String,
}

/// Configuration for remote diagnostic event streaming.
#[derive(Clone)]
pub struct RemoteDiagnosticsConfig {
    pub endpoint: String,
    pub api_token: String,
    pub batch_interval: std::time::Duration,
}

impl std::fmt::Debug for RemoteDiagnosticsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDiagnosticsConfig")
            .field("endpoint", &self.endpoint)
            .field("api_token", &"REDACTED")
            .field("batch_interval", &self.batch_interval)
            .finish()
    }
}

/// Session fingerprint for batch POSTs.
#[derive(Debug, Clone)]
pub struct SessionFingerprint {
    pub gateway_version: i32,
    pub os: &'static str,
    pub account_type: crate::AccountType,
    pub client_version: &'static str,
}

/// Diagnostic event payload for wire transmission.
#[derive(Debug, Clone)]
pub struct DiagnosticEventPayload {
    pub error_code: i32,
    pub farm_status: crate::FarmState,
    pub message: String,
    pub timestamp: String,
    pub gateway_version: i32,
}

/// A batch of diagnostic events ready for POST.
#[derive(Debug, Clone)]
pub struct DiagnosticBatch {
    pub session: SessionFingerprint,
    pub events: Vec<DiagnosticEventPayload>,
}

pub const BATCHER_TO_POLLER_CAPACITY: usize = 16;
pub const DIAGNOSIS_BUFFER: usize = 32;
pub const MAX_BACKOFF_SECS: u64 = 60;

#[cfg(test)]
#[cfg(feature = "remote-diagnostics")]
mod tests {
    use super::*;

    #[test]
    fn remote_diagnosis_deserializes_full_json() {
        let json = r#"{
            "matched_quirk": "Q002",
            "title": "Live Session Blocks Paper Market Data",
            "confidence": 0.94,
            "root_cause": "Paper shares Gateway instance with live session",
            "workaround": "Run separate Gateway on port 4002",
            "verification": "Check logs for 'competing session'"
        }"#;
        let d: RemoteDiagnosis = serde_json::from_str(json).unwrap();
        assert_eq!(d.matched_quirk, "Q002");
    }

    #[test]
    fn config_debug_redacts_token() {
        let cfg = RemoteDiagnosticsConfig {
            endpoint: "https://api.ibquirk.com/v1/diagnose".into(),
            api_token: "ibq_live_secret_123".into(),
            batch_interval: std::time::Duration::from_secs(5),
        };
        let debug = format!("{cfg:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("ibq_live_secret"));
    }

    #[test]
    fn session_fingerprint_serializes_to_json() {
        let fp = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };
        let json = serde_json::to_string(&fp).unwrap();
        assert!(json.contains("gateway_version"));
    }

    #[test]
    fn diagnostic_batch_serializes_full() {
        let batch = DiagnosticBatch {
            session: SessionFingerprint {
                gateway_version: 1030,
                os: "linux",
                account_type: crate::AccountType::Paper,
                client_version: "0.1.1",
            },
            events: vec![DiagnosticEventPayload {
                error_code: 10197,
                farm_status: crate::FarmState::Unknown(10197),
                message: "competing live session".into(),
                timestamp: "2026-06-20T14:30:00Z".into(),
                gateway_version: 1030,
            }],
        };
        let json = serde_json::to_string_pretty(&batch).unwrap();
        assert!(json.contains("10197"));
    }

    #[test]
    fn diagnostic_batch_empty_events_array() {
        let batch = DiagnosticBatch {
            session: SessionFingerprint {
                gateway_version: 1030,
                os: "linux",
                account_type: crate::AccountType::Paper,
                client_version: "0.1.1",
            },
            events: vec![],
        };
        let json = serde_json::to_string(&batch).unwrap();
        assert!(json.contains("\"events\":[]"));
    }
}
