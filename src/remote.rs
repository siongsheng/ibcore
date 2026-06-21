//! Remote diagnostics — batched HTTP POST of `DiagnosticEvent`s to ibquirk API.
//! Feature-gated behind `remote-diagnostics`.

use serde::{Deserialize, Serialize};

/// Diagnosis returned by ibquirk's AI.
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Serialize)]
pub struct SessionFingerprint {
    pub gateway_version: i32,
    pub os: &'static str,
    pub account_type: crate::AccountType,
    pub client_version: &'static str,
}

/// Diagnostic event payload for wire transmission.
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticEventPayload {
    pub error_code: i32,
    pub farm_status: crate::FarmState,
    pub message: String,
    pub timestamp: String,
    pub gateway_version: i32,
}

/// A batch of diagnostic events ready for POST.
#[derive(Debug, Clone, Serialize)]
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
        assert_eq!(d.confidence, 0.94);
        assert!(d.confidence > 0.0 && d.confidence <= 1.0);
    }

    #[test]
    fn remote_diagnosis_deserializes_zero_confidence() {
        let json = r#"{
            "matched_quirk": "Q000",
            "title": "Unknown condition",
            "confidence": 0.0,
            "root_cause": "",
            "workaround": "",
            "verification": ""
        }"#;
        let d: RemoteDiagnosis = serde_json::from_str(json).unwrap();
        assert_eq!(d.confidence, 0.0);
        assert_eq!(d.matched_quirk, "Q000");
    }

    #[test]
    fn remote_diagnosis_deserializes_max_confidence() {
        let json = r#"{
            "matched_quirk": "Q999",
            "title": "Perfect match",
            "confidence": 1.0,
            "root_cause": "Matched perfectly",
            "workaround": "None needed",
            "verification": "Verified"
        }"#;
        let d: RemoteDiagnosis = serde_json::from_str(json).unwrap();
        assert_eq!(d.confidence, 1.0);
    }

    #[test]
    fn remote_diagnosis_rejects_missing_field() {
        let json = r#"{
            "matched_quirk": "Q002",
            "confidence": 0.94
        }"#;
        let result: Result<RemoteDiagnosis, _> = serde_json::from_str(json);
        assert!(result.is_err(), "missing fields should fail deserialization");
    }

    #[test]
    fn config_debug_redacts_token() {
        let cfg = RemoteDiagnosticsConfig {
            endpoint: "https://api.ibquirk.com/v1/diagnose".into(),
            api_token: "ibq_live_secret_123".into(),
            batch_interval: std::time::Duration::from_secs(5),
        };
        let debug = format!("{cfg:?}");
        assert!(debug.contains("REDACTED"), "token should be redacted: {debug}");
        assert!(!debug.contains("ibq_live_secret"), "token leaked: {debug}");
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
        assert!(json.contains("1030"));
        assert!(json.contains("client_version"));
        assert!(json.contains("0.1.1"));
        assert!(json.contains("account_type"));
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
        assert!(json.contains("competing live session"));
        assert!(json.contains("farm_status"));
        assert!(json.contains("\"Unknown\": 10197"));
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
