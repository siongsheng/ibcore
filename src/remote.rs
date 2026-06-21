//! Remote diagnostics — batched HTTP POST of `DiagnosticEvent`s to ibquirk API.
//! Feature-gated behind `remote-diagnostics`.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tracing;

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

/// Maximum ring buffer size for the batcher.
const RING_BUFFER_CAPACITY: usize = 64;

/// Maximum time to wait for an mpsc send before dropping the batch.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

impl From<&crate::DiagnosticEvent> for DiagnosticEventPayload {
    fn from(event: &crate::DiagnosticEvent) -> Self {
        Self {
            error_code: event.error_code,
            farm_status: event.farm_status,
            message: event.error_message.clone(),
            timestamp: event.timestamp.to_rfc3339(),
            gateway_version: event.gateway_version,
        }
    }
}

/// Run the diagnostic event batcher background task.
///
/// Subscribes to `diagnostic_rx`, accumulates events in a ring buffer, and
/// flushes batches via `batch_tx` at every `interval_duration` tick.
///
/// On error_code 10197 (competing session / FarmDisconnect), flushes
/// immediately.
pub(crate) async fn run_batcher(
    mut diagnostic_rx: broadcast::Receiver<crate::DiagnosticEvent>,
    interval_duration: Duration,
    batch_tx: mpsc::Sender<DiagnosticBatch>,
    session: SessionFingerprint,
) {
    let mut ring: VecDeque<DiagnosticEventPayload> = VecDeque::with_capacity(RING_BUFFER_CAPACITY);
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            result = diagnostic_rx.recv() => {
                match result {
                    Ok(event) => {
                        if ring.len() >= RING_BUFFER_CAPACITY {
                            let dropped = ring.pop_front();
                            if let Some(dropped) = dropped {
                                tracing::warn!(
                                    "ring buffer overflow — dropped event code={}",
                                    dropped.error_code
                                );
                            }
                        }
                        let payload = DiagnosticEventPayload::from(&event);
                        let is_critical = event.error_code == 10197;
                        ring.push_back(payload);
                        if is_critical {
                            flush_batch(&mut ring, &batch_tx, &session).await;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("batcher: diagnostic broadcast closed");
                        flush_batch(&mut ring, &batch_tx, &session).await;
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("batcher: lagged by {n} diagnostic events");
                    }
                }
            }
            _ = interval.tick() => {
                flush_batch(&mut ring, &batch_tx, &session).await;
            }
        }
    }
}

/// Flush the current ring buffer as a batch, if non-empty.
/// Clears the ring after a successful send to prevent cumulative duplication.
async fn flush_batch(
    ring: &mut VecDeque<DiagnosticEventPayload>,
    batch_tx: &mpsc::Sender<DiagnosticBatch>,
    session: &SessionFingerprint,
) {
    if ring.is_empty() {
        return;
    }

    let batch = DiagnosticBatch {
        session: session.clone(),
        events: ring.iter().cloned().collect(),
    };

    match tokio::time::timeout(FLUSH_TIMEOUT, batch_tx.send(batch)).await {
        Ok(Ok(())) => {
            ring.clear();
        }
        Ok(Err(mpsc::error::SendError { .. })) => {
            tracing::error!("batcher: poller dropped — stopping flush");
        }
        Err(_) => {
            tracing::warn!("batcher: flush timed out after {FLUSH_TIMEOUT:?} — batch dropped");
        }
    }
}

/// Run the HTTP poller background task.
///
/// Receives batches from `batch_rx`, POSTs them to the ibquirk endpoint,
/// and emits `RemoteDiagnosis` responses on `diagnosis_tx`.
pub(crate) async fn run_poller(
    mut batch_rx: mpsc::Receiver<DiagnosticBatch>,
    config: RemoteDiagnosticsConfig,
    diagnosis_tx: broadcast::Sender<RemoteDiagnosis>,
) {
    let client = reqwest::Client::new();
    let mut consecutive_failures: u64 = 0;

    while let Some(batch) = batch_rx.recv().await {
        let payload = match serde_json::to_string(&batch) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("poller: failed to serialize batch: {e}");
                continue;
            }
        };

        let response = client
            .post(&config.endpoint)
            .header("Content-Type", "application/json")
            .bearer_auth(&config.api_token)
            .body(payload)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    consecutive_failures = 0;
                    if let Ok(diagnoses) = resp.json::<DiagnosticResponse>().await {
                        for d in diagnoses.diagnoses {
                            let _ = diagnosis_tx.send(d);
                        }
                    }
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    tracing::warn!("poller: 429 rate limited");
                } else {
                    consecutive_failures += 1;
                    let backoff = Duration::from_secs(
                        (consecutive_failures * config.batch_interval.as_secs())
                            .min(MAX_BACKOFF_SECS),
                    );
                    tracing::warn!(
                        "poller: POST returned {status} — backoff {backoff:?} (failure #{consecutive_failures})"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                let backoff = Duration::from_secs(
                    (consecutive_failures * config.batch_interval.as_secs())
                        .min(MAX_BACKOFF_SECS),
                );
                tracing::error!(
                    "poller: request failed: {e} — backoff {backoff:?} (failure #{consecutive_failures})"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }

    tracing::error!("poller: batcher task died — stopping poller");
}

/// Wrapper for the ibquirk API response body.
#[derive(Debug, Deserialize)]
struct DiagnosticResponse {
    diagnoses: Vec<RemoteDiagnosis>,
}

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

    // ── Batcher async tests ──

    #[tokio::test]
    async fn batcher_flushes_on_interval() {
        let (diagnostic_tx, diagnostic_rx) = broadcast::channel(16);
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let session = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };

        let handle = tokio::spawn(run_batcher(
            diagnostic_rx,
            Duration::from_millis(50),
            batch_tx,
            session,
        ));

        let event = crate::DiagnosticEvent {
            gateway_version: 1030,
            error_code: 2104,
            error_message: "OK".into(),
            error_time: None,
            farm_status: crate::FarmState::Ok,
            connection_state: crate::ConnectionState::Connected,
            account_type: crate::AccountType::Paper,
            os: "linux",
            timestamp: chrono::Utc::now(),
        };
        diagnostic_tx.send(event).unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .expect("timeout waiting for batch")
            .expect("batcher closed before sending batch");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].error_code, 2104);

        handle.abort();
    }

    #[tokio::test]
    async fn batcher_flushes_immediately_on_10197() {
        let (diagnostic_tx, diagnostic_rx) = broadcast::channel(16);
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let session = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };

        let handle = tokio::spawn(run_batcher(
            diagnostic_rx,
            Duration::from_secs(60),
            batch_tx,
            session,
        ));

        let event = crate::DiagnosticEvent {
            gateway_version: 1030,
            error_code: 10197,
            error_message: "competing session".into(),
            error_time: None,
            farm_status: crate::FarmState::Unknown(10197),
            connection_state: crate::ConnectionState::Connected,
            account_type: crate::AccountType::Paper,
            os: "linux",
            timestamp: chrono::Utc::now(),
        };
        diagnostic_tx.send(event).unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .expect("timeout waiting for critical flush")
            .expect("batcher closed before sending batch");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].error_code, 10197);

        handle.abort();
    }

    #[tokio::test]
    async fn batcher_skips_empty_flush() {
        let (diagnostic_tx, diagnostic_rx) = broadcast::channel(16);
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let session = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };

        let handle = tokio::spawn(run_batcher(
            diagnostic_rx,
            Duration::from_millis(20),
            batch_tx,
            session,
        ));

        let result = tokio::time::timeout(Duration::from_millis(100), batch_rx.recv()).await;
        assert!(result.is_err(), "empty batch should not be sent");

        let event = crate::DiagnosticEvent {
            gateway_version: 1030,
            error_code: 2104,
            error_message: "OK".into(),
            error_time: None,
            farm_status: crate::FarmState::Ok,
            connection_state: crate::ConnectionState::Connected,
            account_type: crate::AccountType::Paper,
            os: "linux",
            timestamp: chrono::Utc::now(),
        };
        diagnostic_tx.send(event).unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .expect("timeout waiting for batch after event")
            .expect("batcher closed");
        assert_eq!(batch.events.len(), 1);

        handle.abort();
    }

    #[tokio::test]
    async fn batcher_exits_on_broadcast_close() {
        let (diagnostic_tx, diagnostic_rx) = broadcast::channel::<crate::DiagnosticEvent>(16);
        let (batch_tx, _batch_rx) = mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let session = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };

        let handle = tokio::spawn(run_batcher(
            diagnostic_rx,
            Duration::from_secs(60),
            batch_tx,
            session,
        ));

        drop(diagnostic_tx);

        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "batcher did not exit on broadcast close");
    }

    #[tokio::test]
    async fn batcher_ring_buffer_overflow_drops_oldest() {
        let (diagnostic_tx, diagnostic_rx) = broadcast::channel::<crate::DiagnosticEvent>(256);
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCHER_TO_POLLER_CAPACITY);
        let session = SessionFingerprint {
            gateway_version: 1030,
            os: "linux",
            account_type: crate::AccountType::Paper,
            client_version: "0.1.1",
        };

        let handle = tokio::spawn(run_batcher(
            diagnostic_rx,
            Duration::from_millis(50),
            batch_tx,
            session,
        ));

        for i in 0..RING_BUFFER_CAPACITY + 10 {
            let event = crate::DiagnosticEvent {
                gateway_version: 1030,
                error_code: 2100 + i as i32,
                error_message: format!("event {i}"),
                error_time: None,
                farm_status: crate::FarmState::Ok,
                connection_state: crate::ConnectionState::Connected,
                account_type: crate::AccountType::Paper,
                os: "linux",
                timestamp: chrono::Utc::now(),
            };
            diagnostic_tx.send(event).unwrap();
        }

        let batch = tokio::time::timeout(Duration::from_secs(2), batch_rx.recv())
            .await
            .expect("timeout")
            .expect("batcher closed");

        assert!(batch.events.len() <= RING_BUFFER_CAPACITY);
        let first_code = batch.events[0].error_code;
        assert!(
            first_code > 2100,
            "expected newer events, got code={first_code}"
        );

        handle.abort();
    }
}
