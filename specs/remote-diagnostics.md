# Spec: ibcore Remote Diagnostics → ibquirk AI

**Date:** 2026-06-20
**Status:** Draft
**Confidence:** HIGH — trivial HTTP POST, existing DiagnosticEvent struct
**Impact:** LOW on ibcore (~100 lines, feature-gated). MEDIUM on ibquirk (new endpoint + LLM call).

---

## 1. What It Does

ibcore optionally streams `DiagnosticEvent`s to ibquirk's API. ibquirk's AI matches events against its quirk knowledge base and returns real-time diagnosis.

**Before:** User sees error 10197 in logs → Googles → Maybe finds ibquirk → Copies error → Gets diagnosis.

**After:** ibcore POSTs error 10197 to ibquirk → 200ms later → "Q002: Live session blocking paper data — run separate Gateway on port 4002."

---

## 2. Architecture

```
┌──────────────┐     POST /v1/diagnose      ┌──────────────┐
│   ibcore     │ ──────────────────────────→ │   ibquirk    │
│  (Rust/Py)   │    Authorization: Bearer    │  (Vercel)    │
│              │ ←────────────────────────── │              │
│  Diagnostic  │    { diagnosis, quirk }     │  LLM + KB    │
│   Events     │                             │  matching    │
└──────────────┘                             └──────────────┘
```

### 2.1 ibcore Side

New optional method on `IbClient::connect()`:

```rust
// Feature-gated behind "remote-diagnostics"
let ib = IbClient::connect("127.0.0.1", 4002, 1, "delayed", AccountType::Paper)
    .with_remote_diagnostics(
        "https://api.ibquirk.com/v1/diagnose",
        "ibq_live_xxxxxxxx",   // API token
        Duration::from_secs(5), // batching interval
    )
    .await?;
```

When configured, ibcore spawns two background tasks:

**Task A: Event batcher**
- Accumulates `DiagnosticEvent`s in a ring buffer (last 64 events)
- Every `batching_interval` (or immediately on `FarmDisconnect` / `CompetingSession`), POSTs the batch
- Session fingerprint included once per batch (gateway version, OS, account type, client version)

**Task B: Response handler**
- Receives diagnosis JSON
- Emits `RemoteDiagnosis` events on a new `broadcast::Sender` so consumers can subscribe

### 2.2 API Contract

```
POST /v1/diagnose
Content-Type: application/json
Authorization: Bearer ***

{
  "session": {
    "gateway_version": 1030,
    "os": "linux",
    "account_type": "paper",
    "client_version": "0.1.1"
  },
  "events": [
    {
      "error_code": 10197,
      "farm_status": "Inactive",
      "message": "competing live session",
      "timestamp": "2026-06-20T14:30:00Z"
    }
  ]
}
```

**Response (200):**

```json
{
  "diagnoses": [
    {
      "matched_quirk": "Q002",
      "title": "Live Session Blocks Paper Market Data",
      "confidence": 0.94,
      "root_cause": "Paper account shares Gateway instance with live trading session...",
      "workaround": "Run separate Gateway instance on port 4002...",
      "verification": "Check Gateway logs for 'competing session'..."
    }
  ]
}
```

**Response (401):** Invalid/missing API token.
**Response (429):** Rate limited. Includes `Retry-After` header.

### 2.3 ibquirk Side

New Vercel Edge Function at `api.ibquirk.com/v1/diagnose`:

1. **Auth:** Validate API token against Memberful → tier check (Free: 25 calls/mo, Indiv: 100, Pro: unlimited)
2. **Pattern match:** For each `DiagnosticEvent`, run LLM call (Claude Haiku for latency) against the quirk knowledge base
3. **Return:** Array of diagnoses with confidence scores
4. **Log:** Store anonymized event patterns for quirk gap analysis (Pro feature: "We noticed recurring error 2107 in your Gateway — here's a new quirk entry")

---

## 3. New Rust Types

```rust
/// Configuration for remote diagnostic streaming.
#[derive(Debug, Clone)]
pub struct RemoteDiagnosticsConfig {
    pub endpoint: String,          // "https://api.ibquirk.com/v1/diagnose"
    pub api_token: String,         // "ibq_live_xxx"
    pub batch_interval: Duration,  // 5s default
}

/// Diagnosis returned by ibquirk's AI.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteDiagnosis {
    pub matched_quirk: String,     // "Q002"
    pub title: String,
    pub confidence: f64,           // 0.0–1.0
    pub root_cause: String,
    pub workaround: String,
    pub verification: String,
}
```

**Feature gate:** `remote-diagnostics` in `Cargo.toml`:
```toml
[features]
remote-diagnostics = ["dep:reqwest", "dep:serde_json", "dep:tokio"]
```

---

## 4. Python Bindings

```python
from ibcore import IbClient, RemoteDiagnosticsConfig

config = RemoteDiagnosticsConfig(
    endpoint="https://api.ibquirk.com/v1/diagnose",
    api_token="ibq_live_xxx",
    batch_interval_secs=5,
)

ib = await IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
ib.configure_remote_diagnostics(config)

# Subscribe to diagnoses
async for diagnosis in ib.remote_diagnoses():
    if diagnosis.confidence > 0.8:
        print(f"[{diagnosis.matched_quirk}] {diagnosis.title}")
        print(f"  Fix: {diagnosis.workaround}")
```

---

## 5. Tier Limits (ibquirk Side)

| Tier | Diagnose calls/month | Real-time? | Max events/batch |
|------|---------------------|------------|------------------|
| Free | 25 | Batched only (30s interval) | 16 |
| Indiv $29/mo | 100 | Batch (15s) | 32 |
| Pro $99/mo | Unlimited | Immediate on critical events | 64 |

---

## 6. What This Does NOT Do

- **Does NOT send order data or P&L.** Only `DiagnosticEvent` fields (error codes, messages, farm states).
- **Does NOT auto-fix.** Diagnosis only. The workaround text says what to do; the user executes it.
- **Does NOT replace local `diagnostic_events()`.** Local broadcast channel still works. Remote is additive.
- **Does NOT phone home by default.** Must explicitly call `with_remote_diagnostics()`.

---

## 7. Sprint Plan Placement

**ibcore side (Week 6 — Jul 20-26):**
- `remote-diagnostics` feature gate + `RemoteDiagnosticsConfig`
- Batcher background task
- Response handler + `RemoteDiagnosis` broadcast channel
- Python binding exposure
- Tests: mock HTTP server, verify batches POST correctly

**ibquirk side (Week 7 — Jul 27-Aug 2):**
- `POST /v1/diagnose` Edge Function
- API token auth via Memberful tier check
- LLM pattern matching (Haiku for latency)
- Rate limiting per tier
- Anonymized event logging

**Integration test (Week 8 — Aug 3-9):**
- End-to-end: ibcore connects to Gateway → error 10197 → POST to ibquirk → Q002 returned
- Verify latency <500ms p95
- Verify tier enforcement

---

## 8. Strategic Value

This is the feature that makes ibquirk viral. Every ibcore user who flips `with_remote_diagnostics()` becomes an ibquirk demo. When they see `"Q002 — confidence 0.94"` appear in their logs 200ms after a Gateway error, they convert. The library is the funnel.
