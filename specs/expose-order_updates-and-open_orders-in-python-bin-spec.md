# expose order_updates and open_orders in Python bindings for ibcore

Let me find the actual spec files — that file looks like a creation log, not the spec content.

Now let me check the Rust orders module and the existing Python receiver pattern.

Now I have full context. Let me read the remaining sections of the Python lib.rs for the PyIbClient struct and drop/disconnect handling.

Good — I now have full context on the existing code, the with_client/mutex pattern, PyDiagnosticEventReceiver conventions, and the OrderStatusStream internals. Let me analyse each gap and write the updated spec.

Here's my gap-by-gap verdict:
    
    Gap: 1 — re-export
    Status: VALID
    Action: Add task for pub use in src/lib.rs
    ────────────────────────────────────────
    Gap: 2 — #[new]
    Status: VALID
    Action: Add #[new] with kwargs to both pyclasses
    ────────────────────────────────────────
    Gap: 3 — Err(IbError)
    Status: VALID
    Action: Log tracing::warn!, continue stream (don't forward)
    ────────────────────────────────────────
    Gap: 4 — unbounded mpsc
    Status: VALID
    Action: Use sync_channel(1024), log warn + skip on full
    ────────────────────────────────────────
    Gap: 5 — Drop impl
    Status: VALID
    Action: Explicit impl Drop calling .take().abort()
    ────────────────────────────────────────
    Gap: 6 — client drop + receiver alive
    Status: VALID
    Action: Document: channel disconnects → StopIteration
    ────────────────────────────────────────
    Gap: 7 — next return type
    Status: VALID
    Action: Use PyResult<Option<PyOrderStatusEvent>> (match existing pattern)
    ────────────────────────────────────────
    Gap: 8 — mutex deadlock
    Status: NOT A BUG
    Action: Clarify: mutex released between calls, no deadlock
    ────────────────────────────────────────
    Gap: 9 — order_id=0 commission
    Status: VALID
    Action: Document: commission-only events have order_id=0, kind=Filled
    ────────────────────────────────────────
    Gap: 10 — GIL fallback trigger
    Status: VALID
    Action: Document as manual code-change fallback with marker comment
    ────────────────────────────────────────
    Gap: 11 — KeyboardInterrupt
    Status: VALID CONCERN
    Action: Document: delayed until next event (recv is uninterruptible)
    ────────────────────────────────────────
    Gap: 12 — next blocks indefinitely
    Status: DESIGN INTENT
    Action: Document explicitly: blocking is by design
    
    Now writing updated spec files.

Quick consistency sweep — let me verify each gap maps to content across all three files.

GAP 12 check used a too-strict regex. Let me fix it — the concept is there in the "Key design decisions" block but with different phrasing.

Confirmed — it's on plan.md line 16. The regex was too strict (backticks). All 12 gaps are covered.

All 12 gaps addressed. Here's the summary:
    
    
    
    Spec files updated (3 files):
    
    File: specs/order-python-bindings/requirements.md
    What changed: +2 sections (§7 GIL Fallback, §8 Security); +4 constraints;
      +5 edge cases; +1 assumption (mutex non-deadlock)
    ────────────────────────────────────────
    File: specs/order-python-bindings/plan.md
    What changed: +1 task (Task 0 — re-exports); API signatures revised with
      #[new], PyResult<Option<...>>, sync_channel, impl Drop; background task
      pseudocode with error handling + backpressure
    ────────────────────────────────────────
    File: specs/order-python-bindings/validation.md
    What changed: +5 criteria (C11–C15); +2 manual verification rows; +6
      regression checks; C2/C3 extended with default/partial construction
      tests
    
    Key decisions per gap:
    
    Gap: 1 — re-exports
    Verdict: VALID
    Decision: New Task 0: pub use orders::{...} in src/lib.rs
    ────────────────────────────────────────
    Gap: 2 — #[new]
    Verdict: VALID
    Decision: Both pyclasses get #[new] with full keyword defaults
    ────────────────────────────────────────
    Gap: 3 — Err(IbError)
    Verdict: VALID
    Decision: tracing::warn! + continue (never forwarded to Python)
    ────────────────────────────────────────
    Gap: 4 — unbounded channel
    Verdict: VALID
    Decision: sync_channel(1024); try_send + warn + skip on full
    ────────────────────────────────────────
    Gap: 5 — missing Drop
    Verdict: VALID
    Decision: Explicit impl Drop calling .take().abort()
    ────────────────────────────────────────
    Gap: 6 — client drop + receiver
    Verdict: VALID
    Decision: Documented: clean RecvError → StopIteration
    ────────────────────────────────────────
    Gap: 7 — return type
    Verdict: VALID
    Decision: PyResult<Option<PyOrderStatusEvent>> (match existing)
    ────────────────────────────────────────
    Gap: 8 — mutex deadlock
    Verdict: NOT A BUG
    Decision: Clarified: mutex released between calls; no deadlock
    ────────────────────────────────────────
    Gap: 9 — order_id=0 commission
    Verdict: VALID
    Decision: Documented: kind="Filled", order_id=0, check commission is not
      None
    ────────────────────────────────────────
    Gap: 10 — GIL fallback trigger
    Verdict: VALID
    Decision: Manual code-change fallback with // GIL_FALLBACK: marker
    ────────────────────────────────────────
    Gap: 11 — KeyboardInterrupt
    Verdict: VALID CONCERN
    Decision: Documented: delayed until next event (recv is uninterruptible)
    ────────────────────────────────────────
    Gap: 12 — next blocks indefinitely
    Verdict: DESIGN INTENT
    Decision: Explicitly documented as by-design