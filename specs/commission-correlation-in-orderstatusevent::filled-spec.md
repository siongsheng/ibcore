# Commission correlation in OrderStatusEvent::Filled — add exec_id field from CommissionReport so fills and commissions can be matched

Let me first check the ibapi source to understand whether OrderStatus also carries an execution_id — this is essential to addressing GAP 1 properly.

Now I have full context. Let me also quickly verify there's no Execution processing elsewhere.

Now I have all the facts. Let me compile the complete analysis and write the updated spec.
    
    Key findings from source inspection:
    - ibapi::OrderStatus (line 1549): No execution_id field — only order_id, status, filled, remaining, average_fill_price, perm_id, parent_id, last_fill_price, client_id, why_held, market_cap_price
    - ibapi::Execution (line 1461): Has execution_id: String AND order_id: i32 — but currently skipped by OrderStatusStream::next() at line 147 (// OpenOrder / ExecutionData — skip, handled elsewhere)
    - ibapi::CommissionReport (line 1411): Has execution_id: String
    - OrderStatusEvent derives PartialEq (line 53)
    - Python consumers use positional args: cls.call1(("Filled", 42, 50.0, 450.25, 1.50, "", "")) — 7 args

Let me verify the spec reads correctly:

Spec is updated and complete at /home/opc/ibcore/specs/commission-correlation-in-orderstatusevent::filled-spec.md (400 lines, 21KB).
    
    Here's how each gap was resolved:
    
    GAP 1 — Does OrderStatus carry execution_id?
    Confirmed NO via source inspection (ibapi::OrderStatus line 1549: no execution_id field). Added a Data Source Inventory table showing all three ibapi structs. The consumer matching strategy was completely rewritten — since fill events can never carry execution_id, consumers must use temporal pairing. Noted that processing ibapi::Execution messages (which have both order_id and execution_id) is the correct long-term solution.
    
    GAP 2 — Canonical commission identification
    execution_id.is_some() is now canonical. order_id == 0 is retained as backward-compatible fallback with the false-positive caveat documented. Added forward-compatibility rationale.
    
    GAP 3 — Empty string coercion
    Added normalize_execution_id() helper that coerces "" to None. Included a coercion table and rationale (Some("") forces every consumer to check for empty strings).
    
    GAP 4 — Commission-before-fill race
    Documented that consumers must buffer pending commissions in a HashMap<execution_id, CommissionData> with a timeout (30s suggested) to prevent unbounded growth.
    
    GAP 5 — Stream ordering
    Explicitly stated: NO ordering guarantees. The stream does not buffer, reorder, or correlate.
    
    GAP 6 — Clone vs move
    Corrected to move semantics. The CommissionReport is consumed by the match arm. The previous .clone() in Task 1 was an error.
    
    GAP 7 — Field ordering
    execution_id placed LAST (after commission). Three-part justification: least disruptive to destructuring, semantically secondary, Rust convention.
    
    GAP 8 — PartialEq
    Acknowledged. All 4 existing Filled test constructors identified by line number. The map_filled_status assert_eq! must include execution_id: None.
    
    GAP 9 — Python positional args
    Three-case analysis: keyword callers (no impact), 7-arg positional (no impact — default kicks in), 8-arg positional (would break, but no known consumers). End-of-list placement prevents argument shift.
    
    GAP 10 — Debug format testing
    Replaced with field-level assertions. Task 2 now specifies 5 new tests using event.execution_id == Some(...) and event.execution_id.is_none() rather than format!("{e:?}"). Rationale included.