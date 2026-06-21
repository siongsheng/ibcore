# Spec: Commission Correlation in OrderStatusEvent::Filled

**Date:** 2026-06-21
**Status:** Draft
**Confidence:** HIGH — single struct change, IB fields are known
**Impact:** LOW — Rust-only, no API break if backward compat

---

## Problem

IB sends fills and commissions as separate messages. Current ibcore emits two
`OrderStatusEvent::Filled` events that can't be matched:

```rust
// Fill comes first
Filled { order_id: 42, filled_qty: 2.0, avg_price: 450.50, commission: None }

// Commission comes later — order_id is 0, can't match
Filled { order_id: 0, filled_qty: 0.0, avg_price: 0.0, commission: Some(1.30) }
```

## IB's Actual Linking

`CommissionReport` has `exec_id` (String). `OrderStatus` has `perm_id` (i32).
These don't match. The real link: commission reports arrive AFTER the fill status
for the same order. In practice, order_id from the fill + commission from the
next CommissionReport for that order_id are a pair.

ibapi's stream yields `OrderUpdate::CommissionReport(report)` where `report.exec_id`
references the execution.

## Solution

Add `exec_id: Option<String>` to `OrderStatusEvent::Filled`. When processing
CommissionReport, extract the exec_id and emit it in the Filled event.

### Before (broken)
```rust
Filled {
    order_id: i32,
    filled_qty: f64,
    avg_price: f64,
    commission: Option<f64>,
}
```

### After (fixed)
```rust
Filled {
    order_id: i32,
    filled_qty: f64,
    avg_price: f64,
    commission: Option<f64>,
    exec_id: Option<String>,  // NEW — from CommissionReport.exec_id
}
```

## Implementation Plan

### Task 1: Add exec_id field to OrderStatusEvent::Filled
**Files:** `src/orders.rs`
**Description:** Add `exec_id: Option<String>` field to the Filled variant. Update
all Filled constructors to include `exec_id: None` (backward compat).

### Task 2: Extract exec_id from CommissionReport
**Files:** `src/orders.rs`
**Description:** In `OrderStatusStream::next()`, when a CommissionReport arrives,
extract the exec_id and include it in the Filled event. Map:
```
CommissionReport { exec_id, commission, .. } 
→ Filled { exec_id: Some(exec_id), commission: Some(commission), .. }
```

### Task 3: Emit proper fill+commission pair
**Files:** `src/orders.rs`
**Description:** When both fill and commission arrive for the same order, the
consumer can now match by exec_id. Document the pattern:
1. First Filled event: `order_id=42, exec_id=None, commission=None` (the fill)
2. Second Filled event: `order_id=0, exec_id=Some("abc123"), commission=Some(1.30)` (the commission)
3. Consumer matches by tracking order_id → then matches commission by exec_id

### Task 4: Tests
**Files:** `tests/` or inline `#[cfg(test)]`
**Description:** 
- Test that Filled constructors include exec_id
- Test CommissionReport → Filled mapping with exec_id
- Test backward compat: old Filled events still parse correctly

## Impact
- No breaking changes — adding optional field
- No Python bindings change needed (deferred order tracking)
- No Huat changes needed (Huat doesn't use commission tracking yet)
