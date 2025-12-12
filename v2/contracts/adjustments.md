# adjustments.md

## Purpose
`adjustments` is the per-symbol daily adjustment timeline produced by Module 04.
It is an audit table: it explains *why* `panel_adj` differs from `universe_raw`.

## Table contract (schema)

### Keys
- `(symbol, refdate)` unique within this table.

### Required columns
| column            | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| split_value      | numeric   | yes      | same-day split price-factor (1 means none). Multiple splits on same day are multiplied. |
| div_cash         | numeric   | yes      | same-day cash dividend amount (0 means none). Multiple dividends summed. |
| split_factor_cum | numeric   | yes      | cumulative split factor applied to *this date* (exclusive: events with refdate > date) |
| div_factor_event | numeric   | yes      | per-day dividend factor (normally 1; <1 on dividend date when computable) |
| div_factor_cum   | numeric   | yes      | cumulative dividend factor applied to *this date* (exclusive) |
| adj_factor_final | numeric   | yes      | `split_factor_cum * div_factor_cum` |
| source_mask      | character | yes      | provenance summary (e.g., `yahoo`, `manual`, `yahoo+manual`) |
| has_manual       | logical   | yes      | TRUE if any manual event contributed that day |
| issue_div        | logical   | yes      | TRUE when dividend factor could not be computed safely |

## Dividend issue policy (current v2 behavior)
On a day where `div_cash > 0`, dividend factor uses:
- `close_prev = lag(close_adj_split)`  
- If `close_prev` is missing/non-finite, `close_prev <= 0`, or `div_cash >= close_prev`,
  then the system sets:
  - `issue_div = TRUE`
  - `div_factor_event = 1` (no dividend adjustment applied for that event)

This is intentionally conservative: it avoids exploding the factor chain.

## Guarantees
- `split_value > 0`
- `div_cash >= 0`
- `split_factor_cum > 0`
- `div_factor_cum >= 0`
- `adj_factor_final >= 0`
