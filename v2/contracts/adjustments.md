# adjustments.md

## Purpose
`adjustments` is the per-symbol daily adjustment timeline produced by Module 04.
It serves as the audit trail for *why* `panel_adj` differs from `universe_raw`.

## Table contract (schema)

### Keys
- `(symbol, refdate)` unique within this table.

### Required columns
| column           | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| split_value      | numeric   | yes      | same-day split price-factor (1 means none). Multiple splits on same day are multiplied. |
| div_cash         | numeric   | yes      | same-day cash dividend amount (0 means none). Multiple dividends summed. |
| split_factor_cum | numeric   | yes      | cumulative split factor applied to *this date* (exclusive: events with refdate > date) |
| div_factor_event | numeric   | yes      | per-day dividend factor (normally 1; <1 on dividend date when computable) |
| div_factor_cum   | numeric   | yes      | cumulative dividend factor applied to *this date* (exclusive) |
| adj_factor_final | numeric   | yes      | `split_factor_cum * div_factor_cum` |
| source_mask      | character | yes      | provenance summary (e.g., `yahoo`, `manual`, `chart`, `yahoo+manual`) |
| has_manual       | logical   | yes      | TRUE if any manual event contributed that day |
| issue_div        | logical   | yes      | TRUE when dividend factor could not be computed safely (e.g. Div >= Price) |

## Dividend issue policy (V2 behavior)
On a day where `div_cash > 0`:
- `div_factor_event = (close_prev - div_cash) / close_prev`
- If `close_prev` is missing/non-finite, `close_prev <= 0`, or `div_cash >= close_prev`:
  - `issue_div = TRUE`
  - `div_factor_event = 1` (adjustment skipped for safety)

This conservative policy prevents negative prices and exploding factors.

## Guarantees
- `split_value > 0`
- `div_cash >= 0`
- `split_factor_cum > 0`
- `div_factor_cum >= 0`
- `adj_factor_final >= 0`