# adjustments.md (Module 04 output)

## Purpose
`adjustments` is the per-symbol daily adjustment timeline produced by Module 04.
It is the audit trail for *why* `panel_adj` differs from `universe_raw`.

This table is the "explainability spine":
- what happened on each day (split/div),
- what factor got applied cumulatively to earlier dates,
- whether dividend logic had to be skipped for safety,
- whether dividend values were auto-rescaled due to basis mismatch.

## Table contract (schema)

### Primary key
- `(symbol, refdate)` must be unique.

### Required columns
| column            | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| split_value      | numeric   | yes      | same-day split price-factor (1 means none). Multiple splits same day multiplied. |
| div_cash         | numeric   | yes      | same-day cash dividend amount (0 means none). Multiple dividends same day summed. |
| div_cash_eff     | numeric   | yes      | dividend cash amount actually used in formula after any rescue scaling |
| div_scaled       | logical   | yes      | TRUE if `div_cash_eff` was derived by scaling `div_cash` |
| split_factor_cum | numeric   | yes      | cumulative split factor applied to *this date* (exclusive) |
| div_factor_event | numeric   | yes      | per-day dividend factor; normally 1; computed on dividend day when safe |
| div_factor_cum   | numeric   | yes      | cumulative dividend factor applied to *this date* (exclusive) |
| adj_factor_final | numeric   | yes      | `split_factor_cum * div_factor_cum` |
| source_mask      | character | yes      | provenance summary (e.g., `yahoo`, `manual`, `yahoo+manual`, `none`) |
| has_manual       | logical   | yes      | TRUE if any manual event contributed that day |
| issue_div        | logical   | yes      | TRUE if dividend factor was unsafe and therefore skipped |

## Exclusive cumulative logic (core definition)
For any event factor series `f(t)` (split or dividend):
- The factor applied to date `t` is the product of event factors strictly **after** `t`.
- Same-day events do **not** alter that day; they alter history before it.

Implementation note: the code uses a reverse cumulative product plus a lead shift.

## Dividend computation (v2 behavior)
Let:
- `close_adj_split(t-1)` be the prior trading day close after split adjustment,
- `D = div_cash_eff(t)`.

Then on dividend day `t`:
- if `close_prev` is finite and `0 < D < close_prev`:
  - `div_factor_event(t) = (close_prev - D) / close_prev`
- else:
  - `issue_div = TRUE`
  - `div_factor_event(t) = 1` (skip adjustment)

### Basis-mismatch rescue (important)
If vendor dividend appears impossible on the split-adjusted basis AND the symbol has a forward-split cumulative factor:
- when `div_cash >= close_prev` and `split_factor_cum < 1`,
- the code attempts:
  - `div_cash_eff = div_cash * split_factor_cum`
- if that makes `div_cash_eff < close_prev`, it is accepted and `div_scaled = TRUE`.

This is a pragmatic rescue for cases where vendor dividends are quoted on a different share basis.

## Invariants / Guarantees
- `split_value > 0`
- `div_cash >= 0`
- `split_factor_cum > 0`
- `div_factor_event >= 0` (in practice 1 or in (0,1))
- `div_factor_cum >= 0`
- `adj_factor_final >= 0`

## Consumer rules
- Debugging must start here, not in `panel_adj`.
- Any symbol with `any(issue_div == TRUE)` is marked `suspect_unresolved` at the panel level.
