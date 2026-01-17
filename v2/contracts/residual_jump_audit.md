# residual_jump_audit.md (Module 04 output)

## Purpose
`residual_jump_audit` is the post-adjustment safety net report.
It summarizes, per symbol, the largest remaining 1-day discontinuity in `close_adj_final`.

If the largest abs 1-day log-return exceeds the tolerance, the symbol becomes `suspect_unresolved`.

## Table contract (schema)

### Primary key
- `(symbol)` unique.

### Required columns
| column                 | type    | required | semantics |
|------------------------|---------|----------|----------|
| symbol                 | chr     | yes      | uppercase ticker |
| residual_max_abs_logret| dbl     | yes      | max over t of `abs(diff(log(close_adj_final)))` |
| residual_jump_date     | Date    | yes      | refdate corresponding to the max jump (t where diff is realized) |
| residual_jump_flag     | logical | yes      | TRUE if `residual_max_abs_logret >= cfg$adj_residual_jump_tol_log` |

## Notes
- The measure uses `close_adj_final > 0` and finite values only.
- If the symbol has insufficient valid prices, the audit may report:
  - `residual_max_abs_logret = 0` and `residual_jump_date = NA`.

## Consumer rules
- This is a diagnostic/audit table; the enforcement happens via `panel_adj.adjustment_state`.
- When investigating suspicious symbols, inspect:
  1) `events` and `adjustments` first,
  2) then `residual_jump_audit` to confirm the safety-net trigger.
