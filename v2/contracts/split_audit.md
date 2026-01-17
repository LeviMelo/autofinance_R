# split_audit.md (Selective pipeline output)

## Purpose
`split_audit` records how Yahoo split rows were reconciled against the raw B3 series.
It exists to:
- snap vendor split dates onto real trading dates (within a configured window),
- resolve split orientation (ratio vs price-factor) by choosing the orientation that matches the observed raw jump,
- quarantine splits that cannot be verified.

## Table contract (schema)

### Row identity
- `row_id` is an internal identifier used to track original vendor rows during reconciliation.
- No strict primary key; this is an audit log.

### Required columns
| column        | type    | required | semantics |
|--------------|---------|----------|----------|
| row_id       | int     | yes      | internal id |
| symbol       | chr     | yes      | uppercase ticker |
| yahoo_symbol | chr     | yes      | vendor ticker |
| vendor_refdate| Date   | yes      | the raw vendor date |
| eff_refdate  | Date    | yes      | chosen effective trading date (can be NA) |
| lag_days     | int     | yes      | eff_refdate - vendor_refdate (can be NA) |
| yahoo_value  | dbl     | yes      | raw vendor split value |
| chosen_value | dbl     | yes      | selected price-factor orientation/value (may be NA) |
| chosen_err   | dbl     | yes      | matching error in log space (abs(log(obs) - log(chosen_value))) |
| status       | chr     | yes      | one of: `kept`, `rejected`, `dup`, `unverified` |

## Reconciliation algorithm (as frozen)
For each vendor split row:
1) Consider candidate dates:
   `vendor_refdate + offset`, where offset ∈ [-max_back_days, ..., +max_fwd_days].
2) On each candidate date, compute observed jump `obs` using:
   - `open / close_lag` if `split_gap_use_open = TRUE` and open is valid,
   - else `close / close_lag`.
3) Compare `obs` against two candidate orientations:
   - `v` and `1/v` (vendor value and its inverse).
4) Choose (date, orientation) minimizing:
   `err = abs(log(obs) - log(candidate_value))`.
5) If best err <= `split_gap_tol_log` ⇒ status `kept`, else `rejected`.
6) Deduplicate kept rows collapsing to same `(symbol, eff_refdate, chosen_value)`:
   - best err kept as `kept`, others marked `dup`.
7) If no candidate date had valid `close_lag` and open/close ⇒ status `unverified`.

## Policy in selective build (important)
In `af2_build_panel_adj_selective`:
- Splits with `status in {kept, unverified}` may be applied (fail-soft),
- rejected/dup rows go to `corp_actions_quarantine`.

## Consumer rules
- This is audit-only; the “applied” table is `corp_actions_apply`.
- Any persistent mismatch should still trigger `suspect_unresolved` via residual jump safety net.
