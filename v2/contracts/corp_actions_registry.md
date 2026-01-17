# corp_actions_registry.md (Module 03 output)

## Purpose
`corp_actions_registry` is the raw vendor corporate actions table (Yahoo, via chart endpoint or quantmod fallback).
It is an **input** to Module 04, but **not** directly applied until normalized by `events.md`.

## Table contract (schema)

### Row identity
- There is no strict primary key because vendors may emit duplicates.
- Rows are later de-duplicated by Module 04.

### Required columns
| column       | type      | required | semantics |
|--------------|-----------|----------|----------|
| symbol       | character | yes      | uppercase internal ticker |
| yahoo_symbol | character | yes*     | vendor ticker (usually `SYMBOL.SA`) |
| refdate      | Date      | yes      | vendor event date (may be off vs B3 trading day) |
| action_type  | character | yes      | `split` or `dividend` |
| value        | numeric   | yes      | split: *vendor value* (could be ratio or price-factor), dividend: cash amount |
| source       | character | yes      | currently `yahoo` (or `manual` elsewhere, but manual events are separate) |

\* in some internal pathways `yahoo_symbol` may be absent; Module 04 tolerates that and drops it.

### Value semantics
- Dividends:
  - `value` is cash dividend per share in BRL terms as provided by Yahoo.
- Splits:
  - `value` can be either:
    - **price factor**, or
    - **ratio** (orientation ambiguous across feeds).
  - Orientation is later resolved by split-gap validation (selective pipeline).

## Known vendor failure modes (documented behavior)
- Event dates may land on weekends/holidays (non-trading days).
- Splits may be mis-dated by a few days relative to B3 effective trading date.
- Vendors may emit duplicates.

## Consumer rules
- Module 04 must never apply these rows “as-is” without normalization.
- In selective mode, Yahoo splits undergo split-gap reconciliation (`split_audit.md`).

## Output shape guarantee
Even if no actions exist, Module 03 returns an empty table with the same columns.
