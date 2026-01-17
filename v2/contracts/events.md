# events.md (Module 04 intermediate output)

## Purpose
`events` is the **normalized per-symbol per-day event table** consumed by the adjuster’s factor engine.
It is the aggregation of:
- vendor registry (`corp_actions_registry`), plus
- `manual_events` (optional).

## Table contract (schema)

### Primary key
- `(symbol, refdate)` must be unique within this table.

### Required columns
| column      | type      | required | semantics |
|-------------|-----------|----------|----------|
| symbol      | character | yes      | uppercase ticker |
| refdate     | Date      | yes      | trading date used by the adjuster join |
| split_value | numeric   | yes      | same-day split price-factor; 1 means none |
| div_cash    | numeric   | yes      | same-day cash dividend amount; 0 means none |
| source_mask | character | yes      | provenance summary (e.g., `yahoo`, `manual`, `yahoo+manual`) |
| has_manual  | logical   | yes      | TRUE if any manual event contributed that day |

## Aggregation rules (v2 behavior)
After unioning corp actions + manual:
1) exact duplicates removed on:
   `(symbol, refdate, action_type, value, source)`
2) same-day aggregation:
   - splits: `split_value = prod(value)` (multiplicative)
   - dividends: `div_cash = sum(value)` (additive)
3) `source_mask` is the `+`-joined unique sources seen that day.
4) `has_manual = grepl("manual", source_mask)`.

## Invariants / Guarantees
- `split_value > 0`
- `div_cash >= 0`
- empty result is returned as a correctly-typed empty table.

## Notes on split orientation
`events.split_value` is assumed to be a **price factor** by the adjuster.
If vendor split orientation is ambiguous, it must be resolved *before* the event table is built (selective pipeline fixes Yahoo splits).
