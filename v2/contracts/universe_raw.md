# universe_raw.md (Module 01 output)

## Purpose
`universe_raw` is the canonical raw market panel (B3 COTAHIST via rb3), normalized to a strict minimum schema.
It is the **only** raw price/liquidity input to the Adjuster (Module 04).

## Table contract (schema)

### Primary key
- `(symbol, refdate)` must be unique **after Module 01 finishes**.

### Required columns
| column     | type      | required | semantics |
|------------|-----------|----------|----------|
| symbol     | character | yes      | uppercase ticker |
| refdate    | Date      | yes      | trading date |
| open       | numeric   | yes*     | raw open |
| high       | numeric   | yes*     | raw high |
| low        | numeric   | yes*     | raw low |
| close      | numeric   | yes      | raw close (must be finite for usable rows) |
| turnover   | numeric   | yes      | liquidity financial volume in BRL (preferred) |
| qty        | numeric   | yes      | liquidity quantity proxy (units/contracts proxy) |
| asset_type | character | yes      | one of: `equity`, `fii`, `etf`, `bdr` |

\* open/high/low may be NA for some upstream rows, but columns must exist.

### Optional / non-contract columns
Module 01 may temporarily handle:
- `vol_fin`, `qty_raw` (internal), but output is standardized to `turnover` and `qty`.

## Normalizations performed by Module 01 (guaranteed)
- `symbol` trimmed, uppercase.
- `refdate` coerced to Date.
- Liquidity normalized:
  - if upstream has financial volume, it becomes `turnover`;
  - else `turnover = qty * close` when possible.
- Dedupe policy (when upstream emits duplicates):
  - Keep row with max `turnover`, then max `qty`, then max `close`, then deterministic first row.

## Invariants / Guarantees
- No duplicated `(symbol, refdate)`.
- `asset_type` is lowercased and validated against allowed types.
- Rows with missing/empty symbol, missing refdate, or non-finite close are dropped.

## Consumer rules
- Module 04 may assume this schema strictly.
- Downstream modules must not reinterpret split orientation from raw (that belongs to Module 04’s split-gap logic).

## Minimal example
One row:
- `symbol="VALE3"`, `refdate=2025-12-12`, `open=..., high=..., low=..., close=..., turnover=..., qty=..., asset_type="equity"`.
