# panel_adj.md

## Purpose
`panel_adj` is the adjusted daily panel produced by Module 04 (Adjuster).
It contains raw OHLC, split-adjusted OHLC, and split+dividend adjusted OHLC.
It is the only input accepted by the Screener (Module 05).

## Table contract (schema)

### Keys
- **Primary key:** `(symbol, refdate)` must be **unique**.

### Required columns (minimum)
| column            | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| asset_type       | character | yes      | `equity` / `fii` / `etf` / `bdr` |
| close_adj_final  | numeric   | yes      | final adjusted close (splits + dividends) |
| adjustment_state | character | yes      | see below |
| turnover         | numeric   | yes*     | liquidity volume in BRL (or `vol_fin` alternative) |
| qty              | numeric   | yes      | liquidity quantity proxy |

\* Either `turnover` OR `vol_fin` must exist. Downstream modules may normalize `vol_fin -> turnover`.

### Recommended columns (what v2 currently produces)
Raw aliases:
- `open_raw, high_raw, low_raw, close_raw`

Split-adjusted:
- `open_adj_split, high_adj_split, low_adj_split, close_adj_split`

Final adjusted (split + dividend):
- `open_adj_final, high_adj_final, low_adj_final, close_adj_final`

### adjustment_state semantics (exact values)
- `ok` : no split/dividend events in-range
- `dividend_only` : dividend events applied, no splits
- `split_only` : split events applied, no dividends
- `split_dividend` : both applied
- `manual_override` : at least one manual event present for the symbol (anywhere in-range)
- `suspect_unresolved` : dividend adjustment had an unresolved computation issue (`issue_div_any == TRUE`)

### Invariants / guarantees
- `(symbol, refdate)` uniqueness guaranteed.
- Adjustment factors are applied deterministically based on the `events` table.
- If `adjustment_state == suspect_unresolved`, downstream consumers must either:
  - stop (default policy), or
  - explicitly allow unresolved symbols.

## Relationship to other outputs
Module 04 also outputs:
- `events` (per symbol-date split_value/div_cash)
- `adjustments` (timeline of factors per date)
