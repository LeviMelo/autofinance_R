# panel_adj.md

## Purpose
`panel_adj` is the adjusted daily panel produced by Module 04 (Adjuster).
It contains raw OHLC, split-adjusted OHLC, and final adjusted OHLC (Split + Dividend).
It is the only input accepted by the Screener (Module 05).

## Table contract (schema)

### Keys
- **Primary key:** `(symbol, refdate)` must be **unique**.

### Required columns (minimum)
| column           | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| asset_type       | character | yes      | `equity` / `fii` / `etf` / `bdr` |
| close_adj_final  | numeric   | yes      | final adjusted close (splits + dividends) |
| adjustment_state | character | yes      | see below |
| turnover         | numeric   | yes* | liquidity volume in BRL (or `vol_fin`) |
| qty              | numeric   | yes      | liquidity quantity proxy |

\* Either `turnover` OR `vol_fin` must exist. Downstream modules normalize to `turnover`.

### Standard Columns (V2 Production)
Raw aliases:
- `open_raw, high_raw, low_raw, close_raw`

Split-adjusted (intermediate):
- `open_adj_split, high_adj_split, low_adj_split, close_adj_split`

Final adjusted (split + dividend):
- `open_adj_final, high_adj_final, low_adj_final, close_adj_final`

### adjustment_state semantics
- `ok` : no split/dividend events in-range
- `dividend_only` : dividend events applied, no splits
- `split_only` : split events applied, no dividends
- `split_dividend` : both applied
- `manual_override` : at least one manual event present for the symbol
- `suspect_unresolved` : 
    1. Dividend adjustment had computation issue (`issue_div == TRUE`)
    2. **Residual Jump Safety Net:** `close_adj_final` still contains a 1-day log-return > threshold (defaults to 1.0)

### Invariants / Guarantees
- `(symbol, refdate)` uniqueness guaranteed.
- If `adjustment_state == suspect_unresolved`, downstream consumers (Screener) **MUST** exclude the symbol by default.

## Relationship to other outputs
Module 04 also returns:
- `residual_jump_audit`: Table flagging symbols that failed the safety net check.