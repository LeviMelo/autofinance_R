# screener_input.md

## Purpose
Defines what Module 05 (Screener / Feature builder) accepts.

## Input: panel_adj (from Module 04)

### Keys
- `(symbol, refdate)` must be unique.

### Required columns
- `symbol` (character)
- `refdate` (Date)
- `asset_type` (character)
- `close_adj_final` (numeric)
- `adjustment_state` (character)
- liquidity: either `turnover` or `vol_fin` must exist

### Recommended columns (for OHLC-based features)
- `open_adj_final`, `high_adj_final`, `low_adj_final`, `close_adj_final`

## Policy: unresolved suspects
If any rows have `adjustment_state == "suspect_unresolved"` then:
- default behavior: **stop**
- override: allow only if caller explicitly sets `allow_unresolved = TRUE`
