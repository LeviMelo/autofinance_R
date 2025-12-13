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
- `open_adj_final`, `high_adj_final`, `low_adj_final`

## Policy: Unresolved Suspects (The Safety Valve)
If any rows have `adjustment_state == "suspect_unresolved"` (due to dividend errors or residual jumps):
- **Default behavior:** The Screener **STOP** or **DROP** these symbols immediately.
- **Override:** Allow only if caller explicitly sets `allow_unresolved = TRUE`.

This protects the ranking engine from being poisoned by broken data (e.g. +1900% fake returns).