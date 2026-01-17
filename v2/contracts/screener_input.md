# screener_input.md (Module 05 input)

## Purpose
Defines what Module 05 (Screener / Feature builder) accepts.
The Screener is not allowed to fetch or adjust; it consumes `panel_adj` only.

## Input: panel_adj (from Module 04)

### Required columns
- `symbol` (character)
- `refdate` (Date)
- `asset_type` (character)
- `close_adj_final` (numeric)
- `adjustment_state` (character; see panel_adj contract)
- liquidity: `turnover` OR `vol_fin` must exist (v2 typically provides `turnover`)

### Recommended columns
For OHLC-based features:
- `open_adj_final`, `high_adj_final`, `low_adj_final`

## Policy: Unresolved Suspects (Safety Valve)
If any rows have `adjustment_state == "suspect_unresolved"`:
- default behavior: **STOP** (or drop symbols) unless caller overrides.
- override is explicit: `allow_unresolved = TRUE`.

Rationale: protects ranking from poisoned returns (e.g., fake +1900% moves).

## Consumer obligations
- Screener must treat `adjustment_state` as a hard gate, not advisory text.
- Screener must not infer corporate actions itself.
