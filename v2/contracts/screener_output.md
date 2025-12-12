# screener_output.md

## Purpose
Module 05 outputs a **feature vector** per symbol.
Ranking/scoring is optional and should be treated as a separate concern.

## Output: features (primary)
A table with **one row per symbol**.

### Keys
- `symbol` unique.

### Required metadata columns
| column      | type      | semantics |
|------------|-----------|----------|
| symbol     | character | uppercase ticker |
| asset_type | character | `equity` / `fii` / `etf` / `bdr` |
| end_date   | Date      | last date used for feature computation |
| n_obs      | integer   | number of rows used (post-slicing) |
| n_valid    | integer   | number of finite close observations |
| coverage   | numeric   | `n_valid / n_obs` |

### Baseline feature families (v2 minimum)
- Multi-horizon returns from `close_adj_final`:
  - `ret_21d`, `ret_63d`, `ret_126d`, `ret_252d` (configurable horizons)
- Close-to-close realized volatility (annualized):
  - `vol_cc_21d`, `vol_cc_63d`, ... (same horizons)
- OHLC-based realized volatility (annualized, NA-safe):
  - Parkinson range vol: `vol_pk_21d`, ...
  - Garman–Klass vol: `vol_gk_21d`, ...
- Drawdown/stress:
  - `max_dd`, `ulcer_index`
- Liquidity/microstructure proxies:
  - `median_turnover`, `days_traded_ratio`, `amihud`

### NA rules
- If insufficient data for a horizon window, that feature is `NA_real_`.
- Features must be present as columns even when NA (stable schema).

## Optional output: ranking (secondary)
If a scoring model is applied, Module 05 may also output:
- `full` table: features + score + ranks
- `by_type` list: same, split by `asset_type`

But **features remain the primary contract**.
