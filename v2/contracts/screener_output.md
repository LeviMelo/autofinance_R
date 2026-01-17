# screener_output.md (Module 05 output)

## Purpose
Defines the per-symbol **feature vector** produced by Module 05.
This output is intended to be consumed by ranking/scoring logic.

## Output object
`features` (data.table), one row per symbol.

## Required identifier/meta columns (as currently emitted)
| column            | type | meaning |
|------------------|------|---------|
| symbol            | chr  | ticker |
| asset_type        | chr  | equity/fii/etf/bdr (attached by screener loop) |
| end_date          | Date | alias of end_refdate (kept for convenience) |
| end_refdate       | Date | last date used for features |
| n_obs             | int  | number of rows used in the feature window |
| n_valid           | int  | number of finite `close_adj_final` rows in window |
| coverage          | dbl  | n_valid / n_obs (redundant with days_traded_ratio, but currently emitted) |
| days_traded_ratio | dbl  | fraction of non-NA `close_adj_final` in window |
| median_turnover   | dbl  | median turnover over the window |
| amihud            | dbl  | mean(|ret_simple| / turnover) over valid days |

## Core price/return features
For each horizon `h` in config (default: 21, 63, 126, 252):
- `ret_{h}d` : simple horizon return (`p_end / p_start - 1`)
- `vol_cc_{h}d` : annualized close-to-close volatility (`sd(log_ret) * sqrt(252)`)
- `vol_{h}d` : alias of `vol_cc_{h}d`

## OHLC-based risk features (emitted when OHLC adjusted exists)
For each horizon `h`:
- `vol_pk_{h}d` : Parkinson vol (HL) annualized
- `vol_gk_{h}d` : Garman–Klass vol (OHLC) annualized
- `range_mean_{h}d`, `range_max_{h}d`
- `gap_min_{h}d`, `gap_max_{h}d`, `gap_abs_med_{h}d`
- `tr_mean_{h}d` : true-range percent mean (ATR-like)
- `body_mean_{h}d` : mean candle body percent

## Path dependence
- `max_dd` : maximum drawdown over valid price path
- `ulcer_index` : RMS drawdown metric

## NA policy
- Features may be NA if insufficient data exists for a symbol/horizon.
- Column set must remain stable for a given screener config.

## Output stability note
The presence of both `coverage` and `days_traded_ratio` is currently redundant but contractual until removed in a versioned breaking change.
