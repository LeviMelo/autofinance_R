# screener_output.md

## Purpose
Defines the **feature vector** output produced by Module 05.
This output is intended to be consumed by **separate ranking/scoring logic** (or Alpha Model).

## Output object
`features` (data.table), **one row per symbol**.

## Required identifier/meta columns
| column            | type | meaning |
|-------------------|------|---------|
| symbol            | chr  | ticker |
| asset_type        | chr  | equity/fii/etf/bdr |
| end_refdate       | Date | last date used for features |
| n_obs             | int  | number of rows used in the feature window |
| days_traded_ratio | dbl  | fraction of non-NA `close_adj_final` in window |

## Core price/return features (V2 Standard)
For each horizon `h` in config (e.g., 21, 63, 126, 252):
- `ret_{h}d` : Geometric return over horizon (`p_end / p_start - 1`)
- `vol_{h}d` : Annualized close-to-close volatility (`sd(log_ret) * sqrt(252)`)

## OHLC-based risk features (Optional/Recommended)
For each horizon `h` (when OHLC columns exist):
- `vol_pk_{h}d` : Parkinson volatility (High/Low)
- `vol_gk_{h}d` : Garman-Klass volatility (OHLC)
- `range_mean_{h}d` : Mean daily range fraction
- `gap_vol_{h}d` : Volatility of overnight gaps

## Path-dependence / drawdown features
- `max_dd` : Maximum drawdown in the lookback window
- `ulcer_index` : RMS drawdown metric

## Liquidity features
- `median_turnover` : Median daily financial volume
- `amihud` : Illiquidity proxy (`mean(|ret| / volume)`)

## NA policy
- Features may be `NA` if insufficient data exists for a symbol/horizon.
- The table shape (column set) must remain stable across runs for the same config.