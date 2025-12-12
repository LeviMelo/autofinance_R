*** Begin Patch
*** Update File: contracts/screener_output.md
@@
-# screener_output.md
-
-This is a placeholder. Fill with the explicit schema + guarantees.
+# screener_output.md
+
+## Purpose
+Defines the **feature vector** output produced by Module 05.
+This output is intended to be consumed by **separate ranking/scoring logic** later.
+
+## Output object
+`features` (data.table/data.frame), **one row per symbol**.
+
+## Required identifier/meta columns
+| column      | type | meaning |
+|------------|------|---------|
+| symbol     | chr  | ticker |
+| asset_type | chr  | equity/fii/etf/bdr |
+
+## Recommended coverage/meta columns
+| column        | type | meaning |
+|--------------|------|---------|
+| end_refdate  | Date | last date used for features |
+| n_obs        | int  | number of rows used in the feature window |
+| days_traded_ratio | dbl | fraction of non-NA `close_adj_final` in window |
+
+## Core price/return features (examples)
+For each horizon `h` in config (e.g., 21/63/126/252):
+- `ret_{h}d` (simple price return over horizon)
+- `vol_{h}d` (annualized close-to-close volatility over last h obs)
+
+## OHLC-based risk features (recommended)
For each horizon `h` (when OHLC columns exist):
+- `parkinson_vol_{h}d` (annualized high/low-based volatility estimator)
+- `range_mean_{h}d` (mean daily range fraction: (high-low)/close)
+- `gap_vol_{h}d` (annualized volatility of open-to-prev-close returns)
+
+## Path-dependence / drawdown features
- `max_dd`
- `ulcer_index`
+
+## Liquidity features
- `amihud` (|ret| / turnover proxy)
+
+## NA policy
- Features may be NA if insufficient data exists for a symbol/horizon.
+- The table shape (column set) must remain stable across runs for the same config.
+
*** End Patch
