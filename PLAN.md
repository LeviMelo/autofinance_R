Here’s a concrete, implementable architecture you can drop into `PLAN.md` and build against.

---

# Autofinance V2 – Architectural Plan

## 0. Scope and Non-Negotiables

**Goal:**
Local, R-based “research station” to:

* Ingest and persist **B3 tape** (COTAHIST via `rb3`).
* Ingest and persist **corporate actions** (splits, and optionally dividends) via `quantmod::getSplits` / `getDividends`.
* Ingest and persist **macro factors** (CDI, Selic, IPCA, FX, etc.) from **BCB SGS**.
* Build **split-adjusted price panels** for B3 assets.
* Compute **research-grade metrics** (returns at multiple horizons, vol, skew, kurtosis, VaR, CVaR, drawdowns, betas to multiple factors).
* Run a **screener** (purely gross performance/risk, per-asset-class).
* Run **offline backtests** using only the local DB snapshot.
* Leave tax/fiscal rules and trading rules to the **portfolio/backtest layer**, not the screener.

**Non-negotiable constraints:**

1. **No always-on server, no cron jobs.** Everything is triggered by R functions you explicitly call.
2. **COTAHIST (via `rb3`) is the ground truth** for B3 OHLCV. Yahoo is never a primary price source, only a source of corporate actions (and optionally total-return stuff later).
3. **Splits are never inferred heuristically** from price jumps. Only `getSplits()` results are used. If we have no splits, we either:

   * Treat the series as unsplit, or
   * Flag it as “unverified” and optionally drop from serious analysis.
4. **`quantmod::getSplits()` is acceptable for the entire B3 universe**, with caching to SQLite. The real network constraint is bulk OHLC/dividend history from Yahoo, not splits.
5. **Backtests are 100% offline.** All required history must be in SQLite before the backtest starts.
6. **Screener works on gross world.** No tax logic, no fee modeling. Asset-class separation is allowed (Equity / FII / BDR / ETF buckets), but net-of-tax modeling happens later.

---

## 1. High-Level System Overview

Think of Autofinance V2 as four layers:

1. **Data Layer (SQLite + ingestion)**

   * `rb3` → COTAHIST → `prices_raw` table.
   * `quantmod` → splits/dividends → `corporate_actions` table.
   * BCB SGS → macro series → `macro_series` table.
   * Metadata for tickers (type, active/delisted, last update timestamps) → `assets_meta`.

2. **Adjustment + Panel Layer**

   * Merge **raw OHLCV** with **splits** to produce **split-adjusted OHLC** in memory for a given set of tickers and date range.

3. **Metrics & Screener Layer**

   * Given adjusted OHLC and macro series, compute a dense feature vector per asset: multi-horizon returns, volatilities, drawdown metrics, tail risk, betas to IBOV, SPX, USD, DI, etc.
   * Apply **liquidity filters** and **class-wise rankings** (e.g., Top K equities, Top K FIIs, Top K BDRs).

4. **Backtesting & Portfolio Layer**

   * Take screener outputs + DB snapshot.
   * Run offline simulations, with optional tax rules, costs, rebalancing rules, and constraints.
   * All data access via SQLite; no HTTP inside the simulation loop.

---

## 2. Core Principles and Design Invariants

1. **Single Source of Truth (per domain):**

   * Prices/volume: **B3** via `rb3`.
   * Corporate actions: **Yahoo** via `quantmod` (`getSplits`, maybe `getDividends`).
   * Macro factors: **BCB SGS**.

2. **SQLite as persistent state:**

   * All raw data and events live in a single file `autofinance.sqlite`.
   * No CSVs as canonical storage.

3. **On-demand synchronization:**

   * You call explicit sync functions:

     * `af_sync_b3()` for COTAHIST.
     * `af_sync_splits()` for splits.
     * `af_sync_macro()` for macro.
     * `af_sync_all()` as a convenience wrapper.
   * No background tasks.

4. **Adjustments computed locally:**

   * Use `TTR::adjRatios` (or equivalent) + splits table to adjust **all OHLC** (not only close).
   * Adjusted prices are computed on the fly when needed; they don’t need to be stored as a separate table initially (can be added later for speed).

5. **Dynamic horizons and lookback:**

   * Screener takes a **configurable lookback window**.
   * Only horizons compatible with available history are computed (e.g., no 3-year metrics if lookback is 1 year).

6. **Asset-class-aware ranking:**

   * The screener outputs **per-class rankings** (equities vs FIIs vs BDRs vs ETFs) instead of a single global “Top N”.

---

## 3. Data Sources and Contracts

### 3.1. B3 COTAHIST via `rb3`

* **Responsibility:** Provide daily OHLCV for all B3 tickers, including delisted ones.
* **Usage:**

  * Initial bootstrap: download all past years of interest.
  * On update: download only the missing year(s) and upsert rows.

### 3.2. Yahoo via `quantmod`

* **Splits (mandatory):**

  * `getSplits("PETR4.SA")`, etc.
  * Full-history for each ticker at first fetch, then incremental (from last date + 1).

* **Dividends (optional, later):**

  * `getDividends()`.
  * Only required if you want total-return metrics (DRIP, etc.) or realistic cash flow modeling.

### 3.3. BCB SGS Macro Series

Typical series:

* **Risk-free:** CDI, Selic.
* **Inflation:** IPCA, maybe IGP-M.
* **FX:** USD/BRL PTAX.
* **Activity:** IBC-Br.

Usage:

* For **betas/correlations** (asset returns vs macro factor returns).
* For regime-aware strategies in the backtester.

---

## 4. Data Model – SQLite Schema

This is the backbone. Explicit table definitions:

### 4.1. `assets_meta`

Tracks the universe and state per symbol.

**Columns:**

* `symbol` (TEXT, PK) — e.g., `"PETR4"`, `"KNCR11"`, `"IVVB11"`.
* `asset_type` (TEXT) — `"EQUITY"`, `"FII"`, `"BDR"`, `"ETF"`, etc.
* `sector` (TEXT, nullable).
* `active` (INTEGER) — `1` for currently trading, `0` if delisted.
* `last_update_prices` (TEXT, ISO date) — last date we ingested raw COTAHIST for this symbol (optional; COTAHIST is mostly year-based, so this can be global).
* `last_update_splits` (TEXT, ISO date) — last time we checked `getSplits`.
* `last_update_divs` (TEXT, ISO date) — last time we checked dividends (if used).

Indexes:

* PK (`symbol`) is enough for this table.

### 4.2. `prices_raw`

Immutable tape from B3.

**Columns:**

* `symbol` (TEXT).
* `refdate` (TEXT, `YYYY-MM-DD`).
* `open` (REAL).
* `high` (REAL).
* `low` (REAL).
* `close` (REAL).
* `vol_fin` (REAL) — financial volume.
* `qty` (INTEGER) — volume in shares/quotes.

**Constraints:**

* `PRIMARY KEY (symbol, refdate)`
  `WITHOUT ROWID` for efficiency.

Indexes:

* Composite PK already covers `(symbol, refdate)` queries.
* Optional index on `refdate` alone if you often query “all symbols on a given day”.

### 4.3. `corporate_actions`

Splits and (later) dividends.

**Columns:**

* `symbol` (TEXT).
* `date` (TEXT, `YYYY-MM-DD`) — ex-date.
* `type` (TEXT) — `"SPLIT"` or `"DIVIDEND"`.
* `value` (REAL) — split factor (e.g., `0.1` for 10→1) or dividend amount per share.

**Constraints:**

* `PRIMARY KEY (symbol, date, type)`.

Indexes:

* PK is enough.

### 4.4. `macro_series`

Macro indicators from BCB.

**Columns:**

* `series_id` (TEXT or INTEGER) — e.g. `"CDI"`, `"SELIC"`, `"IPCA"`, `"USD_P TAX"`, or numeric BCB ID.
* `refdate` (TEXT).
* `value` (REAL).

**Constraints:**

* `PRIMARY KEY (series_id, refdate)`.

### 4.5. Possible future tables (not mandatory now)

* `prices_adj_cache` — optional pre-materialized adjusted OHLC for speed.
* `metrics_cache` — optional per-symbol metric snapshots to avoid recomputing everything on each run (only if profiling shows it’s needed).

---

## 5. Codebase Layout (R Modules)

Organize the R code in modules, each with clear responsibilities.

### 5.1. `R/db_core.R`

**Purpose:** Low-level DB utilities.

Public functions:

* `af_db_connect(db_path = "data/autofinance.sqlite")`
  Opens connection, sets pragmas (`WAL`, cache size, etc.).
* `af_db_init(db_path)`
  Creates all tables if not present.

This module never knows about `rb3` or `quantmod`; it only does SQL.

### 5.2. `R/ingest_b3.R`

**Purpose:** Ingest COTAHIST via `rb3` into `prices_raw`.

Key functions:

* `af_sync_b3(years = NULL)`

  * If `years` is `NULL`, infer missing years from `prices_raw` max(refdate).
  * Uses `rb3::cotahist_get` to fetch full-year data.
  * Normalizes to `prices_raw` schema and upserts via `af_db_ingest_raw_b3()`.

* `af_db_ingest_raw_b3(dt_b3, con)`

  * Maps columns to schema and does `INSERT OR REPLACE` into `prices_raw`.

### 5.3. `R/ingest_splits.R`

**Purpose:** Fetch and store splits (and later dividends) via `quantmod`.

Key functions:

* `af_sync_splits(symbols = NULL, force = FALSE, max_age_days = 7)`

  * If `symbols` is `NULL`, select from `assets_meta` where `active = 1`.
  * For each symbol:

    * If `force = FALSE`, check `last_update_splits`. Skip if recent enough.
    * Call `quantmod::getSplits(paste0(symbol, ".SA"))`.
    * Convert to `data.table(symbol, date, value, type="SPLIT")`.
  * Upsert into `corporate_actions`.
  * Update `assets_meta.last_update_splits`.

### 5.4. `R/ingest_macro.R`

**Purpose:** Fetch macro series from BCB and store in `macro_series`.

Key functions:

* `af_sync_macro(series_ids, start_date, end_date)`

  * For each series_id, fetch missing data from BCB.
  * Upsert into `macro_series`.

### 5.5. `R/adjustment.R`

**Purpose:** Build adjusted OHLC panels from `prices_raw` + `corporate_actions`.

Key function:

* `af_build_adjusted_panel(symbols, start_date, end_date, con)`
  Steps:

  1. Query `prices_raw` for those symbols and date range.
  2. Query `corporate_actions` for splits only (`type = 'SPLIT'`).
  3. For each symbol:

     * Build an `xts` or `data.table` of OHLC.
     * Build a splits `xts`.
     * Compute adjustment ratios via `TTR::adjRatios` or equivalent.
     * Apply ratios to O/H/L/C.
  4. Return a single `data.table`:

     * `symbol, refdate, open_adj, high_adj, low_adj, close_adj, vol_fin, qty`.

No Yahoo price data here; only COTAHIST + splits.

### 5.6. `R/metrics.R`

**Purpose:** Given adjusted OHLC and macro series, compute metrics.

Main public function:

* `af_compute_metrics(panel_adj, macro_panel, config)`

Where:

* `panel_adj` is `data.table(symbol, refdate, open_adj, high_adj, low_adj, close_adj, vol_fin, qty)`.
* `macro_panel` is combined macro series and benchmark index returns.
* `config` defines:

  * `lookback_start`, `lookback_end`.
  * `horizons` (in days): e.g. `c(21, 63, 126, 252)`.
  * Which betas to compute (`ibov`, `usd`, `spx`, `di`).

Per symbol, it should compute (if history is long enough):

* **Returns:**

  * `ret_21d`, `ret_63d`, `ret_126d`, `ret_252d`, etc.
    (Skip any horizon > available days.)

* **Volatilities:**

  * `vol_21d`, `vol_252d` (annualized from daily sd).

* **Tail risk:**

  * `skew_252d`, `kurt_252d`.

* **VaR & CVaR (historical, 95%):**

  * Percentage loss threshold and conditional expectation below that.

* **Drawdowns:**

  * `max_dd_lookback`.
  * `ulcer_index` (sqrt of mean squared drawdowns).
  * `avg_time_underwater` (average length of consecutive drawdown periods).

* **Liquidity metrics:**

  * `median_vol_fin_63d` (or 252d).
  * `amihud_illiquidity` (average |return| / volume).

* **Betas/correlations:**

  * `beta_ibov`, `beta_usd`, `beta_spx`, `beta_di`:

    * Compute from daily return series via simple covariance/variance or regression.
  * Possibly `corr_ibov`, `corr_usd`, etc., for raw correlations.

Return shape: one row per symbol, one column per metric.

### 5.7. `R/screener.R`

**Purpose:** Glue: from DB → panel → metrics → ranked screener output.

Key function:

* `af_run_screener(config)`

Flow:

1. **Determine time window** from `config$lookback` (e.g. last 252 trading days).
2. **Query liquidity pre-filter** using `prices_raw` only (no adjustments yet):

   * Compute median `vol_fin` over lookback.
   * Keep only assets with `median_vol_fin >= config$min_liquidity`.
3. **Restrict universe** by `asset_type` (if desired).
4. **Build adjusted panel** for this filtered universe via `af_build_adjusted_panel()`.
5. **Load macro series** for that date range.
6. **Compute metrics** via `af_compute_metrics()`.
7. **Per asset type**, apply scoring/ranking:

   * Example: z-score of `ret_252d`, `vol_252d`, `ulcer_index`, `beta_ibov`, etc., with your weighting scheme.
8. Return a **list** or long data.frame:

   * `list(equity = df_equity_ranked, fii = df_fii_ranked, bdr = df_bdr_ranked, etf = df_etf_ranked)`.

Screener does **not** apply tax or trading rules.

### 5.8. `R/backtest.R`

**Purpose:** Run offline simulations based on DB.

Key pieces:

* `af_backtest_prepare(symbols, start_date, end_date, con)`

  * Uses `af_build_adjusted_panel()` to produce full adjusted history for all needed symbols and time range.
  * Optionally adds dividend cash flows if you support them.

* `af_backtest_run(strategy_fn, universe, start_date, end_date, config)`

  * `strategy_fn` is a user-supplied function that:

    * Given date `t` and current state, returns target weights or trades.
  * The engine:

    * Steps through time (e.g. daily or rebalance frequency).
    * Updates portfolio, applies costs, optionally taxes.
    * Produces equity curve and statistics.

Inside `af_backtest_run`, **no HTTP calls**. All data comes from the `panel_adj` prepared beforehand.

### 5.9. `R/portfolio.R` (later)

This is where you implement:

* Tax rules (BDR vs equity vs FIIs).
* Capital gains vs dividends modeling.
* Specific allocation schemes (risk parity, max Sharpe, constraints, position limits).

Screener outputs feed into portfolio construction, but screener itself remains tax-agnostic.

### 5.10. `R/config.R`

Central place for defaults:

* DB path.
* Default lookback window.
* Horizons.
* Macro series mapping.
* Liquidity thresholds.

---

## 6. Data Flows (Step-by-Step)

### 6.1. Initial Bootstrap

1. `af_db_init()`
2. `af_sync_b3(years = 2010:current_year)`
   → fills `prices_raw`.
3. Populate `assets_meta`:

   * From B3 info or manually (asset_type, active, etc.).
4. `af_sync_splits()`

   * Full universe splits → `corporate_actions`.
5. `af_sync_macro()`

   * Fill `macro_series` with CDI, Selic, IPCA, USD, etc.

After this, you have a complete local snapshot.

### 6.2. On-Demand Update Before Analysis

Any time you want to rebalance / re-run:

1. `af_sync_b3()` (only missing years or days; COTAHIST makes this cheap).
2. `af_sync_macro()` (only missing dates).
3. `af_sync_splits()` (only stale symbols by `last_update_splits` threshold).

Then:

* `af_run_screener(config)` for fresh rankings.
* Or `af_backtest_run(...)` on the current DB snapshot.

### 6.3. Screener Execution Flow (Detailed)

Given `config`:

1. Determine date range `[t0, t1]` based on lookback.
2. From `prices_raw`:

   * For each symbol in `assets_meta[active=1]`, compute `median_vol_fin` over `[t0, t1]`.
   * Filter: `median_vol_fin >= min_liquidity`.
3. Build adjusted OHLC for filtered symbols via `af_build_adjusted_panel()`.
4. Load macro series for `[t0, t1]`.
5. Compute metrics via `af_compute_metrics()`:

   * Skip horizons that exceed available history for each symbol.
6. Split metrics by `asset_type` and rank within each class.

---

## 7. Core Algorithms (Implementation Notes)

### 7.1. Split Adjustment

Given:

* Raw OHLC series `P_t` (open, high, low, close).
* Splits: dates `s_i` with ratios `r_i` (as given by `getSplits`, e.g. `0.1` for 10→1).

Use `TTR::adjRatios(P, splits)` to compute a cumulative adjustment factor `A_t` such that:

* For each date `t`, `P_adj,t = P_t * A_t`.

Apply `A_t` identically to `open`, `high`, `low`, `close` so the OHLC range remains coherent.

### 7.2. Returns and Volatility

Daily log return:

* `r_t = log(close_adj_t / close_adj_{t-1})`.

Horizon return over N days (geometric):

* `R_N = exp(sum_{i=1..N} r_{t-i+1}) - 1`.

Annualized vol over window N:

* `vol_N = sd(r_{window}) * sqrt(252)`.

### 7.3. VaR and CVaR (Historical, 95%)

Given a vector of daily returns `r` in the window:

* Sort `r` ascending.
* `VaR_95` = 5th percentile (e.g., `quantile(r, probs = 0.05)`).
* `CVaR_95` = mean of returns below `VaR_95`.

### 7.4. Drawdowns and Ulcer Index

Equity curve from price (normalized):

* `E_t = close_adj_t / close_adj_{t0}`.

Running max:

* `M_t = max_{k <= t} E_k`.

Drawdown:

* `DD_t = (E_t - M_t) / M_t` (negative or zero).

Max drawdown:

* `min(DD_t)`.

Ulcer index:

* `UI = sqrt(mean(DD_t^2))` over the window.

Time underwater:

* Identify contiguous runs where `DD_t < 0`.
* Take average length of those runs.

### 7.5. Betas to Factors

Given asset returns `r_a,t` and factor returns `r_f,t` for aligned dates:

Beta:

* `beta = cov(r_a, r_f) / var(r_f)`.

You can also run a regression:

* `r_a = alpha + beta * r_f + error`, but for speed, covariance/variance is enough.

Compute betas against:

* IBOV (or IBOV11/IBOV index series).
* SPX (converted to BRL if needed).
* USD/BRL FX.
* DI or proxy fixed-income index.

---

## 8. Configuration Layer

Either:

* A simple `list` in R (`af_default_config()`), or
* A YAML file that gets parsed once per session.

Key config fields:

* `db_path`.
* `lookback_days` (e.g., 252).
* `horizons_days` (e.g., `c(21, 63, 126, 252)`).
* `min_liquidity` (e.g., 500000 BRL median daily volume).
* `macro_series_ids`.
* `benchmarks` (e.g., `"IBOV"`, `"SPX"`, `"USD"`, `"CDI"`).

---

## 9. Validation & QA Strategy

You need systematic sanity checks.

### 9.1. Price sanity

* Randomly sample symbols.
* Compare your adjusted close vs Yahoo Adjusted Close for those symbols and dates (one-off diagnostic script).
* Flag if deviations exceed some tolerance (e.g., >1–2%).

### 9.2. Corporate actions sanity

* For a few known split events (ABEV3, GOGL34, etc.), verify:

  * Splits appear in `corporate_actions`.
  * Adjusted price curve behaves as expected across split dates (no fake crash).

### 9.3. Macro sanity

* Plot each macro series; visually check for obviously broken segments (zeros, missing months).

### 9.4. Metric sanity

For a single symbol:

* Manually compute a subset of metrics (e.g., 21d return, 252d vol, max drawdown) in a separate R script and compare to `af_compute_metrics` output.

---

## 10. Roadmap / Phases

### Phase 1 – DB + B3 Ingest

* Implement `db_core.R`, `ingest_b3.R`.
* Get `prices_raw` populated from 2010 to present.

### Phase 2 – Splits Ingest + Adjustment

* Implement `ingest_splits.R` and `adjustment.R`.
* Verify adjusted prices for a small set of symbols vs Yahoo.

### Phase 3 – Macro Ingest

* Implement `ingest_macro.R`.
* Store CDI, Selic, IPCA, USD, etc., in `macro_series`.

### Phase 4 – Metrics Engine

* Implement `metrics.R`.
* Test per symbol metrics thoroughly.

### Phase 5 – Screener

* Implement `screener.R`.
* End-to-end flow: DB → screener → per-class rankings.

### Phase 6 – Backtest Core

* Implement `backtest.R` with a simple long-only strategy to validate the engine.
* Confirm it runs offline and uses only SQLite + in-memory panels.

### Phase 7 – Portfolio & Fiscal Logic (if/when you care)

* Implement tax rules, position limits, rebalancing logic in `portfolio.R`.
* Integrate backtester with fiscal modeling.

---

This is the architecture. If you follow this as `PLAN.md`, you’ll have a clean separation between:

* **Data acquisition & storage** (B3, Yahoo splits, BCB),
* **Price adjustment & panel construction**,
* **Metric computation & screening**,
* **Offline simulation & portfolio logic**.

From here, next concrete step is Phase 1: write `af_db_init()` and `af_sync_b3()` exactly against the schema above, and we can start hardening from there.
