# v2/modules/00_core/R/config.R

af2_config_default <- list(
  # Universe scope (used later)
  # Dev default: keep it small and fast.
  # Override explicitly for full rebuild.
  years = {
    y <- as.integer(format(Sys.Date(), "%Y"))
    (y-1L):y
  },
  include_types = c("equity", "fii", "etf", "bdr"),

  # Screener liquidity
  min_turnover = 5e5,
  min_days_traded_ratio = 0.8,

  # Corporate actions toggles (used later)
  enable_splits = TRUE,
  enable_manual_events = TRUE,

  # Paths
  cache_dir  = "v2/data/cache",
  raw_dir    = "v2/data/raw",
  fixtures_dir = "v2/data/fixtures",
  manual_dir = "v2/data/manual",
  logs_dir   = "v2/logs",

  # Safety
  allow_unresolved_in_screener = FALSE,

  # Adjuster residual jump safety net (log-return tolerance).
  # 1.0 means any 1-day move >= e^1 (~2.718x) flags suspect_unresolved.
  adj_residual_jump_tol_log = 1.0,

  # -------------------------------
  # Selective corporate-actions policy (the "trick")
  # -------------------------------
  enable_selective_actions = TRUE,

  # Cache strategy for corp actions:
  # - "batch" keeps current behavior (hash by symbol set)
  # - "by_symbol" enables incremental caching per ticker
  ca_cache_mode = "batch",

  # Corporate actions fetch mode:
  # - "chart"   = Yahoo chart endpoint (splits+dividends in one call)
  # - "quantmod"= legacy quantmod calls (getSplits + getDividends)
  ca_fetch_mode = "chart",

  # Candidate prefilter: detect reverse-split-like jumps using abs(log(close/lag)).
  # 1.0 ~ e^1 = 2.718x jump threshold.
  ca_prefilter_jump_log_thr = 1.0,

  # Prefilter heuristics (B3-only)
  ca_prefilter_recent_days   = 252L,
  ca_prefilter_top_n_overall = 200L,
  ca_prefilter_top_n_by_type = 50L,
  ca_prefilter_max_candidates = 300L,

  # One-day gap thresholds by type (very cheap anomaly filter)
  ca_prefilter_gap_equity = -0.20,
  ca_prefilter_gap_fii    = -0.12,
  ca_prefilter_gap_etf    = -0.15,
  ca_prefilter_gap_bdr    = -0.20,

  # Activeness / recency guards for selective CA
  ca_prefilter_active_days = 10L,

  # Use a shorter window for liquidity scoring (robust to old junk)
  ca_prefilter_liq_window_days = 63L,

  # -------------------------------
  # Split sanity gates (debug + safety)
  # Values are PRICE FACTORS after normalization.
  # Typical forward splits: 0.5, 0.333..., 0.2
  # Typical reverse splits: 2, 5, 10 (rare)
  # -------------------------------
  enable_split_plausibility_gate = FALSE,
  split_gate_min = 0.05,  # conservative, allows 20:1
  split_gate_max = 10.0,  # conservative, allows 1:10 reverse

  # Split-gap validation against raw
  enable_split_gap_validation = TRUE,
  split_gap_tol_log = 0.35,

  # NEW: how far forward we allow snapping Yahoo split dates
  # to the next B3 trading day (for weekend/holiday/vendor-date mismatches)
  split_gap_max_forward_days = 5L,

  # How far BACK we allow snapping Yahoo vendor split dates
  # (vendor date can be 1-3 days off vs B3 effective trading day).
  split_gap_max_back_days = 3L,

  # NEW: prefer using post-day OPEN when available (splits are effective at market open);
  # fallback to CLOSE if OPEN missing.
  split_gap_use_open = TRUE

)

af2_get_config <- function(config = NULL) {
  cfg <- af2_config_default
  if (!is.null(config)) {
    # shallow override for now (keep simple)
    for (nm in names(config)) cfg[[nm]] <- config[[nm]]
  }
  # ensure dirs exist
  dir.create(cfg$cache_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$fixtures_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$manual_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$logs_dir, recursive = TRUE, showWarnings = FALSE)

  # -------------------------------
  # Validation of sensitive knobs
  # -------------------------------
  if (!is.null(cfg$ca_cache_mode) &&
      !cfg$ca_cache_mode %in% c("batch", "by_symbol")) {
    stop("Invalid ca_cache_mode: ", cfg$ca_cache_mode,
         ". Allowed: batch, by_symbol", call. = FALSE)
  }

  cfg$ca_prefilter_recent_days <- as.integer(cfg$ca_prefilter_recent_days %||% 252L)
  cfg$ca_prefilter_top_n_overall <- as.integer(cfg$ca_prefilter_top_n_overall %||% 200L)
  cfg$ca_prefilter_top_n_by_type <- as.integer(cfg$ca_prefilter_top_n_by_type %||% 50L)
  cfg$ca_prefilter_max_candidates <- as.integer(cfg$ca_prefilter_max_candidates %||% 300L)

  cfg$enable_split_gap_validation <- isTRUE(cfg$enable_split_gap_validation)
  cfg$split_gap_tol_log <- as.numeric(cfg$split_gap_tol_log %||% 0.35)

  cfg$split_gap_max_forward_days <- as.integer(cfg$split_gap_max_forward_days %||% 5L)
  if (!is.finite(cfg$split_gap_max_forward_days) || cfg$split_gap_max_forward_days < 0L) {
    cfg$split_gap_max_forward_days <- 5L
  }

  cfg$split_gap_use_open <- isTRUE(cfg$split_gap_use_open)

  # -------------------------------
  # Normalize newer knobs
  # -------------------------------
  cfg$ca_fetch_mode <- tolower(trimws(cfg$ca_fetch_mode %||% "chart"))
  if (!cfg$ca_fetch_mode %in% c("chart", "quantmod")) {
    cfg$ca_fetch_mode <- "chart"
  }

  cfg$ca_prefilter_jump_log_thr <- as.numeric(cfg$ca_prefilter_jump_log_thr %||% 1.0)
  if (!is.finite(cfg$ca_prefilter_jump_log_thr) || cfg$ca_prefilter_jump_log_thr < 0.5) {
    cfg$ca_prefilter_jump_log_thr <- 1.0
  }

  cfg$adj_residual_jump_tol_log <- as.numeric(cfg$adj_residual_jump_tol_log %||% 1.0)
  if (!is.finite(cfg$adj_residual_jump_tol_log) || cfg$adj_residual_jump_tol_log <= 0) {
    cfg$adj_residual_jump_tol_log <- 1.0
  }

  cfg$split_gap_max_back_days <- as.integer(cfg$split_gap_max_back_days %||% 3L)
  if (!is.finite(cfg$split_gap_max_back_days) || cfg$split_gap_max_back_days < 0L) {
    cfg$split_gap_max_back_days <- 3L
  }

  cfg
}
