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

  # -------------------------------
  # Selective corporate-actions policy (the "trick")
  # -------------------------------
  enable_selective_actions = TRUE,

  # Cache strategy for corp actions:
  # - "batch" keeps current behavior (hash by symbol set)
  # - "by_symbol" enables incremental caching per ticker
  ca_cache_mode = "batch",

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
  enable_split_plausibility_gate = TRUE,
  split_value_min = 0.05,  # conservative, allows 20:1
  split_value_max = 10.0   # conservative, allows 1:10 reverse

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

  cfg
}
