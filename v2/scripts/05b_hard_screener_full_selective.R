# v2/scripts/05b_hard_screener_full_selective.R
# Full-B3-ish hard test WITH the selective Yahoo "trick".
# Expectation:
# - Universe may have ~2000+ symbols
# - Yahoo fetch should be a small fraction (candidates)
# - Per-symbol caching can be enabled

# ----------------------------
# 00) Core
# ----------------------------
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

cfg <- af2_get_config(list(
  # Turn on the real architecture knobs here
  enable_selective_actions = TRUE,

  # Strongly recommended for the hard run:
  ca_cache_mode = "by_symbol",

  # You can tighten/loosen these as you learn
  ca_prefilter_top_n_overall = 200L,
  ca_prefilter_top_n_by_type = 50L,
  ca_prefilter_max_candidates = 300L
))

af2_log_cfg(cfg)

# ----------------------------
# 01) Universe (rb3)
# ----------------------------
source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

types_all <- c("equity", "fii", "etf", "bdr")

end_date   <- Sys.Date() - 1
start_date <- end_date - 730

af2_log("AF2_HARD05:", "Universe window ", as.character(start_date), " to ", as.character(end_date))

dt_univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

af2_log("AF2_HARD05:", "Universe rows=", nrow(dt_univ),
        " symbols=", length(unique(dt_univ$symbol)))

# ----------------------------
# 02) Corporate actions + Adjuster (selective wrapper)
# ----------------------------
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")
source("v2/modules/03_corporate_actions/R/select_candidates.R")

source("v2/modules/04_adjuster/R/zzz_depends.R")
source("v2/modules/04_adjuster/R/build_adjustments.R")
source("v2/modules/04_adjuster/R/apply_adjustments.R")
source("v2/modules/04_adjuster/R/build_panel_adj.R")
source("v2/modules/04_adjuster/R/build_panel_adj_selective.R")
source("v2/modules/04_adjuster/R/validate_panel_adj.R")

af2_log("AF2_HARD05:", "Building adjusted panel with selective Yahoo registry...")

res_adj <- af2_build_panel_adj_selective(
  universe_raw = dt_univ,
  manual_events = NULL,
  cfg = cfg,
  from_ca = "2018-01-01",
  to_ca   = Sys.Date(),
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = 1L
)

panel_adj <- res_adj$panel_adj
af2_validate_panel_adj(panel_adj)

af2_log("AF2_HARD05:", "panel_adj rows=", nrow(panel_adj),
        " symbols=", length(unique(panel_adj$symbol)))

# ----------------------------
# 03) Screener
# ----------------------------
source("v2/modules/05_screener/R/screener_config.R")
source("v2/modules/05_screener/R/validate_screener_input.R")
source("v2/modules/05_screener/R/liquidity_filter.R")
source("v2/modules/05_screener/R/compute_metrics.R")
source("v2/modules/05_screener/R/score_rank.R")
source("v2/modules/05_screener/R/run_screener.R")

af2_log("AF2_HARD05:", "Running screener on full adjusted panel...")

res <- af2_run_screener(
  panel_adj,
  config = list(
    lookback_days = 252L,
    horizons_days = c(21L, 63L, 126L, 252L)
  ),
  allow_unresolved = NULL
)

af2_log("AF2_HARD05:", "Top 20 overall:")
print(utils::head(res$full, 20))

af2_log("AF2_HARD05:", "OK - hard selective full run finished.")
