# v2/scripts/05a_smoke_screener_real_small_window.R
# End-to-end real-data smoke:
# Universe (rb3 daily) -> Corp actions (quantmod/Yahoo cached)
# -> Adjuster -> Screener
#
# Goal:
# - Prove the full v2 spine works on a small real window + sampled symbols
# - Avoid Yahoo hammering via Module 03 cache
# - Surface any contract mismatches early

# ----------------------------
# 00) Core
# ----------------------------
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

cfg <- af2_get_config()

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
source("v2/modules/01_b3_universe/R/fetch_yearly.R")
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

types_all <- c("equity", "fii", "etf", "bdr")

end_date   <- Sys.Date() - 1
start_date <- end_date - 730  # ~2y for faster smoke

af2_log("AF2_SMOKE05:", "Universe window ", as.character(start_date), " to ", as.character(end_date))

dt_univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

af2_log("AF2_SMOKE05:", "Universe rows=", nrow(dt_univ),
        " symbols=", length(unique(dt_univ$symbol)))

# ----------------------------
# 02) Sample symbols
# ----------------------------
set.seed(42)
sample_n <- 50L

all_syms <- unique(dt_univ$symbol)
syms_smoke <- if (length(all_syms) > sample_n) sample(all_syms, sample_n) else all_syms

dt_univ_smoke <- dt_univ[symbol %in% syms_smoke]

af2_log("AF2_SMOKE05:", "Sampled symbols=", length(syms_smoke),
        " sampled rows=", nrow(dt_univ_smoke))

# ----------------------------
# 03) Corporate actions (quantmod/Yahoo)
# ----------------------------
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")

from_ca <- as.Date("2018-01-01")
to_ca   <- Sys.Date()

# keep conservative parallel for Windows; adjust if you want
n_workers <- 2L

af2_log("AF2_SMOKE05:", "Building corp-actions registry for ", length(syms_smoke), " symbols")

registry <- af2_ca_build_registry(
  symbols = syms_smoke,
  cfg = cfg,
  from = from_ca,
  to = to_ca,
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = n_workers
)

af2_log("AF2_SMOKE05:", "Registry rows=", nrow(registry),
        " symbols with actions=", length(unique(registry$symbol)))

# ----------------------------
# 04) Adjuster (Module 04)
# ----------------------------
source("v2/modules/04_adjuster/R/zzz_depends.R")
source("v2/modules/04_adjuster/R/build_adjustments.R")
source("v2/modules/04_adjuster/R/apply_adjustments.R")
source("v2/modules/04_adjuster/R/build_panel_adj.R")
source("v2/modules/04_adjuster/R/validate_panel_adj.R")

res_adj <- af2_build_panel_adj(
  universe_raw = dt_univ_smoke,
  corp_actions = registry,
  manual_events = NULL,
  cfg = cfg,
  verbose = TRUE
)

panel_adj <- res_adj$panel_adj

af2_validate_panel_adj(panel_adj)

af2_log("AF2_SMOKE05:", "panel_adj rows=", nrow(panel_adj),
        " symbols=", length(unique(panel_adj$symbol)))

# ----------------------------
# 05) Screener (real data)
# ----------------------------
source("v2/modules/05_screener/R/screener_config.R")
source("v2/modules/05_screener/R/validate_screener_input.R")
source("v2/modules/05_screener/R/liquidity_filter.R")
source("v2/modules/05_screener/R/compute_metrics.R")
source("v2/modules/05_screener/R/score_rank.R")
source("v2/modules/05_screener/R/run_screener.R")

af2_log("AF2_SMOKE05:", "Running real-data screener (allow_unresolved=TRUE for smoke).")

res <- af2_run_screener(
  panel_adj,
  allow_unresolved = TRUE
)

af2_log("AF2_SMOKE05:", "by_type integrity check:")
print(lapply(res$by_type, function(x) unique(x$asset_type)))

af2_log("AF2_SMOKE05:", "Top 10 overall:")
print(utils::head(res$full, 10))

af2_log("AF2_SMOKE05:", "Top 5 by type:")
for (tp in names(res$by_type)) {
  af2_log("AF2_SMOKE05:", "Type=", tp)
  print(utils::head(res$by_type[[tp]], 5))
}

af2_log("AF2_SMOKE05:", "OK - real-data screener smoke passed.")
