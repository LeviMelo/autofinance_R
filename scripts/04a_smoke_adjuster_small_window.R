# v2/scripts/04a_smoke_adjuster_small_window.R

# Core
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

# Module 01 (universe)
source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_yearly.R")
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

# Module 03 (corp actions)
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")

# Module 04 (adjuster)
source("v2/modules/04_adjuster/R/zzz_depends.R")
source("v2/modules/04_adjuster/R/build_adjustments.R")
source("v2/modules/04_adjuster/R/apply_adjustments.R")
source("v2/modules/04_adjuster/R/build_panel_adj.R")
source("v2/modules/04_adjuster/R/validate_panel_adj.R")

cfg <- af2_get_config()

# 1) Build small recent universe window
end_date <- Sys.Date() - 1
start_date <- end_date - 365  # 1 year smoke

types_all <- c("equity","fii","etf","bdr")

af2_log("AF2_SMOKE04:", "Universe window ", start_date, " to ", end_date)

univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

# take a small symbol subset for faster Yahoo calls
set.seed(1)
syms <- sort(unique(univ$symbol))
syms_small <- head(sample(syms, min(20, length(syms))), 20)

af2_log("AF2_SMOKE04:", "Corp actions for ", length(syms_small), " symbols")

ca <- af2_ca_build_registry(
  symbols = syms_small,
  cfg = cfg,
  from = "2018-01-01",
  to = Sys.Date(),
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = 1L
)

# 2) Filter universe to same subset
univ_small <- univ[symbol %in% syms_small]

# 3) Build adjusted panel
res <- af2_build_panel_adj(
  universe_raw = univ_small,
  corp_actions = ca,
  manual_events = NULL,
  cfg = cfg,
  verbose = TRUE
)

panel_adj <- res$panel_adj

# 4) Validate
af2_validate_panel_adj(panel_adj)

# 5) Show sample
print(utils::head(panel_adj, 20))

af2_log("AF2_SMOKE04:", "OK - adjuster smoke passed.")
