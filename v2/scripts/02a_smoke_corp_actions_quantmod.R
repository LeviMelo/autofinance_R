# v2/scripts/02a_smoke_corp_actions_quantmod.R

# Core
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

# Universe (to get a recent symbol list)
source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_yearly.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

# Corporate actions
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")

cfg <- af2_get_config()

# Use last config year, tiny types subset to keep smoke fast
year_test <- max(cfg$years)
types_test <- c("equity")

af2_log("AF2_B3:", "SMOKE universe for corp-actions seed: year=", year_test)

dt_univ <- af2_b3_build_universe_year(
  year = year_test,
  include_types = types_test,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

# Grab a small symbol slice
syms <- unique(dt_univ$symbol)
syms <- head(syms, 20)

af2_log("AF2_CA:", "SMOKE fetching splits+dividends for 20 symbols...")

dt_ca <- af2_ca_build_registry(
  symbols = syms,
  cfg = cfg,
  from = as.Date(paste0(year_test - 3, "-01-01")),
  to = Sys.Date(),
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = 1L
)

print(dt_ca)
af2_log("AF2_CA:", "OK - corp-actions smoke finished.")
