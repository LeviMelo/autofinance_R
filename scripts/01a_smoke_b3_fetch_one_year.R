# v2/scripts/01a_smoke_b3_fetch_one_year.R
# Real-data smoke test:
# - fetch 1 year
# - include only 1-2 types
# - confirm contract + speed + caching

source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_yearly.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

cfg <- af2_get_config()
af2_log("AF2_CFG:", "\n", paste(capture.output(str(cfg)), collapse = "\n"))

year_test <- max(cfg$years)
types_test <- c("equity", "etf")

af2_log("AF2_B3:", "SMOKE: year=", year_test, " types=", paste(types_test, collapse = ","))

t0 <- Sys.time()
dt <- af2_b3_build_universe_year(
  year = year_test,
  include_types = types_test,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE,
  force_download = FALSE,
  reprocess = FALSE
)
t1 <- Sys.time()

af2_log("AF2_B3:", "SMOKE elapsed seconds: ", round(as.numeric(difftime(t1, t0, units = "secs")), 2))

af2_log("AF2_B3:", "Head:")
print(utils::head(dt, 10))

# Contract check
req <- c("symbol","refdate","open","high","low","close","turnover","qty","asset_type")
stopifnot(all(req %in% names(dt)))

af2_log("AF2_B3:", "SMOKE OK.")
