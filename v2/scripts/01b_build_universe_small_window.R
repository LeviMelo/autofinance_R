# v2/scripts/01b_build_universe_small_window.R
# Build a small 2-year universe for all 4 types
# and save a narrow raw panel artifact.

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

years_small <- sort(unique(c(max(cfg$years) - 1L, max(cfg$years))))
types_all <- c("equity","fii","etf","bdr")

af2_log("AF2_B3:", "Building small-window universe: ",
        paste(years_small, collapse = ", "),
        " types=", paste(types_all, collapse = ","))

dt_all <- af2_b3_build_universe(
  years = years_small,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

af2_log("AF2_B3:", "Universe rows total: ", nrow(dt_all))
af2_log("AF2_B3:", "Symbols total: ", length(unique(dt_all$symbol)))

# Save an artifact for next module (panel)
artifact_dir <- file.path(cfg$raw_dir, "b3_universe")
if (!dir.exists(artifact_dir)) dir.create(artifact_dir, recursive = TRUE)

artifact_file <- file.path(
  artifact_dir,
  paste0("universe_raw_", min(years_small), "_", max(years_small), ".rds")
)

saveRDS(dt_all, artifact_file)

af2_log("AF2_B3:", "Wrote artifact: ", artifact_file)
