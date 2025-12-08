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
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

cfg <- af2_get_config()

# Choose mode here to avoid future screener regressions.
# "yearly" = backfill-style artifact
# "daily"  = screener-grade recent window
mode <- "yearly"
types_all <- c("equity","fii","etf","bdr")

if (mode == "yearly") {

  years_small <- sort(unique(c(max(cfg$years) - 1L, max(cfg$years))))

  af2_log("AF2_B3:", "Building small-window universe (YEARLY): ",
          paste(years_small, collapse = ", "),
          " types=", paste(types_all, collapse = ","))

  dt_all <- af2_b3_build_universe(
    years = years_small,
    include_types = types_all,
    cfg = cfg,
    verbose = TRUE,
    use_cache = TRUE
  )

  artifact_file <- file.path(
    file.path(cfg$raw_dir, "b3_universe"),
    paste0("universe_raw_", min(years_small), "_", max(years_small), ".rds")
  )

} else {

  # Recent ~2-year screener-style window
  end_date <- Sys.Date() - 1
  start_date <- end_date - 730

  af2_log("AF2_B3:", "Building small-window universe (DAILY): ",
          as.character(start_date), " to ", as.character(end_date),
          " types=", paste(types_all, collapse = ","))

  dt_all <- af2_b3_build_universe_window(
    start_date = start_date,
    end_date   = end_date,
    include_types = types_all,
    cfg = cfg,
    verbose = TRUE,
    use_cache = TRUE
  )

  artifact_file <- file.path(
    file.path(cfg$raw_dir, "b3_universe"),
    paste0("universe_raw_daily_",
           format(as.Date(start_date), "%Y%m%d"), "_",
           format(as.Date(end_date), "%Y%m%d"),
           ".rds")
  )
}

af2_log("AF2_B3:", "Universe rows total: ", nrow(dt_all))
af2_log("AF2_B3:", "Symbols total: ", length(unique(dt_all$symbol)))

artifact_dir <- file.path(cfg$raw_dir, "b3_universe")
if (!dir.exists(artifact_dir)) dir.create(artifact_dir, recursive = TRUE)

saveRDS(dt_all, artifact_file)
af2_log("AF2_B3:", "Wrote artifact: ", artifact_file)

RUN_DAILY_SMOKE <- FALSE

if (mode == "yearly" && isTRUE(RUN_DAILY_SMOKE)) {
  end_date <- Sys.Date() - 1
  start_date <- end_date - 90  # ~3 months
  dt_daily_lazy <- af2_b3_fetch_daily_lazy(
    start_date, end_date,
    cfg = cfg,
    verbose = TRUE,
    throttle = FALSE
  )
  # just force a tiny collect to prove it works
  print(dt_daily_lazy |> dplyr::slice_head(n = 5) |> dplyr::collect())
}
