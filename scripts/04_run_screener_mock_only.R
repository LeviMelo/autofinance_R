# v2/scripts/04_run_screener_mock_only.R
# Runs the v2 screener ONLY on fixtures.
# This is your daily sanity anchor.

# Core
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

# Screener
source("v2/modules/05_screener/R/screener_config.R")
source("v2/modules/05_screener/R/validate_screener_input.R")
source("v2/modules/05_screener/R/liquidity_filter.R")
source("v2/modules/05_screener/R/compute_metrics.R")
source("v2/modules/05_screener/R/score_rank.R")
source("v2/modules/05_screener/R/run_screener.R")

cfg <- af2_get_config()

fixture_path <- file.path(cfg$fixtures_dir, "panel_mock_small.rds")
if (!file.exists(fixture_path)) {
  stop("Fixture not found: ", fixture_path,
       "\nRun: source('v2/scripts/00_make_fixtures.R')", call. = FALSE)
}

panel <- readRDS(fixture_path)

af2_log_cfg(cfg)
af2_log("AF2_SCR:", "Running screener on mock_small...")

res <- af2_run_screener(panel)

stopifnot(is.list(res), "full" %in% names(res))
stopifnot(nrow(res$full) == 4L)
stopifnot(all(c("AAA4","BBB11","CCC34","DDD11") %in% res$full$symbol))

# Rankings should be complete for this fixture
stopifnot(all(!is.na(res$full$rank_overall)))

print(utils::head(res$full, 10))

af2_log("AF2_SCR:", "OK - mock-only screener passed.")
