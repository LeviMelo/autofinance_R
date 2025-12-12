# v2/scripts/00_load_module_05.R
# Loads Module 05 (Screener) + the core dependencies it relies on.
# Guarantees af2_compute_symbol_metrics exists.

af2_src <- function(path) {
  if (!file.exists(path)) stop("Missing file: ", path, call. = FALSE)
  message("SOURCE: ", normalizePath(path, winslash = "/", mustWork = TRUE))
  source(path, local = FALSE)
}

# Ensure we are at project root (the folder that contains /v2)
if (!dir.exists("v2") && basename(getwd()) == "v2") setwd("..")
stopifnot(dir.exists("v2"))

# Core (required by screener functions)
af2_src("v2/modules/00_core/R/utils.R")
af2_src("v2/modules/00_core/R/logging.R")
af2_src("v2/modules/00_core/R/config.R")

# Module 05 (ORDER MATTERS: compute_metrics BEFORE run_screener)
af2_src("v2/modules/05_screener/R/screener_config.R")
af2_src("v2/modules/05_screener/R/validate_screener_input.R")
af2_src("v2/modules/05_screener/R/liquidity_filter.R")
af2_src("v2/modules/05_screener/R/score_rank.R")
af2_src("v2/modules/05_screener/R/compute_metrics.R")
af2_src("v2/modules/05_screener/R/run_screener.R")

# Hard sanity check (this is what just failed for you)
need <- c("af2_run_screener", "af2_compute_symbol_metrics", "af2_get_screener_config",
          "af2_validate_screener_input", "af2_compute_liquidity_from_panel")
miss <- need[!vapply(need, exists, logical(1), inherits = TRUE)]
if (length(miss)) {
  stop("Module 05 load FAILED. Missing functions: ", paste(miss, collapse = ", "), call. = FALSE)
}

message("OK: Module 05 loaded (including af2_compute_symbol_metrics).")
