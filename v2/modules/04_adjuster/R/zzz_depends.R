# v2/modules/04_adjuster/R/zzz_depends.R
# Dependencies + guardrails for Module 04 (Adjuster)

# Must have core loaded first
if (!exists("af2_get_config")) {
  stop("af2_get_config not found. Source v2/modules/00_core/R/config.R first.")
}
if (!exists("af2_log")) {
  stop("af2_log not found. Source v2/modules/00_core/R/logging.R first.")
}
if (!exists("%||%")) {
  stop("%||% not found. Source v2/modules/00_core/R/utils.R first.")
}
if (!exists("af2_require")) {
  stop("af2_require not found. Source v2/modules/00_core/R/utils.R first.")
}
if (!exists("af2_assert_cols")) {
  stop("af2_assert_cols not found. Source v2/modules/00_core/R/utils.R first.")
}
if (!exists("af2_assert_no_dupes")) {
  stop("af2_assert_no_dupes not found. Source v2/modules/00_core/R/utils.R first.")
}

af2_adj_require <- function(pkgs) {
  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      stop("Missing package for adjuster module: ", p, call. = FALSE)
    }
  }
  invisible(TRUE)
}

af2_adj_require(c("data.table"))

suppressPackageStartupMessages({
  library(data.table)
})
