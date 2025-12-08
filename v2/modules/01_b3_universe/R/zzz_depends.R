# v2/modules/01_b3_universe/R/zzz_depends.R
# Centralized dependency + core imports (NO recursion)

if (!exists("af2_get_config")) {
  stop("af2_get_config not found. Source v2/modules/00_core/R/config.R first.")
}
if (!exists("af2_log")) {
  stop("af2_log not found. Source v2/modules/00_core/R/logging.R first.")
}

# Keep it dumb and safe: no wrapper, no indirection.
af2_b3_require <- function(pkgs) {
  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      stop("Missing package: ", p)
    }
  }
  invisible(TRUE)
}

af2_b3_require(c("data.table", "dplyr", "lubridate", "rb3"))

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
  library(rb3)
})
