# v2/modules/03_corporate_actions/R/zzz_depends.R

if (!exists("af2_get_config")) {
  stop("af2_get_config not found. Source v2/modules/00_core/R/config.R first.")
}
if (!exists("af2_log")) {
  stop("af2_log not found. Source v2/modules/00_core/R/logging.R first.")
}
if (!exists("%||%")) {
  stop("%||% not found. Source v2/modules/00_core/R/utils.R first.")
}

af2_ca_require <- function(pkgs) {
  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      stop("Missing package for corporate actions module: ", p, call. = FALSE)
    }
  }
  invisible(TRUE)
}

# quantmod is the key dependency here
af2_ca_require(c("data.table", "quantmod", "xts"))

suppressPackageStartupMessages({
  library(data.table)
  library(quantmod)
  library(xts)
})
