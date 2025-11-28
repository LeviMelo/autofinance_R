############################################################
# autofinance_config.R
# Global config: DB path, package management
############################################################

# ---- 1. DB PATH ----

af_default_db_path <- "data/autofinance.sqlite"

af_get_db_path <- function() {
  # Ensure ./data exists
  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  af_default_db_path
}

# Constant used by all other modules
AF_DB_PATH <- af_get_db_path()

# ---- 2. DEFAULT PACKAGES ----
# This is "everything we conceptually care about" for the project.
# Not all of them are required by every script, but af_attach_packages()
# will be called with specific subsets where needed.

af_default_packages <- c(
  # Core infra
  "data.table",
  "DBI",
  "RSQLite",

  # Time series & finance
  "xts",
  "zoo",
  "quantmod",
  "TTR",

  # Utils
  "httr",
  "jsonlite",
  "lubridate",

  # Optimization
  "quadprog",

  # Risk-model packages (we DO care about them)
  # These may be heavy; they will be installed on demand.
  "rugarch",
  "rmgarch",
  "vars"
)

# ---- 3. PACKAGE ATTACH / INSTALL LOGIC ----

af_attach_packages <- function(pkgs = af_default_packages,
                               auto_install = TRUE) {
  pkgs <- unique(pkgs)

  # Ensure we have a CRAN repo set (needed for install.packages)
  if (auto_install) {
    repos <- getOption("repos")
    if (is.null(repos) || identical(repos["CRAN"], "@CRAN@") || is.na(repos["CRAN"])) {
      options(repos = c(CRAN = "https://cloud.r-project.org"))
    }
  }

  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      if (auto_install) {
        message("Package '", p, "' not found. Installing from CRAN...")
        utils::install.packages(p)
      }
      # Try again
      if (!requireNamespace(p, quietly = TRUE)) {
        stop(
          sprintf("Package '%s' is required but could not be loaded/installed.", p),
          call. = FALSE
        )
      }
    }
    # Attach so you can inspect stuff interactively if you want
    suppressPackageStartupMessages(
      library(p, character.only = TRUE)
    )
  }

  invisible(TRUE)
}
