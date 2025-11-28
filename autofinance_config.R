############################################################
# autofinance_config.R
# Configurações globais, caminhos e defaults
############################################################

af_default_db_path <- "data/autofinance_v2.sqlite"

af_default_packages <- c(
  "data.table",
  "RSQLite",
  "DBI",
  "xts",
  "zoo",
  "quantmod",
  "TTR",
  "lubridate"
  # Para risk_models avançados:
  # "rugarch",
  # "rmgarch",
  # "vars"
)

af_attach_packages <- function(pkgs = af_default_packages) {
  for (p in pkgs) {
    if (!requireNamespace(p, quietly = TRUE)) {
      stop(sprintf("Package '%s' is required but not installed.", p))
    }
    suppressPackageStartupMessages(library(p, character.only = TRUE))
  }
}

af_get_db_path <- function() {
  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  af_default_db_path
}
