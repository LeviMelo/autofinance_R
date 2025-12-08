# v2/modules/00_core/R/config.R

af2_config_default <- list(
  # Universe scope (used later)
  years = 2018:as.integer(format(Sys.Date(), "%Y")),
  include_types = c("equity", "fii", "etf", "bdr"),

  # Screener liquidity
  min_turnover = 5e5,
  min_days_traded_ratio = 0.8,

  # Corporate actions toggles (used later)
  enable_splits = TRUE,
  enable_manual_events = TRUE,

  # Paths
  cache_dir  = "v2/data/cache",
  raw_dir    = "v2/data/raw",
  fixtures_dir = "v2/data/fixtures",
  manual_dir = "v2/data/manual",
  logs_dir   = "v2/logs",

  # Safety
  allow_unresolved_in_screener = FALSE
)

af2_get_config <- function(config = NULL) {
  cfg <- af2_config_default
  if (!is.null(config)) {
    # shallow override for now (keep simple)
    for (nm in names(config)) cfg[[nm]] <- config[[nm]]
  }
  # ensure dirs exist
  dir.create(cfg$cache_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$fixtures_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$manual_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cfg$logs_dir, recursive = TRUE, showWarnings = FALSE)
  cfg
}
