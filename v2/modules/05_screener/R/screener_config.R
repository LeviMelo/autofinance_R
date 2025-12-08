# v2/modules/05_screener/R/screener_config.R

af2_screener_config_default <- list(
  lookback_days   = 252L,
  horizons_days   = c(21L, 63L, 126L, 252L),

  # Liquidity
  min_turnover = 5e5,
  min_days_traded_ratio = 0.8,

  # Scoring weights (z-score aggregation)
  score_weights = list(
    # Momentum
    ret_21d  = +0.3,
    ret_63d  = +0.6,
    ret_126d = +0.9,
    ret_252d = +1.0,

    # Risk / stability penalties
    vol_21d  = -0.4,
    vol_252d = -0.7,
    max_dd   = -0.8,
    ulcer_index = -0.8,

    # Liquidity penalty
    amihud = -0.5
  )
)

af2_get_screener_config <- function(config = NULL) {
  cfg <- af2_screener_config_default
  if (!is.null(config)) {
    for (nm in names(config)) cfg[[nm]] <- config[[nm]]
  }
  cfg
}
