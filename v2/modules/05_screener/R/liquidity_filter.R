# v2/modules/05_screener/R/liquidity_filter.R

af2_compute_liquidity_from_panel <- function(panel_adj,
                                             min_turnover,
                                             min_days_traded_ratio) {
  af2_require("data.table")
  dt <- data.table::as.data.table(panel_adj)

  # unify turnover column name
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }

  dt[, traded_flag := is.finite(close_adj_final) & !is.na(close_adj_final)]

  liq <- dt[, .(
    median_turnover = stats::median(turnover, na.rm = TRUE),
    days_traded_ratio = mean(traded_flag, na.rm = TRUE)
  ), by = symbol]

  liq[is.na(median_turnover), median_turnover := 0]
  liq[is.na(days_traded_ratio), days_traded_ratio := 0]

  liq[
    median_turnover >= min_turnover &
      days_traded_ratio >= min_days_traded_ratio
  ]
}
