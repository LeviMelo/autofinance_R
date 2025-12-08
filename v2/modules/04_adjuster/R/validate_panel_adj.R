# v2/modules/04_adjuster/R/validate_panel_adj.R

af2_validate_panel_adj <- function(panel_adj) {

  af2_require(c("data.table"))

  af2_assert_cols(
    panel_adj,
    c("symbol", "refdate", "close_adj_final", "asset_type", "adjustment_state"),
    name = "panel_adj"
  )

  dt <- data.table::as.data.table(panel_adj)
  dt[, refdate := as.Date(refdate)]

  # Must have liquidity column for screener use
  has_turnover <- "turnover" %in% names(dt)
  has_vol_fin  <- "vol_fin" %in% names(dt)
  if (!has_turnover && !has_vol_fin) {
    stop("panel_adj must contain 'turnover' or 'vol_fin'.", call. = FALSE)
  }

  # No duplicate symbol-date
  af2_assert_no_dupes(dt, c("symbol", "refdate"), name = "panel_adj")

  invisible(TRUE)
}
