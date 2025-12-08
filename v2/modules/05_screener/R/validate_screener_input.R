# v2/modules/05_screener/R/validate_screener_input.R

af2_validate_screener_input <- function(panel_adj, allow_unresolved = FALSE) {
  af2_require(c("data.table"))
  af2_assert_cols(
    panel_adj,
    c("symbol", "refdate", "close_adj_final", "asset_type", "adjustment_state"),
    name = "panel_adj"
  )

  dt <- data.table::as.data.table(panel_adj)
  dt[, refdate := as.Date(refdate)]

  # Basic type sanity
  if (!is.character(dt$symbol)) stop("panel_adj$symbol must be character.", call. = FALSE)
  if (!is.character(dt$asset_type)) stop("panel_adj$asset_type must be character.", call. = FALSE)
  if (!is.character(dt$adjustment_state)) stop("panel_adj$adjustment_state must be character.", call. = FALSE)

  # Must have at least one liquidity column
  has_turnover <- "turnover" %in% names(dt)
  has_vol_fin  <- "vol_fin" %in% names(dt)
  if (!has_turnover && !has_vol_fin) {
    stop("panel_adj must contain either 'turnover' or 'vol_fin' for liquidity logic.", call. = FALSE)
  }

  # No duplicate rows per symbol-date
  af2_assert_no_dupes(dt, c("symbol", "refdate"), name = "panel_adj")

  # Adjustment state policy
  bad <- dt[adjustment_state == "suspect_unresolved"]
  if (nrow(bad) && !isTRUE(allow_unresolved)) {
    stop(
      "Screener input contains unresolved suspects.\n",
      "You must fix upstream adjuster/manual registry OR set allow_unresolved=TRUE.\n",
      "Example symbols: ", paste(unique(utils::head(bad$symbol, 10)), collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}
