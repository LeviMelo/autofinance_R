# v2/modules/04_adjuster/R/apply_adjustments.R

# Helper: reverse cumulative product excluding same-day event.
# Optimized to handle vector input but respect grouping via by=.
af2_adj_rev_cumprod_exclusive <- function(x) {
  if (!length(x)) return(x)
  # rev(cumprod(rev(x))) matches the logic: factor applies to all PREVIOUS dates
  v <- rev(cumprod(rev(x)))
  # shift lead fill=1 shifts it so factor(t) = product(events > t)
  data.table::shift(v, 1L, type = "lead", fill = 1)
}

# Apply adjustments to full universe_raw (VECTORIZED)
# Returns:
#   list(panel_adj = ..., adjustments = ...)
af2_adj_apply_adjustments <- function(universe_raw,
                                      events,
                                      verbose = TRUE) {

  dt <- data.table::as.data.table(universe_raw)
  ev <- data.table::as.data.table(events)

  # 1) Join events onto market days
  data.table::setkey(dt, symbol, refdate)
  if (nrow(ev) > 0) data.table::setkey(ev, symbol, refdate)

  if (nrow(ev) == 0) {
    dt[, `:=`(split_value = 1, div_cash = 0, source_mask = "none", has_manual = FALSE)]
  } else {
    dt <- merge(dt, ev, by = c("symbol", "refdate"), all.x = TRUE)

    dt[is.na(split_value), split_value := 1]
    dt[is.na(div_cash), div_cash := 0]
    dt[is.na(source_mask), source_mask := "none"]
    dt[is.na(has_manual), has_manual := FALSE]
  }

  data.table::setorder(dt, symbol, refdate)

  # 2) Split cumulative factor (exclusive)
  dt[, split_factor_cum := af2_adj_rev_cumprod_exclusive(split_value), by = symbol]

  # Split-adjusted OHLC
  dt[, `:=`(
    open_adj_split  = open  * split_factor_cum,
    high_adj_split  = high  * split_factor_cum,
    low_adj_split   = low   * split_factor_cum,
    close_adj_split = close * split_factor_cum
  )]

  # 3) Dividend factor (with basis-mismatch rescue)
  dt[, close_prev := data.table::shift(close_adj_split, 1L, type = "lag"), by = symbol]

  dt[, `:=`(
    div_factor_event = 1,
    issue_div = FALSE,
    # NEW (audit): effective dividend used in the formula
    div_cash_eff = div_cash,
    div_scaled = FALSE
  )]

  is_div <- dt$div_cash > 0

  # If dividend looks impossible on split-adjusted basis, try scaling it by split_factor_cum
  # This ONLY helps for forward splits (factor < 1).
  need_scale <- is_div &
    is.finite(dt$close_prev) & dt$close_prev > 0 &
    is.finite(dt$split_factor_cum) & dt$split_factor_cum > 0 & dt$split_factor_cum < 1 &
    is.finite(dt$div_cash) & dt$div_cash >= dt$close_prev

  if (any(need_scale)) {
    idx_need <- which(need_scale)
    scaled <- dt$div_cash[idx_need] * dt$split_factor_cum[idx_need]

    ok_scaled <- is.finite(scaled) & scaled >= 0 & scaled < dt$close_prev[idx_need]
    if (any(ok_scaled)) {
      idx_ok <- idx_need[ok_scaled]
      dt[idx_ok, `:=`(div_cash_eff = scaled[ok_scaled], div_scaled = TRUE)]
    }
  }

  # Bad dividends (after rescue attempt)
  bad_div <- is_div & (
    is.na(dt$close_prev) | dt$close_prev <= 0 |
      is.na(dt$div_cash_eff) | dt$div_cash_eff < 0 |
      dt$div_cash_eff >= dt$close_prev
  )

  if (any(bad_div)) dt[bad_div, issue_div := TRUE]

  good_div <- is_div & !bad_div
  if (any(good_div)) {
    dt[good_div, div_factor_event := (close_prev - div_cash_eff) / close_prev]
  }

  # Dividend cumulative factor (exclusive)
  dt[, div_factor_cum := af2_adj_rev_cumprod_exclusive(div_factor_event), by = symbol]

  # 4) Final factor + final adjusted OHLC
  dt[, adj_factor_final := split_factor_cum * div_factor_cum]

  dt[, `:=`(
    open_adj_final  = open  * adj_factor_final,
    high_adj_final  = high  * adj_factor_final,
    low_adj_final   = low   * adj_factor_final,
    close_adj_final = close * adj_factor_final
  )]

  # 5) Outputs
  adjustments <- dt[, .(
    symbol, refdate,
    split_value, div_cash,
    div_cash_eff, div_scaled,
    split_factor_cum, div_factor_event, div_factor_cum,
    adj_factor_final,
    source_mask, has_manual, issue_div
  )]

  dt[, c("split_value", "div_cash", "source_mask", "has_manual",
         "split_factor_cum", "div_factor_event", "div_factor_cum",
         "adj_factor_final", "close_prev", "issue_div",
         "div_cash_eff", "div_scaled") := NULL]

  list(panel_adj = dt, adjustments = adjustments)
}