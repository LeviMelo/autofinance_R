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

  # 1) Join Events onto Universe (Left Join)
  #    We want all market days, with event info where it exists.
  
  # Ensure keys
  data.table::setkey(dt, symbol, refdate)
  if (nrow(ev) > 0) data.table::setkey(ev, symbol, refdate)

  # Merge
  # If ev is empty, we create dummy columns
  if (nrow(ev) == 0) {
    dt[, `:=`(split_value = 1, div_cash = 0, source_mask = "none", has_manual = FALSE)]
  } else {
    dt <- merge(dt, ev, by = c("symbol", "refdate"), all.x = TRUE)
    
    # Fill NAs for non-event days
    dt[is.na(split_value), split_value := 1]
    dt[is.na(div_cash), div_cash := 0]
    dt[is.na(source_mask), source_mask := "none"]
    dt[is.na(has_manual), has_manual := FALSE]
  }

  # Ensure order for cumprod
  data.table::setorder(dt, symbol, refdate)

  # -----------------------
  # 2) Calculate Factors (Grouped by Symbol)
  # -----------------------
  
  # A) Split Cumulative Factor
  dt[, split_factor_cum := af2_adj_rev_cumprod_exclusive(split_value), by = symbol]

  # Apply Split to OHLC (Intermediate)
  # We need split-adjusted close to calculate dividend factor correctly
  dt[, `:=`(
    open_adj_split  = open  * split_factor_cum,
    high_adj_split  = high  * split_factor_cum,
    low_adj_split   = low   * split_factor_cum,
    close_adj_split = close * split_factor_cum
  )]

  # B) Dividend Event Factor
  # Logic: (PrevClose - Div) / PrevClose
  # We shift within group
  dt[, close_prev := data.table::shift(close_adj_split, 1L, type = "lag"), by = symbol]
  
  # Default
  dt[, div_factor_event := 1]
  dt[, issue_div := FALSE]

  # Vectorized Dividend Logic
  # Identify rows with dividends
  is_div <- dt$div_cash > 0
  
  # Bad Dividends: PrevClose missing, <=0, or Div >= PrevClose
  bad_div <- is_div & (is.na(dt$close_prev) | dt$close_prev <= 0 | dt$div_cash >= dt$close_prev)
  if (any(bad_div)) {
    dt[bad_div, issue_div := TRUE]
  }

  # Good Dividends
  good_div <- is_div & !bad_div
  if (any(good_div)) {
    dt[good_div, div_factor_event := (close_prev - div_cash) / close_prev]
  }

  # C) Dividend Cumulative Factor
  dt[, div_factor_cum := af2_adj_rev_cumprod_exclusive(div_factor_event), by = symbol]

  # -----------------------
  # 3) Final Adjustment
  # -----------------------
  dt[, adj_factor_final := split_factor_cum * div_factor_cum]

  dt[, `:=`(
    open_adj_final  = open  * adj_factor_final,
    high_adj_final  = high  * adj_factor_final,
    low_adj_final   = low   * adj_factor_final,
    close_adj_final = close * adj_factor_final
  )]

  # -----------------------
  # 4) Extract Outputs
  # -----------------------
  
  # Adjustment Timeline (Audit table)
  # Keep only columns relevant for auditing
  adjustments <- dt[, .(
    symbol, refdate,
    split_value, div_cash,
    split_factor_cum, div_factor_event, div_factor_cum,
    adj_factor_final,
    source_mask, has_manual, issue_div
  )]

  # Panel (Drop temp calc columns to keep it clean)
  # We keep the _adj columns we created.
  # We drop intermediate calculation helpers.
  dt[, c("split_value", "div_cash", "source_mask", "has_manual", 
         "split_factor_cum", "div_factor_event", "div_factor_cum", 
         "adj_factor_final", "close_prev", "issue_div") := NULL]

  list(
    panel_adj = dt,
    adjustments = adjustments
  )
}