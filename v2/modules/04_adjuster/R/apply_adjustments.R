# v2/modules/04_adjuster/R/apply_adjustments.R

# Helper: reverse cumulative product excluding same-day event.
# For a vector of daily event factors:
# factor(date) = product of event_factors for events with refdate > date
af2_adj_rev_cumprod_exclusive <- function(x) {
  if (!length(x)) return(x)
  v <- rev(cumprod(rev(x)))
  data.table::shift(v, 1L, type = "lead", fill = 1)
}

# Compute adjustments for one symbol
af2_adj_compute_symbol_adjustments <- function(dt_sym, ev_sym) {
  dt <- data.table::as.data.table(dt_sym)
  data.table::setorder(dt, refdate)

  # Universe raw contract minimum for adjuster
  af2_assert_cols(dt,
                  c("symbol", "refdate", "open", "high", "low", "close",
                    "turnover", "qty", "asset_type"),
                  name = "universe_raw(symbol)")

  # Prep event table for this symbol
  ev <- data.table::as.data.table(ev_sym)
  if (!nrow(ev)) {
    ev <- data.table(
      symbol = unique(dt$symbol)[1],
      refdate = as.Date(character()),
      split_value = numeric(),
      div_cash = numeric(),
      source_mask = character(),
      has_manual = logical()
    )
  }

  # Join events onto daily grid
  dt[, split_value := 1]
  dt[, div_cash := 0]
  dt[, source_mask := "none"]
  dt[, has_manual := FALSE]

  if (nrow(ev)) {
    dt[ev,
       on = .(refdate),
       `:=`(
         split_value = i.split_value,
         div_cash = i.div_cash,
         source_mask = i.source_mask,
         has_manual = i.has_manual
       )]
  }

  dt[is.na(split_value), split_value := 1]
  dt[is.na(div_cash), div_cash := 0]
  dt[is.na(source_mask), source_mask := "none"]
  dt[is.na(has_manual), has_manual := FALSE]

  # -----------------------
  # 1) Split cumulative factor
  # -----------------------
  dt[, split_factor_cum := af2_adj_rev_cumprod_exclusive(split_value)]

  # Apply split adjustment to OHLC
  dt[, open_adj_split  := open  * split_factor_cum]
  dt[, high_adj_split  := high  * split_factor_cum]
  dt[, low_adj_split   := low   * split_factor_cum]
  dt[, close_adj_split := close * split_factor_cum]

  # -----------------------
  # 2) Dividend event factor
  # -----------------------
  dt[, close_prev := data.table::shift(close_adj_split, 1L)]

  # Default no-dividend factor
  dt[, div_factor_event := 1]

  # Flag issues when we cannot compute a sane dividend factor
  dt[, issue_div := FALSE]

  has_div_row <- dt$div_cash > 0

  if (any(has_div_row, na.rm = TRUE)) {
    # bad cases
    bad_idx <- which(
      has_div_row & (
        !is.finite(dt$close_prev) |
          is.na(dt$close_prev) |
          dt$close_prev <= 0 |
          dt$div_cash >= dt$close_prev
      )
    )
    if (length(bad_idx)) dt[bad_idx, issue_div := TRUE]

    # good cases
    good_idx <- which(
      has_div_row &
        is.finite(dt$close_prev) &
        !is.na(dt$close_prev) &
        dt$close_prev > 0 &
        dt$div_cash < dt$close_prev
    )

    if (length(good_idx)) {
      dt[good_idx,
         div_factor_event := (close_prev - div_cash) / close_prev]
    }
  }

  # Guarantee numeric stability
  dt[!is.finite(div_factor_event) | is.na(div_factor_event), div_factor_event := 1]
  dt[div_factor_event < 0, div_factor_event := 0]

  # -----------------------
  # 3) Dividend cumulative factor
  # -----------------------
  dt[, div_factor_cum := af2_adj_rev_cumprod_exclusive(div_factor_event)]

  # -----------------------
  # 4) Final factor + apply
  # -----------------------
  dt[, adj_factor_final := split_factor_cum * div_factor_cum]

  dt[, open_adj_final  := open  * adj_factor_final]
  dt[, high_adj_final  := high  * adj_factor_final]
  dt[, low_adj_final   := low   * adj_factor_final]
  dt[, close_adj_final := close * adj_factor_final]

  # Prepare adjustment timeline output for auditing
  adj_timeline <- dt[, .(
    symbol,
    refdate,
    split_value,
    div_cash,
    split_factor_cum,
    div_factor_event,
    div_factor_cum,
    adj_factor_final,
    source_mask,
    has_manual,
    issue_div
  )]

  list(
    panel_cols = dt,
    adjustments = adj_timeline
  )
}

# Apply adjustments to full universe_raw
# Returns:
#   list(panel_adj = ..., adjustments = ...)
af2_adj_apply_adjustments <- function(universe_raw,
                                      events,
                                      verbose = TRUE) {

  dt <- data.table::as.data.table(universe_raw)
  data.table::setorder(dt, symbol, refdate)

  ev <- data.table::as.data.table(events)
  if (!nrow(ev)) {
    ev <- data.table(
      symbol = character(),
      refdate = as.Date(character()),
      split_value = numeric(),
      div_cash = numeric(),
      source_mask = character(),
      has_manual = logical()
    )
  }

  syms <- unique(dt$symbol)

  res_pan <- vector("list", length(syms))
  res_adj <- vector("list", length(syms))

  for (i in seq_along(syms)) {
    s <- syms[i]

    dt_sym <- dt[symbol == s]
    ev_sym <- ev[symbol == s]

    out <- af2_adj_compute_symbol_adjustments(dt_sym, ev_sym)

    res_pan[[i]] <- out$panel_cols
    res_adj[[i]] <- out$adjustments

    if (verbose && i %% 200 == 0) {
      af2_log("AF2_ADJ:", "Progress ", i, "/", length(syms))
    }
  }

  panel_all <- data.table::rbindlist(res_pan, use.names = TRUE, fill = TRUE)
  adj_all   <- data.table::rbindlist(res_adj, use.names = TRUE, fill = TRUE)

  data.table::setorder(panel_all, symbol, refdate)
  data.table::setorder(adj_all, symbol, refdate)

  list(panel_adj = panel_all, adjustments = adj_all)
}
