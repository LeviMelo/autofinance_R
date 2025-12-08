# v2/modules/04_adjuster/R/build_panel_adj.R

# High-level builder:
#   universe_raw (Module 01 output)
#   corp_actions (Module 03 output)
#   manual_events (optional)
#
# Returns:
#   list(panel_adj = ..., adjustments = ..., events = ...)

af2_build_panel_adj <- function(universe_raw,
                                corp_actions = NULL,
                                manual_events = NULL,
                                cfg = NULL,
                                verbose = TRUE) {

  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)

  # Strict universe column expectations for v2
  af2_assert_cols(
    dt,
    c("symbol", "refdate", "open", "high", "low", "close",
      "turnover", "qty", "asset_type"),
    name = "universe_raw"
  )

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, refdate := as.Date(refdate)]

  af2_assert_no_dupes(dt, c("symbol", "refdate"), name = "universe_raw")

  # 1) Build event table (split_value + div_cash per symbol-date)
  events <- af2_adj_build_events(
    corp_actions = corp_actions,
    manual_events = manual_events,
    verbose = verbose
  )

  # 2) Apply adjustments
  out <- af2_adj_apply_adjustments(
    universe_raw = dt,
    events = events,
    verbose = verbose
  )

  panel <- out$panel_adj
  adj_tl <- out$adjustments

  # 3) Add raw aliases for clarity/audit
  panel[, `:=`(
    open_raw = open,
    high_raw = high,
    low_raw  = low,
    close_raw = close
  )]

  # 4) Compute adjustment_state per symbol (research-grade honesty)
  # We base this on:
  # - presence of split/dividend events
  # - whether manual was involved
  # - whether dividend computation had issues

  # Restrict event flags to the panel date range for state semantics
  panel_min <- min(panel$refdate, na.rm = TRUE)
  panel_max <- max(panel$refdate, na.rm = TRUE)

  events_state <- events[
    refdate >= panel_min & refdate <= panel_max
  ]
  
  ev_flags <- if (nrow(events_state)) {
    events_state[, .(
      has_split = any(is.finite(split_value) & split_value != 1),
      has_div   = any(is.finite(div_cash) & div_cash > 0),
      has_manual = any(isTRUE(has_manual))
    ), by = symbol]
  } else {
    data.table(symbol = unique(panel$symbol),
               has_split = FALSE, has_div = FALSE, has_manual = FALSE)
  }

  issue_flags <- adj_tl[, .(
    issue_div_any = any(isTRUE(issue_div))
  ), by = symbol]

  state_dt <- merge(ev_flags, issue_flags, by = "symbol", all = TRUE)
  state_dt[is.na(has_split), has_split := FALSE]
  state_dt[is.na(has_div), has_div := FALSE]
  state_dt[is.na(has_manual), has_manual := FALSE]
  state_dt[is.na(issue_div_any), issue_div_any := FALSE]

  state_dt[, adjustment_state := fifelse(
    issue_div_any, "suspect_unresolved",
    fifelse(
      has_manual, "manual_override",
      fifelse(
        has_split & has_div, "split_dividend",
        fifelse(
          has_split, "split_only",
          fifelse(
            has_div, "dividend_only",
            "ok"
          )
        )
      )
    )
  )]

  panel[state_dt, on = "symbol", adjustment_state := i.adjustment_state]

  # 5) Final column cleanup ordering
  keep_cols <- c(
    "symbol", "refdate", "asset_type",
    "open_raw", "high_raw", "low_raw", "close_raw",
    "open_adj_split", "high_adj_split", "low_adj_split", "close_adj_split",
    "open_adj_final", "high_adj_final", "low_adj_final", "close_adj_final",
    "turnover", "qty",
    "adjustment_state"
  )

  # Some columns may be missing if upstream had NA open/high/low;
  # preserve everything but move preferred cols to front.
  present_keep <- intersect(keep_cols, names(panel))
  other_cols <- setdiff(names(panel), present_keep)
  data.table::setcolorder(panel, c(present_keep, other_cols))

  if (verbose) {
    af2_log("AF2_ADJ:", "panel_adj rows = ", nrow(panel))
    af2_log("AF2_ADJ:", "symbols = ", length(unique(panel$symbol)))
    af2_log("AF2_ADJ:", "states:")
    print(panel[, .N, by = adjustment_state][order(-N)])
  }

  list(
    panel_adj = panel,
    adjustments = adj_tl,
    events = events
  )
}
