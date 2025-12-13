# v2/modules/04_adjuster/R/build_panel_adj.R

# High-level builder:
#   universe_raw (Module 01 output)
#   corp_actions (Module 03 output)
#   manual_events (optional)
#
# Returns:
#   list(panel_adj = ..., adjustments = ..., events = ..., residual_jump_audit = ...)

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

  # ------------------------------------------------------------------
  # Defensive dedupe (rb3 edge cases)
  # ------------------------------------------------------------------
  data.table::setorder(dt, asset_type, symbol, refdate)

  dup_check <- dt[, .N, by = .(symbol, refdate)][N > 1L]
  if (nrow(dup_check)) {
    af2_log(
      "AF2_ADJ:",
      "WARNING: universe_raw had duplicated symbol/refdate rows. Keeping first row per key."
    )
    dt <- dt[, .SD[1L], by = .(symbol, refdate)]
  }

  af2_assert_no_dupes(dt, c("symbol", "refdate"), name = "universe_raw")

  # 1) Build event table (split_value + div_cash per symbol-date)
  events <- af2_adj_build_events(
    corp_actions = if (isTRUE(cfg$enable_splits)) corp_actions else NULL,
    manual_events = if (isTRUE(cfg$enable_manual_events)) manual_events else NULL,
    cfg = cfg,
    verbose = verbose
  )

  # 2) Apply adjustments (Now Vectorized & Fast)
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

  # 4) Compute adjustment_state per symbol (Vectorized)
  #    Using adj_tl to detect issues and events to detect types
  
  # Aggregate flags from adjustments timeline
  # This is much faster than subsetting events iteratively
  issue_flags <- adj_tl[, .(
    issue_div_any = any(isTRUE(issue_div))
  ), by = symbol]

  # Aggregate flags from events
  # Filter events to relevant window first
  panel_min <- min(panel$refdate, na.rm = TRUE)
  panel_max <- max(panel$refdate, na.rm = TRUE)
  
  ev_in_window <- events[refdate >= panel_min & refdate <= panel_max]
  
  if (nrow(ev_in_window) > 0) {
    ev_flags <- ev_in_window[, .(
      has_split = any(is.finite(split_value) & split_value != 1),
      has_div   = any(is.finite(div_cash) & div_cash > 0),
      has_manual = any(isTRUE(has_manual))
    ), by = symbol]
  } else {
    ev_flags <- data.table::data.table(symbol = character(), has_split=logical(), has_div=logical(), has_manual=logical())
  }

  # Merge all metadata
  # Start with all symbols in panel
  state_dt <- data.table::data.table(symbol = unique(panel$symbol))
  
  state_dt <- merge(state_dt, ev_flags, by = "symbol", all.x = TRUE)
  state_dt <- merge(state_dt, issue_flags, by = "symbol", all.x = TRUE)
  
  # Fill NAs
  state_dt[is.na(has_split), has_split := FALSE]
  state_dt[is.na(has_div), has_div := FALSE]
  state_dt[is.na(has_manual), has_manual := FALSE]
  state_dt[is.na(issue_div_any), issue_div_any := FALSE]

  # Determine State
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

  # ------------------------------------------------------------
  # 4b) Residual jump safety net (The Safety Valve)
  # ------------------------------------------------------------
  jump_tol <- as.numeric(cfg$adj_residual_jump_tol_log %||% 1.0)
  if (!is.finite(jump_tol) || jump_tol <= 0) jump_tol <- 1.0

  # Compute max log-jump on FINAL adjusted close
  jump_audit <- panel[
    is.finite(close_adj_final) & close_adj_final > 0,
    {
      # Diff Log gives log returns
      v <- abs(diff(log(close_adj_final)))
      if (!length(v) || all(!is.finite(v))) {
        .(residual_max_abs_logret = 0, residual_jump_date = as.Date(NA))
      } else {
        k <- which.max(v)
        .(
          residual_max_abs_logret = as.numeric(v[k]),
          residual_jump_date = refdate[k + 1L] # date of the jump
        )
      }
    },
    by = symbol
  ]

  jump_audit[, residual_jump_flag := is.finite(residual_max_abs_logret) & residual_max_abs_logret >= jump_tol]

  # Merge jump info into state_dt
  state_dt <- merge(state_dt, jump_audit, by = "symbol", all.x = TRUE)
  
  # OVERRIDE state if residual jump detected
  state_dt[isTRUE(residual_jump_flag), adjustment_state := "suspect_unresolved"]

  # Apply state back to panel
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
    events = events,
    residual_jump_audit = jump_audit
  )
}