# v2/modules/04_adjuster/R/build_panel_adj_selective.R
# High-level wrapper implementing the selective Yahoo "trick".

# ------------------------------------------------------------
# PATCH D2 helper:
# Validate and auto-orient Yahoo split values using Cotahist raw.
# Works even if getSplits() mixes conventions across symbols.
# ------------------------------------------------------------
af2_ca_fix_yahoo_splits_by_raw_gap <- function(corp_actions,
                                               universe_raw,
                                               cfg = NULL,
                                               verbose = TRUE) {
  cfg <- cfg %||% af2_get_config()

  ca <- data.table::as.data.table(corp_actions)
  if (!nrow(ca)) {
    return(list(corp_actions = ca, split_audit = data.table::data.table()))
  }

  dt <- data.table::as.data.table(universe_raw)
  if (!nrow(dt)) {
    return(list(corp_actions = ca, split_audit = data.table::data.table()))
  }

  # Required columns
  af2_assert_cols(ca, c("symbol","refdate","action_type","value","source"), name = "corp_actions")
  af2_assert_cols(dt, c("symbol","refdate","open","close"), name = "universe_raw")

  # Normalize
  ca[, symbol := toupper(trimws(as.character(symbol)))]
  ca[, refdate := as.Date(refdate)]
  ca[, action_type := tolower(trimws(as.character(action_type)))]
  ca[, source := tolower(trimws(as.character(source)))]
  ca[, value := as.numeric(value)]

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, refdate := as.Date(refdate)]

  # Only Yahoo splits with positive numeric values
  ys <- ca[action_type == "split" & source == "yahoo" & is.finite(value) & value > 0,
           .(symbol, vendor_refdate = refdate, yahoo_value = value)]
  if (!nrow(ys)) {
    return(list(corp_actions = ca, split_audit = data.table::data.table()))
  }
  ys[, row_id := .I]

  # Build raw trading-day table + neighbor prices
  raw <- dt[, .(
    symbol,
    refdate,
    open  = suppressWarnings(as.numeric(open)),
    close = suppressWarnings(as.numeric(close))
  )]
  data.table::setorder(raw, symbol, refdate)
  raw[, close_prev := data.table::shift(close, 1L), by = symbol]
  raw[, refdate_prev := data.table::shift(refdate, 1L), by = symbol]
  raw[, close_next := data.table::shift(close, -1L), by = symbol]

  # Non-equi join: snap vendor_refdate to the first B3 trading day >= vendor_refdate
  data.table::setkey(raw, symbol, refdate)
  snapped <- raw[ys, on = .(symbol, refdate >= vendor_refdate), mult = "first", nomatch = NA]

  # Config knobs
  tol <- as.numeric(cfg$split_gap_tol_log %||% 0.35)
  if (!is.finite(tol) || tol <= 0) tol <- 0.35

  max_fwd <- as.integer(cfg$split_gap_max_forward_days %||% 5L)
  if (!is.finite(max_fwd) || max_fwd < 0L) max_fwd <- 5L

  use_open <- isTRUE(cfg$split_gap_use_open)

  safe_log <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)

  # Observed raw ratios around the snapped effective date
  # We treat the observable as POST/PRE (a price-factor-like ratio):
  # - split 2-for-1 -> ~0.5
  # - reverse 1-for-10 -> ~10
  snapped[, eff_refdate := refdate]
  snapped[, lag_days := as.integer(eff_refdate - vendor_refdate)]

  snapped[, ratio_close := ifelse(
    is.finite(close_prev) & close_prev > 0 & is.finite(close) & close > 0,
    close / close_prev, NA_real_
  )]

  snapped[, ratio_open := ifelse(
    use_open &
      is.finite(close_prev) & close_prev > 0 &
      is.finite(open) & open > 0,
    open / close_prev, NA_real_
  )]

  snapped[, ratio_next := ifelse(
    is.finite(close) & close > 0 &
      is.finite(close_next) & close_next > 0,
    close_next / close, NA_real_
  )]

  snapped[, log_v   := safe_log(yahoo_value)]
  snapped[, log_inv := safe_log(1 / yahoo_value)]

  # Hypothesis errors:
  # H_val: Yahoo value already matches observed ratio
  # H_inv: 1/Yahoo matches observed ratio
  snapped[, e1v := abs(safe_log(ratio_open)  - log_v)]
  snapped[, e2v := abs(safe_log(ratio_close) - log_v)]
  snapped[, e3v := abs(safe_log(ratio_next)  - log_v)]

  snapped[, e1i := abs(safe_log(ratio_open)  - log_inv)]
  snapped[, e2i := abs(safe_log(ratio_close) - log_inv)]
  snapped[, e3i := abs(safe_log(ratio_next)  - log_inv)]

  snapped[, err_val := pmin(e1v, e2v, e3v, na.rm = TRUE)]
  snapped[!is.finite(err_val), err_val := NA_real_]

  snapped[, err_inv := pmin(e1i, e2i, e3i, na.rm = TRUE)]
  snapped[!is.finite(err_inv), err_inv := NA_real_]

  # Choose orientation -> ALWAYS normalize to "price factor" space
  snapped[, chosen_as := data.table::fifelse(
    !is.na(err_inv) & (is.na(err_val) | err_inv < err_val),
    "inverse",
    "as_is"
  )]

  snapped[, chosen_value := data.table::fifelse(
    chosen_as == "inverse",
    1 / yahoo_value,
    yahoo_value
  )]

  snapped[, chosen_err := pmin(err_val, err_inv, na.rm = TRUE)]
  snapped[!is.finite(chosen_err), chosen_err := NA_real_]

  # Decide status:
  # - unverified if we cannot safely compute a raw ratio in this universe_raw window
  # - kept if it matches raw within tol
  # - dropped if raw exists but contradicts
  snapped[, status := "unverified_no_raw_window"]

  can_check <- !is.na(eff_refdate) &
    !is.na(refdate_prev) &
    is.finite(close_prev) & close_prev > 0 &
    !is.na(chosen_err) &
    (is.na(lag_days) | lag_days <= max_fwd)

  snapped[can_check == TRUE & chosen_err <= tol, status := "kept_oriented"]
  snapped[can_check == TRUE & chosen_err >  tol, status := "dropped_conflict_raw_gap"]

  # Audit table
  audit <- snapped[, .(
    symbol,
    vendor_refdate,
    eff_refdate,
    pre_refdate = refdate_prev,
    yahoo_value,
    chosen_value,
    chosen_as,
    chosen_err,
    tol_log = tol,
    lag_days,
    ratio_open,
    ratio_close,
    ratio_next,
    status
  )]

  # Apply updates to corp_actions:
  # - For kept: snap refdate -> eff_refdate (if present) and set value -> chosen_value
  # - For dropped: remove the Yahoo split row
  kept <- audit[status == "kept_oriented" & !is.na(eff_refdate),
                .(symbol, refdate_old = vendor_refdate, refdate_new = eff_refdate, value_new = chosen_value)]

  if (nrow(kept)) {
    ca[kept,
       on = .(symbol, refdate = refdate_old, action_type = "split", source = "yahoo"),
       `:=`(refdate = i.refdate_new, value = i.value_new)]
  }

  dropped <- audit[status == "dropped_conflict_raw_gap",
                   .(symbol, refdate_old = vendor_refdate)]
  if (nrow(dropped)) {
    ca <- ca[!dropped, on = .(symbol, refdate = refdate_old, action_type = "split", source = "yahoo")]
  }

  if (verbose) {
    n_keep <- sum(audit$status == "kept_oriented", na.rm = TRUE)
    n_drop <- sum(audit$status == "dropped_conflict_raw_gap", na.rm = TRUE)
    n_unv  <- sum(audit$status == "unverified_no_raw_window", na.rm = TRUE)

    af2_log("AF2_CA_PREF:",
            "Split-gap validation: kept=", n_keep,
            " dropped=", n_drop,
            " unverified=", n_unv,
            " (tol_log=", tol, ", max_fwd_days=", max_fwd, ", use_open=", use_open, ")")
  }

  list(corp_actions = ca, split_audit = audit)
}

af2_build_panel_adj_selective <- function(universe_raw,
                                          manual_events = NULL,
                                          cfg = NULL,
                                          from_ca = "2018-01-01",
                                          to_ca = Sys.Date(),
                                          verbose = TRUE,
                                          use_cache = TRUE,
                                          force_refresh = FALSE,
                                          n_workers = 1L,
                                          force_symbols = NULL
                                          ) {

  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)
  af2_assert_cols(
    dt,
    c("symbol", "refdate", "open", "high", "low", "close", "turnover", "qty", "asset_type"),
    name = "universe_raw"
  )

  # 1) Decide candidate symbols for Yahoo actions
  if (isTRUE(cfg$enable_selective_actions)) {
    cand <- af2_ca_select_candidates(
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )
  } else {
    cand <- sort(unique(toupper(dt$symbol)))
  }

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Selective actions enabled=", isTRUE(cfg$enable_selective_actions))
    af2_log("AF2_CA_PREF:", "Yahoo candidate symbols=", length(cand))
  }

  # -------------------------------
  # PATCH: allow explicit forced symbols
  # -------------------------------
  if (!is.null(force_symbols)) {
    force_symbols <- toupper(trimws(as.character(force_symbols)))
    force_symbols <- force_symbols[!is.na(force_symbols) & nzchar(force_symbols)]
    cand <- sort(unique(c(cand, force_symbols)))
  }

  # 2) Fetch registry ONLY for candidates
  ca <- NULL
  if (length(cand)) {
    ca <- af2_ca_build_registry(
      symbols = cand,
      asset_types = NULL,
      cfg = cfg,
      from = from_ca,
      to = to_ca,
      verbose = verbose,
      use_cache = use_cache,
      force_refresh = force_refresh,
      n_workers = n_workers,
      cache_mode = cfg$ca_cache_mode %||% "batch"
    )
  }

  # -------------------------------
  # PATCH D2: reconcile Yahoo split conventions
  # against Cotahist raw gaps (auto-orient + drop)
  # -------------------------------
  split_audit <- NULL

  if (!is.null(ca) && nrow(ca) &&
      isTRUE(cfg$enable_split_gap_validation)) {

    tmp <- af2_ca_fix_yahoo_splits_by_raw_gap(
      corp_actions = ca,
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )

    ca <- tmp$corp_actions
    split_audit <- tmp$split_audit
  }

  # 3) Run the normal adjuster builder
  out <- af2_build_panel_adj(
    universe_raw = dt,
    corp_actions = ca,
    manual_events = manual_events,
    cfg = cfg,
    verbose = verbose
  )

  # Attach audit (can be NULL if validation disabled)
  out$split_audit <- split_audit
  out
}
