# v2/modules/04_adjuster/R/build_panel_adj_selective.R
# High-level wrapper implementing the selective Yahoo "trick".

# ------------------------------------------------------------
# Split-gap validator + snapper for Yahoo splits
# - snaps split refdate forward to next trading day (<= max_fwd_days)
# - normalizes Yahoo split "value" orientation (ratio vs price-factor)
# - rejects splits that do not match raw jump (within tol_log)
# - returns BOTH: cleaned corp_actions + split_audit table
# ------------------------------------------------------------
af2_ca_fix_yahoo_splits_by_raw_gap <- function(corp_actions,
                                               universe_raw,
                                               cfg = NULL,
                                               verbose = TRUE) {

  cfg <- cfg %||% af2_get_config()

  ca <- data.table::as.data.table(corp_actions)
  dt <- data.table::as.data.table(universe_raw)

  if (!nrow(ca) || !nrow(dt)) {
    return(list(
      corp_actions = ca,
      split_audit = data.table::data.table()[0],
      yahoo_splits_fixed = data.table::data.table()[0],
      yahoo_splits_quarantine = data.table::data.table()[0]
    ))
  }

  af2_assert_cols(ca, c("symbol","refdate","action_type","value","source"), name = "corp_actions")
  af2_assert_cols(dt, c("symbol","refdate","open","close"), name = "universe_raw")

  # Normalize
  ca[, symbol := toupper(trimws(as.character(symbol)))]
  ca[, action_type := tolower(trimws(as.character(action_type)))]
  ca[, source := tolower(trimws(as.character(source)))]
  ca[, refdate := as.Date(refdate)]
  ca[, value := as.numeric(value)]

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, refdate := as.Date(refdate)]
  dt[, open := as.numeric(open)]
  dt[, close := as.numeric(close)]

  # Only Yahoo split rows are validated/snapped
  ys <- ca[
    action_type == "split" &
      source == "yahoo" &
      is.finite(value) & value > 0 &
      !is.na(refdate)
  ]

  if (!nrow(ys)) {
    return(list(
      corp_actions = ca,
      split_audit = data.table::data.table()[0],
      yahoo_splits_fixed = data.table::data.table()[0],
      yahoo_splits_quarantine = data.table::data.table()[0]
    ))
  }

  # Knobs
  tol <- as.numeric(cfg$split_gap_tol_log %||% 0.35)
  if (!is.finite(tol) || tol <= 0) tol <- 0.35

  max_fwd <- as.integer(cfg$split_gap_max_forward_days %||% 0L)
  if (!is.finite(max_fwd) || max_fwd < 0L) max_fwd <- 0L

  use_open <- isTRUE(cfg$split_gap_use_open %||% FALSE)

  # Prep raw grid
  data.table::setorder(dt, symbol, refdate)
  data.table::setkey(dt, symbol, refdate)
  dt[, close_prev := data.table::shift(close, 1L), by = symbol]

  # Stable row id for Yahoo split rows
  ys_key <- ys[, .(
    row_id = .I,
    symbol,
    vendor_refdate = refdate,
    yahoo_value = value,
    yahoo_symbol = if ("yahoo_symbol" %in% names(ys)) as.character(yahoo_symbol) else NA_character_
  )]
  data.table::setkey(ys_key, symbol, vendor_refdate)

  # Snap vendor_refdate -> next trading day within <= max_fwd days
  snapped <- dt[ys_key,
    on      = .(symbol, refdate = vendor_refdate),
    roll    = -max_fwd,
    nomatch = NA,
    .(
      row_id         = i.row_id,
      symbol         = i.symbol,
      yahoo_symbol   = i.yahoo_symbol,
      vendor_refdate = i.vendor_refdate,
      eff_refdate    = refdate,
      lag_days       = as.integer(refdate - i.vendor_refdate),
      open           = open,
      close          = close,
      close_prev     = close_prev,
      yahoo_value    = i.yahoo_value
    )
  ]

  safe_log <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)

  # Observed raw ratios (post/pre)
  snapped[, ratio_cc := data.table::fifelse(
    is.finite(close) & is.finite(close_prev) & close_prev > 0,
    close / close_prev,
    NA_real_
  )]

  snapped[, ratio_oc := data.table::fifelse(
    use_open & is.finite(open) & is.finite(close_prev) & close_prev > 0,
    open / close_prev,
    NA_real_
  )]

  # Two hypotheses:
  # - yahoo_value is already PRICE-FACTOR (e.g. 0.2)
  # - 1/yahoo_value is PRICE-FACTOR (vendor gave ratio like 5)
  snapped[, log_v   := safe_log(yahoo_value)]
  snapped[, log_inv := safe_log(1 / yahoo_value)]
  snapped[, log_cc  := safe_log(ratio_cc)]
  snapped[, log_oc  := safe_log(ratio_oc)]

  snapped[, err_val := abs(log_cc - log_v)]
  if (use_open) snapped[, err_val := pmin(err_val, abs(log_oc - log_v), na.rm = TRUE)]

  snapped[, err_inv := abs(log_cc - log_inv)]
  if (use_open) snapped[, err_inv := pmin(err_inv, abs(log_oc - log_inv), na.rm = TRUE)]

  snapped[, chosen_value := data.table::fifelse(err_inv < err_val, 1 / yahoo_value, yahoo_value)]
  snapped[, chosen_err   := pmin(err_val, err_inv, na.rm = TRUE)]

  snapped[, has_prices := is.finite(chosen_err) & !is.na(chosen_err)]
  snapped[, is_snapped := !is.na(eff_refdate) & is.finite(lag_days) & lag_days >= 0 & lag_days <= max_fwd]

  snapped[, status := data.table::fifelse(
    !is_snapped | !has_prices, "unverified",
    data.table::fifelse(chosen_err <= tol, "kept", "rejected")
  )]

  split_audit <- snapped[, .(
    row_id,
    symbol, yahoo_symbol,
    vendor_refdate, eff_refdate, lag_days,
    yahoo_value,
    chosen_value,
    chosen_err,
    status
  )]

  # Build an APPLY table = only "kept" rows, with snapped refdate + oriented value
  yahoo_splits_fixed <- split_audit[
    status == "kept" & !is.na(eff_refdate) & is.finite(chosen_value) & chosen_value > 0,
    .(
      symbol = symbol,
      yahoo_symbol = yahoo_symbol,
      refdate = eff_refdate,
      action_type = "split",
      value = chosen_value,
      source = "yahoo"
    )
  ]

  # Quarantine = original vendor rows, annotated with audit fields
  ys_orig <- ys[, .(
    row_id = .I,
    symbol,
    yahoo_symbol = if ("yahoo_symbol" %in% names(ys)) as.character(yahoo_symbol) else NA_character_,
    refdate,
    action_type,
    value,
    source
  )]

  yahoo_splits_quarantine <- merge(
    ys_orig,
    split_audit[, .(row_id, status, eff_refdate, chosen_value, chosen_err, lag_days, vendor_refdate)],
    by = "row_id",
    all.x = TRUE
  )
  yahoo_splits_quarantine <- yahoo_splits_quarantine[status != "kept" | is.na(status)]

  if (verbose) {
    af2_log(
      "AF2_CA_PREF:",
      "Split-gap validation: total=", nrow(split_audit),
      " kept=", sum(split_audit$status == "kept"),
      " rejected=", sum(split_audit$status == "rejected"),
      " unverified=", sum(split_audit$status == "unverified"),
      " (tol_log=", tol,
      ", max_fwd_days=", max_fwd,
      ", use_open=", use_open, ")"
    )
  }

  list(
    corp_actions = ca,                    # vendor registry untouched (audit)
    split_audit = split_audit,
    yahoo_splits_fixed = yahoo_splits_fixed,
    yahoo_splits_quarantine = yahoo_splits_quarantine
  )
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

  ca_vendor_raw <- if (!is.null(ca)) data.table::copy(ca) else ca

  # -------------------------------
  # PATCH D2: reconcile Yahoo split conventions
  # against Cotahist raw gaps (auto-orient + drop)
  # -------------------------------
  split_audit <- NULL
  yahoo_splits_fixed <- NULL
  yahoo_splits_quarantine <- NULL
  
  if (!is.null(ca) && nrow(ca) &&
      isTRUE(cfg$enable_split_gap_validation)) {
  
    fix <- af2_ca_fix_yahoo_splits_by_raw_gap(
      corp_actions = ca,
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )
  
    # Keep vendor registry intact for audit/logging
    ca <- fix$corp_actions
  
    # These are the important outputs
    split_audit <- fix$split_audit
    yahoo_splits_fixed <- fix$yahoo_splits_fixed
    yahoo_splits_quarantine <- fix$yahoo_splits_quarantine
  }
  
  # (optional but highly recommended sanity log)
  if (verbose) {
    if (is.null(ca) || !nrow(ca)) {
      af2_log("AF2_CA_PREF:", "corp_actions AFTER validation is EMPTY -> panel will be unadjusted.")
    } else {
      af2_log("AF2_CA_PREF:", "corp_actions AFTER validation rows= ", nrow(ca))
      print(ca[, .N, by = .(action_type, source)][order(-N)])
    }
  }

  # ------------------------------------------------------------
  # APPLY POLICY:
  # - remove all Yahoo split rows from the vendor registry
  # - add back only the "fixed kept" Yahoo splits (snapped + oriented)
  # - quarantine everything else for inspection
  # ------------------------------------------------------------
  ca_apply <- ca
  ca_quarantine <- NULL
  
  if (!is.null(ca_apply) && nrow(ca_apply)) {
  
    # Drop vendor Yahoo splits (we never apply them directly)
    ca_apply <- ca_apply[!(action_type == "split" & source == "yahoo")]
  
    # Add back only validated+fixed Yahoo splits
    if (!is.null(yahoo_splits_fixed) && nrow(yahoo_splits_fixed)) {
      ca_apply <- data.table::rbindlist(
        list(ca_apply, yahoo_splits_fixed),
        use.names = TRUE, fill = TRUE
      )
    }
  
    ca_apply <- unique(ca_apply)
    ca_quarantine <- yahoo_splits_quarantine
  
    if (verbose) {
      af2_log(
        "AF2_CA_PREF:",
        "Yahoo split apply policy: fixed_kept=",
        if (is.null(yahoo_splits_fixed)) 0L else nrow(yahoo_splits_fixed),
        " quarantine=",
        if (is.null(ca_quarantine)) 0L else nrow(ca_quarantine)
      )
    }
  }

  # 3) Run the normal adjuster builder
  out <- af2_build_panel_adj(
    universe_raw = dt,
    corp_actions = ca_apply,
    manual_events = manual_events,
    cfg = cfg,
    verbose = verbose
  )

  # Attach audit + quarantine (can be NULL if validation disabled)
  out$split_audit <- split_audit
  out$corp_actions_apply <- ca_apply
  out$corp_actions_quarantine <- ca_quarantine
  out
}
