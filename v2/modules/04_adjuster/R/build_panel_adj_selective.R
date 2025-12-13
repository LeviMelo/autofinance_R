# v2/modules/04_adjuster/R/build_panel_adj_selective.R
# High-level wrapper implementing the selective Yahoo "trick".

# ------------------------------------------------------------
# Split-gap validator + snapper for Yahoo splits
# - snaps split refdate forward to next trading day (<= max_fwd_days)
# - normalizes Yahoo split "value" orientation (ratio vs price-factor)
# - rejects splits that do not match raw jump (within tol_log)
# - returns BOTH: cleaned corp_actions + split_audit table
# ------------------------------------------------------------
af2_ca_fix_yahoo_splits_by_raw_gap <- function(dt_prices,
                                              splits_dt,
                                              cfg = NULL,
                                              tol_log = NULL,
                                              max_fwd_days = NULL,
                                              max_back_days = NULL,
                                              use_open = NULL) {

  cfg <- cfg %||% af2_get_config()
  af2_require("data.table")

  tol_log <- as.numeric(tol_log %||% cfg$split_gap_tol_log %||% 0.35)
  if (!is.finite(tol_log) || tol_log <= 0) tol_log <- 0.35

  max_fwd_days <- as.integer(max_fwd_days %||% cfg$split_gap_max_forward_days %||% 5L)
  if (!is.finite(max_fwd_days) || max_fwd_days < 0L) max_fwd_days <- 5L

  max_back_days <- as.integer(max_back_days %||% cfg$split_gap_max_back_days %||% 3L)
  if (!is.finite(max_back_days) || max_back_days < 0L) max_back_days <- 3L

  use_open <- isTRUE(use_open %||% cfg$split_gap_use_open %||% TRUE)

  dt <- data.table::as.data.table(dt_prices)
  sp <- data.table::as.data.table(splits_dt)

  if (!nrow(sp)) {
    return(list(fixed_kept = sp[0], quarantine = sp[0], audit = sp[0]))
  }

  # Expect dt has symbol, refdate, close, and optionally open
  data.table::setorder(dt, symbol, refdate)
  dt[, close_lag := data.table::shift(close, 1L), by = symbol]

  # Add row_id to preserve identity
  sp <- data.table::copy(sp)
  if (!"row_id" %in% names(sp)) sp[, row_id := .I]

  sp[, vendor_refdate := as.Date(refdate)]
  sp[, value := as.numeric(value)]
  sp <- sp[is.finite(value) & value > 0]

  # Candidate offsets: back..forward
  offs <- seq.int(-max_back_days, max_fwd_days)

  # For each split row, evaluate candidate effective dates + both orientations
  eval_one <- function(sym, vdate, vval) {
    best <- list(err = Inf, eff = as.Date(NA), chosen = NA_real_)
    for (o in offs) {
      d <- vdate + o
      row <- dt[symbol == sym & refdate == d]
      if (!nrow(row)) next

      # observed ratio at effective day
      if (use_open && "open" %in% names(row) && is.finite(row$open) && is.finite(row$close_lag) && row$open > 0 && row$close_lag > 0) {
        obs <- row$open / row$close_lag
      } else if (is.finite(row$close) && is.finite(row$close_lag) && row$close > 0 && row$close_lag > 0) {
        obs <- row$close / row$close_lag
      } else {
        next
      }

      # test value and inverse(value)
      cand_vals <- c(vval, 1 / vval)
      for (cv in cand_vals) {
        if (!is.finite(cv) || cv <= 0) next
        e <- abs(log(obs) - log(cv))
        if (is.finite(e) && e < best$err) {
          best$err <- e
          best$eff <- d
          best$chosen <- cv
        }
      }
    }
    best
  }

  out <- sp[, {
    b <- eval_one(symbol, vendor_refdate, value)
    lag_days <- as.integer(b$eff - vendor_refdate)
    status <- if (!is.finite(b$err) || is.infinite(b$err) || is.na(b$eff)) "unverified" else if (b$err <= tol_log) "kept" else "rejected"
    .(
      eff_refdate = as.Date(b$eff),
      chosen_value = as.numeric(b$chosen),
      chosen_err = as.numeric(b$err),
      lag_days = as.integer(ifelse(is.na(lag_days), NA_integer_, lag_days)),
      status = status
    )
  }, by = .(row_id, symbol, yahoo_symbol, vendor_refdate, refdate, action_type, value, source)]

  # Dedup: if multiple rows collapse to same (symbol, eff_refdate, chosen_value), keep best err
  data.table::setorder(out, symbol, eff_refdate, chosen_value, chosen_err)
  out[, dup_rank := seq_len(.N), by = .(symbol, eff_refdate, chosen_value)]
  dup <- out[dup_rank > 1L & status == "kept"]
  if (nrow(dup)) {
    out[dup_rank > 1L & status == "kept", status := "dup"]
  }
  out[, dup_rank := NULL]

  fixed_kept <- out[status == "kept",
                    .(yahoo_symbol, refdate = eff_refdate, action_type, value = chosen_value, source, symbol)]

  quarantine <- out[status != "kept",
                    .(row_id, symbol, yahoo_symbol, refdate = vendor_refdate, action_type, value, source,
                      status, eff_refdate, chosen_value, chosen_err, lag_days, vendor_refdate)]

  audit <- out[, .(row_id, symbol, yahoo_symbol,
                   vendor_refdate, eff_refdate, lag_days,
                   yahoo_value = value,
                   chosen_value, chosen_err,
                   status)]

  list(
    fixed_kept = fixed_kept,
    quarantine = quarantine,
    audit = audit
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
