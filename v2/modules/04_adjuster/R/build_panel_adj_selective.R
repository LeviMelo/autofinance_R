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

  if (!nrow(ca)) {
    return(list(corp_actions = ca, split_audit = NULL))
  }
  if (!nrow(dt)) {
    return(list(corp_actions = ca, split_audit = NULL))
  }

  af2_assert_cols(ca, c("symbol","refdate","action_type","value","source"), name = "corp_actions")
  af2_assert_cols(dt, c("symbol","refdate","open","close"), name = "universe_raw")

  # normalize
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
    return(list(corp_actions = ca, split_audit = data.table::data.table()[0]))
  }

  # knobs
  tol <- as.numeric(cfg$split_gap_tol_log %||% 0.35)
  if (!is.finite(tol) || tol <= 0) tol <- 0.35

  max_fwd <- as.integer(cfg$split_gap_max_forward_days %||% 0L)
  if (!is.finite(max_fwd) || max_fwd < 0L) max_fwd <- 0L

  use_open <- isTRUE(cfg$split_gap_use_open %||% FALSE)

  # prep raw grid (keyed) + prev close
  data.table::setorder(dt, symbol, refdate)
  data.table::setkey(dt, symbol, refdate)
  dt[, close_prev := data.table::shift(close, 1L), by = symbol]

  # create a per-row id so audit is stable
  ys_key <- ys[, .(
    row_id = .I,
    symbol,
    vendor_refdate = refdate,
    yahoo_value = value,
    yahoo_symbol = if ("yahoo_symbol" %in% names(ys)) as.character(yahoo_symbol) else NA_character_
  )]
  data.table::setkey(ys_key, symbol, vendor_refdate)

  # snap vendor_refdate -> next trading refdate (<= max_fwd days)
  # roll = -N rolls forward (next observation) up to N days
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

  # observed raw ratios (post/pre)
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

  snapped[, log_v   := safe_log(yahoo_value)]
  snapped[, log_inv := safe_log(1 / yahoo_value)]
  snapped[, log_cc  := safe_log(ratio_cc)]
  snapped[, log_oc  := safe_log(ratio_oc)]

  # errors for two hypotheses:
  # Hval: yahoo_value already equals PRICE RATIO (price factor)
  # Hinv: 1/yahoo_value equals PRICE RATIO (yahoo_value is split "ratio")
  snapped[, err_val := abs(log_cc - log_v)]
  if (use_open) snapped[, err_val := pmin(err_val, abs(log_oc - log_v), na.rm = TRUE)]

  snapped[, err_inv := abs(log_cc - log_inv)]
  if (use_open) snapped[, err_inv := pmin(err_inv, abs(log_oc - log_inv), na.rm = TRUE)]

  snapped[, chosen_value := data.table::fifelse(err_inv < err_val, 1 / yahoo_value, yahoo_value)]
  snapped[, chosen_err   := pmin(err_val, err_inv, na.rm = TRUE)]

  # validity
  snapped[, has_prices := is.finite(chosen_err) & !is.na(chosen_err)]
  snapped[, is_snapped := !is.na(eff_refdate) & is.finite(lag_days) & lag_days >= 0 & lag_days <= max_fwd]

  snapped[, status := data.table::fifelse(
    !is_snapped | !has_prices, "unverified",
    data.table::fifelse(chosen_err <= tol, "kept", "rejected")
  )]

  split_audit <- snapped[, .(
    symbol, yahoo_symbol,
    vendor_refdate, eff_refdate, lag_days,
    yahoo_value,
    chosen_value,
    chosen_err,
    status
  )]

  # apply changes:
  # - kept: snap refdate -> eff_refdate, normalize value -> chosen_value
  # - rejected: drop split row
  # - unverified: keep as-is (do NOT drop; do NOT change)
  kept <- snapped[status == "kept" & is_snapped == TRUE & has_prices == TRUE]
  rej  <- snapped[status == "rejected"]  # keep for audit/log only; NEVER delete vendor rows

  if (verbose) {
    af2_log(
      "AF2_CA_PREF:",
      "Split-gap validation: snapped= ", nrow(snapped),
      "  kept= ", nrow(kept),
      "  rejected= ", nrow(rej),
      "  unverified= ", sum(split_audit$status == "unverified"),
      "  (tol_log= ", tol,
      " , max_fwd_days= ", max_fwd,
      " , use_open= ", use_open, " )"
    )
  }

  list(corp_actions = ca, split_audit = split_audit)
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
  
  if (!is.null(ca) && nrow(ca) &&
      isTRUE(cfg$enable_split_gap_validation)) {
  
    fix <- af2_ca_fix_yahoo_splits_by_raw_gap(
      corp_actions = ca,
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )
  
    ca <- fix$corp_actions
    split_audit <- fix$split_audit
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
  # POLICY: DO NOT auto-apply unverified/rejected Yahoo splits.
  # We keep vendor registry (ca) intact, but pass only "kept"
  # Yahoo split rows into the adjuster.
  # ------------------------------------------------------------
  ca_apply <- ca
  ca_quarantine <- NULL

  if (!is.null(ca_apply) && nrow(ca_apply) &&
      !is.null(split_audit) && nrow(split_audit)) {

    kept_keys <- split_audit[
      status == "kept" & !is.na(eff_refdate),
      .(symbol, refdate = eff_refdate)
    ]

    ys_all <- ca_apply[action_type == "split" & source == "yahoo"]

    ys_kept <- ys_all[kept_keys, on = .(symbol, refdate), nomatch = 0L]
    ca_quarantine <- ys_all[!kept_keys, on = .(symbol, refdate)]

    ca_apply <- data.table::rbindlist(
      list(
        ca_apply[!(action_type == "split" & source == "yahoo")],
        ys_kept
      ),
      use.names = TRUE, fill = TRUE
    )

    ca_apply <- unique(ca_apply)

    if (verbose) {
      af2_log(
        "AF2_CA_PREF:",
        "Yahoo split apply policy: kept=", nrow(ys_kept),
        " quarantine=", if (is.null(ca_quarantine)) 0L else nrow(ca_quarantine)
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
