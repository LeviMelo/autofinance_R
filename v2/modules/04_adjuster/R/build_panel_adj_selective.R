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
                                               verbose = TRUE,
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

  # 1) Normalize Inputs
  dt <- data.table::as.data.table(universe_raw)
  sp <- data.table::as.data.table(corp_actions)

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, refdate := as.Date(refdate)]
  dt[, `:=`(open = as.numeric(open), close = as.numeric(close))]

  sp[, symbol := toupper(trimws(as.character(symbol)))]
  sp[, refdate := as.Date(refdate)]
  sp[, action_type := tolower(trimws(as.character(action_type)))]
  sp[, source := tolower(trimws(as.character(source)))]
  sp[, value := as.numeric(value)]
  if (!"yahoo_symbol" %in% names(sp)) sp[, yahoo_symbol := NA_character_]

  # 2) Filter target splits
  sp0 <- data.table::copy(sp) 
  if (!"row_id" %in% names(sp0)) sp0[, row_id := .I] 

  to_validate <- sp0[action_type == "split" & source == "yahoo" & is.finite(value) & value > 0]

  # Empty return structure
  empty_audit <- data.table::data.table(
      row_id=integer(), symbol=character(), yahoo_symbol=character(),
      vendor_refdate=as.Date(character()), eff_refdate=as.Date(character()), 
      lag_days=integer(), yahoo_value=numeric(), chosen_value=numeric(), 
      chosen_err=numeric(), status=character()
  )

  if (!nrow(to_validate)) {
    return(list(
      corp_actions = sp0,
      fixed_kept = sp[0], 
      quarantine = data.table::copy(empty_audit),
      audit = empty_audit
    ))
  }

  # 3) Prepare Universe for Keyed Lookup
  # Enforce unique keys for validation logic
  data.table::setorder(dt, symbol, refdate)
  dt <- dt[, .SD[1L], by = .(symbol, refdate)]
  
  # Set Key for fast lookups
  data.table::setkey(dt, symbol, refdate)
  
  # Compute lag (requires stable ordering, which we just did)
  dt[, close_lag := data.table::shift(close, 1L), by = symbol]

  # 4) Validation Logic
  offs <- seq.int(-max_back_days, max_fwd_days)

  eval_one <- function(sym, vdate, vval) {
    best <- list(err = Inf, eff = as.Date(NA), chosen = NA_real_)
    for (o in offs) {
      d <- vdate + o
      
      row <- dt[.(sym, d), nomatch = 0L]
      if (nrow(row) != 1L) next
      
      cl <- row$close[[1]]
      op <- row$open[[1]]
      lag <- row$close_lag[[1]]
      
      # Need lag and either open or close
      if (!is.finite(lag) || lag <= 0) next
      
      if (use_open && is.finite(op) && op > 0) {
        obs <- op / lag
      } else if (is.finite(cl) && cl > 0) {
        obs <- cl / lag
      } else {
        next
      }

      # observed ratio
      if (use_open && is.finite(row$open) && is.finite(row$close_lag) && row$open > 0 && row$close_lag > 0) {
        obs <- row$open / row$close_lag
      } else if (is.finite(row$close) && is.finite(row$close_lag) && row$close > 0 && row$close_lag > 0) {
        obs <- row$close / row$close_lag
      } else {
        next
      }

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

  out <- to_validate[, {
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
  }, by = .(row_id, symbol, yahoo_symbol, vendor_refdate = as.Date(refdate), action_type, value, source)]

  # Dedup: if multiple rows collapse to same (symbol, eff_refdate, chosen_value), keep best err
  data.table::setorder(out, symbol, eff_refdate, chosen_value, chosen_err)
  out[, dup_rank := seq_len(.N), by = .(symbol, eff_refdate, chosen_value)]
  
  if (nrow(out[dup_rank > 1L & status == "kept"])) {
    out[dup_rank > 1L & status == "kept", status := "dup"]
  }
  out[, dup_rank := NULL]

  fixed_kept <- out[status == "kept",
                    .(yahoo_symbol, refdate = eff_refdate, action_type = "split", value = chosen_value, source, symbol)]

  quarantine <- out[status != "kept",
                    .(row_id, symbol, yahoo_symbol, refdate = vendor_refdate, action_type = "split", value, source,
                      status, eff_refdate, chosen_value, chosen_err, lag_days, vendor_refdate)]

  audit <- out[, .(row_id, symbol, yahoo_symbol,
                   vendor_refdate, eff_refdate, lag_days,
                   yahoo_value = value,
                   chosen_value, chosen_err,
                   status)]

  list(
    corp_actions = sp0,
    fixed_kept = fixed_kept,
    quarantine = quarantine,
    audit = audit
  )
}

af2_build_panel_adj_selective <- function(universe_raw,
                                          manual_events = NULL,
                                          cfg = NULL,
                                          from_ca = NULL,
                                          to_ca = NULL,
                                          verbose = TRUE,
                                          use_cache = TRUE,
                                          force_refresh = FALSE,
                                          n_workers = 1L,
                                          force_symbols = NULL) {

  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)
  
  # 1) Assert columns FIRST
  af2_assert_cols(
    dt,
    c("symbol", "refdate", "open", "high", "low", "close", "turnover", "qty", "asset_type"),
    name = "universe_raw"
  )

  # 2) Normalize keys & Dedupe (CRITICAL FIX)
  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, asset_type := tolower(trimws(as.character(asset_type)))]
  dt[, refdate := as.Date(refdate)]
  dt[, `:=`(
      open = as.numeric(open),
      high = as.numeric(high),
      low  = as.numeric(low),
      close = as.numeric(close),
      turnover = as.numeric(turnover),
      qty = as.numeric(qty)
  )]

  # Defensive dedupe
  data.table::setorder(dt, asset_type, symbol, refdate)
  dup_check <- dt[, .N, by = .(symbol, refdate)][N > 1L]
  if (nrow(dup_check)) {
    if (verbose) af2_log("AF2_ADJ:", "WARNING: universe_raw had duplicates. Keeping first row per key.")
    dt <- dt[, .SD[1L], by = .(symbol, refdate)]
  }
  af2_assert_no_dupes(dt, c("symbol", "refdate"), name = "universe_raw(selective)")

  # 3) Dynamic Date Defaults
  if (is.null(from_ca) || is.na(from_ca)) {
    from_ca <- min(dt$refdate, na.rm = TRUE) - 10
  }
  if (is.null(to_ca) || is.na(to_ca)) {
    to_ca <- max(dt$refdate, na.rm = TRUE) + 10
  }
  from_ca <- as.Date(from_ca)
  to_ca <- as.Date(to_ca)

  # 4) Decide candidate symbols
  if (isTRUE(cfg$enable_selective_actions)) {
    cand <- af2_ca_select_candidates(universe_raw = dt, cfg = cfg, verbose = verbose)
  } else {
    cand <- sort(unique(toupper(dt$symbol)))
  }

  if (!is.null(force_symbols)) {
    force_symbols <- toupper(trimws(as.character(force_symbols)))
    force_symbols <- force_symbols[!is.na(force_symbols) & nzchar(force_symbols)]
    cand <- sort(unique(c(cand, force_symbols)))
  }

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Selective actions enabled=", isTRUE(cfg$enable_selective_actions))
    af2_log("AF2_CA_PREF:", "Yahoo candidate symbols=", length(cand))
  }

  # 5) Fetch registry
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

  # 6) Reconcile/Fix Splits
  split_audit <- data.table::data.table()
  yahoo_splits_fixed <- data.table::data.table()
  yahoo_splits_quarantine <- data.table::data.table()

  if (!is.null(ca) && nrow(ca) && isTRUE(cfg$enable_split_gap_validation)) {
    fix <- af2_ca_fix_yahoo_splits_by_raw_gap(
      corp_actions = ca,
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )
    ca <- fix$corp_actions
    split_audit <- fix$audit
    yahoo_splits_fixed <- fix$fixed_kept
    yahoo_splits_quarantine <- fix$quarantine
  }

  # 7) Apply Policy (Fail-Soft)
  ca_apply <- if (!is.null(ca) && nrow(ca)) data.table::copy(ca) else data.table::data.table()
  ca_quarantine <- data.table::data.table()

  if (nrow(ca_apply)) {
    # Normalize CA fields
    ca_apply[, symbol := toupper(trimws(as.character(symbol)))]
    ca_apply[, refdate := as.Date(refdate)]
    ca_apply[, action_type := tolower(trimws(as.character(action_type)))]
    ca_apply[, source := tolower(trimws(as.character(source)))]
    ca_apply[, value := as.numeric(value)]

    # Separate non-yahoo-splits
    base_no_vendor <- ca_apply[!(action_type == "split" & source == "yahoo")]

    # Build Fail-Soft Splits from Audit
    splits_apply <- data.table::data.table()
    
    if (nrow(split_audit)) {
        # Use KEPT or UNVERIFIED (Fail Soft)
        sa <- data.table::as.data.table(split_audit)
        sa_apply <- sa[status %in% c("kept", "unverified")]
        
        if (nrow(sa_apply)) {
            sa_apply[, refdate_apply := data.table::fifelse(!is.na(eff_refdate), eff_refdate, vendor_refdate)]
            sa_apply[, value_apply := data.table::fifelse(status == "kept" & is.finite(chosen_value), chosen_value, yahoo_value)]
            
            splits_apply <- sa_apply[
                !is.na(refdate_apply) & is.finite(value_apply) & value_apply > 0,
                .(symbol, yahoo_symbol, refdate = as.Date(refdate_apply), action_type = "split", value = as.numeric(value_apply), source = "yahoo")
            ]
            splits_apply <- unique(splits_apply, by = c("symbol", "refdate", "action_type", "value", "source"))
        }
    } else {
        # No audit? Keep original vendor splits as last resort
        splits_apply <- ca_apply[action_type == "split" & source == "yahoo"]
    }

    # Recombine
    ca_apply <- data.table::rbindlist(list(base_no_vendor, splits_apply), use.names = TRUE, fill = TRUE)
    ca_apply <- unique(ca_apply, by = c("symbol", "refdate", "action_type", "value", "source"))
    if ("row_id" %in% names(ca_apply)) ca_apply[, row_id := NULL]
    if (!is.null(ca) && "row_id" %in% names(ca)) ca[, row_id := NULL]

    if (nrow(yahoo_splits_quarantine)) ca_quarantine <- yahoo_splits_quarantine
    
    if (verbose) {
      af2_log("AF2_CA_PREF:", "Applied splits count=", nrow(splits_apply), " Quarantine=", nrow(ca_quarantine))
    }
  }

  # 8) Run Adjuster
  out <- af2_build_panel_adj(
    universe_raw = dt,
    corp_actions = ca_apply,
    manual_events = manual_events,
    cfg = cfg,
    verbose = verbose
  )

  out$split_audit <- split_audit
  out$corp_actions_apply <- ca_apply
  out$corp_actions_quarantine <- ca_quarantine
  out
}