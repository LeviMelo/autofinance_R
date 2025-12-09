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
  if (!nrow(ca)) return(ca)

  dt <- data.table::as.data.table(universe_raw)
  if (!nrow(dt)) return(ca)

  # Required columns
  af2_assert_cols(ca, c("symbol","refdate","action_type","value","source"),
                  name = "corp_actions")
  af2_assert_cols(dt, c("symbol","refdate","close"),
                  name = "universe_raw")

  ca[, symbol := toupper(symbol)]
  dt[, symbol := toupper(symbol)]
  ca[, refdate := as.Date(refdate)]
  dt[, refdate := as.Date(refdate)]

  # Only Yahoo splits
  ys <- ca[action_type == "split" & source == "yahoo" &
             is.finite(value) & value > 0]
  if (!nrow(ys)) return(ca)

  data.table::setorder(dt, symbol, refdate)
  dt[, close_prev := data.table::shift(close, 1L), by = symbol]
  dt[, close_next := data.table::shift(close, -1L), by = symbol]

  ys2 <- merge(
    ys,
    dt[, .(symbol, refdate, close_prev, close, close_next)],
    by = c("symbol","refdate"),
    all.x = TRUE
  )

  # Raw ratios around event day
  ys2[, ratio_prev := fifelse(is.finite(close_prev) & close_prev > 0,
                              close / close_prev, NA_real_)]
  ys2[, ratio_next := fifelse(is.finite(close) & close > 0,
                              close_next / close, NA_real_)]

  # Expected raw jump should match split RATIO
  # But Yahoo value may be ratio OR factor depending on symbol.
  # We test both hypotheses:
  #   H1: expected = value
  #   H2: expected = 1/value
  tol <- as.numeric(cfg$split_gap_tol_log %||% 0.35)

  safe_log <- function(x) {
    ifelse(is.finite(x) & x > 0, log(x), NA_real_)
  }

  ys2[, log_rprev := safe_log(ratio_prev)]
  ys2[, log_rnext := safe_log(ratio_next)]
  ys2[, log_v    := safe_log(value)]
  ys2[, log_inv  := safe_log(1 / value)]

  # Compute best local error for each hypothesis
  ys2[, err_val := pmin(abs(log_rprev - log_v),
                        abs(log_rnext - log_v), na.rm = TRUE)]
  ys2[, err_inv := pmin(abs(log_rprev - log_inv),
                        abs(log_rnext - log_inv), na.rm = TRUE)]

  # Choose orientation that fits raw better
  ys2[, chosen_value := fifelse(err_inv < err_val, 1 / value, value)]
  ys2[, chosen_err   := pmin(err_val, err_inv, na.rm = TRUE)]

  # Mark rows that don't match raw at all
  ys2[, ok := is.finite(chosen_err) & chosen_err <= tol]

  bad <- ys2[!ok]

  if (verbose) {
    af2_log("AF2_CA_PREF:",
            "Split-gap auto-orient: ",
            nrow(ys2) - nrow(bad), " Yahoo split rows kept/normalized; ",
            nrow(bad), " dropped (no raw match).")
  }

  # Apply replacements for GOOD rows
  good <- ys2[ok, .(symbol, refdate, chosen_value)]
  if (nrow(good)) {
    ca[good, on = .(symbol, refdate),
       value := i.chosen_value]
  }

  # Drop BAD rows
  if (nrow(bad)) {
    ca <- ca[!(
      action_type == "split" & source == "yahoo" &
        paste(symbol, refdate) %in% paste(bad$symbol, bad$refdate)
    )]
  }

  ca
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
  if (!is.null(ca) && nrow(ca) &&
      isTRUE(cfg$enable_split_gap_validation)) {

    ca <- af2_ca_fix_yahoo_splits_by_raw_gap(
      corp_actions = ca,
      universe_raw = dt,
      cfg = cfg,
      verbose = verbose
    )
  }

  # 3) Run the normal adjuster builder
  af2_build_panel_adj(
    universe_raw = dt,
    corp_actions = ca,
    manual_events = manual_events,
    cfg = cfg,
    verbose = verbose
  )
}
