# v2/modules/05_screener/R/run_screener.R

# Return modes:
#   return="ranked"   -> current behavior (score + ranks)
#   return="features" -> ONLY the feature table (per-symbol metrics), no scoring

af2_run_screener <- function(panel_adj,
                             config = NULL,
                             allow_unresolved = NULL,
                             return = c("ranked", "features")) {

  af2_require("data.table")
  return <- match.arg(return)

  # If caller didn't specify, default to core config policy
  if (is.null(allow_unresolved)) {
    cfg_core <- af2_get_config()
    allow_unresolved <- isTRUE(cfg_core$allow_unresolved_in_screener)
  }

  cfg <- af2_get_screener_config(config)

  # 0) Validate input contract hard
  af2_validate_screener_input(panel_adj, allow_unresolved = allow_unresolved)

  dt <- data.table::as.data.table(panel_adj)
  dt[, refdate := as.Date(refdate)]

  # unify turnover name
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }

  # 1) Liquidity filter
  liq <- af2_compute_liquidity_from_panel(
    dt,
    min_turnover = cfg$min_turnover,
    min_days_traded_ratio = cfg$min_days_traded_ratio
  )
  if (!nrow(liq)) stop("af2_run_screener: no symbols pass liquidity filter.", call. = FALSE)

  dt <- dt[symbol %in% liq$symbol]
  data.table::setorder(dt, symbol, refdate)

  # 2) Compute metrics per symbol on last lookback window
  metrics_list <- list()
  syms <- unique(dt$symbol)

  for (sym in syms) {
    sdt <- dt[symbol == sym]

    # Ensure we keep enough rows to compute the longest horizon return.
    need_n <- max(
      as.integer(cfg$lookback_days),
      as.integer(max(cfg$horizons_days)) + 1L
    )

    if (nrow(sdt) > need_n) {
      sdt <- sdt[(.N - need_n + 1):.N]
    }

    m <- af2_compute_symbol_metrics(sdt, cfg$horizons_days)
    if (!is.null(m)) {
      # attach asset_type
      m[, asset_type := unique(sdt$asset_type)[1]]
      metrics_list[[sym]] <- m
    }
  }

  metrics <- data.table::rbindlist(metrics_list, fill = TRUE)
  if (!nrow(metrics)) stop("af2_run_screener: metrics computation yielded zero rows.", call. = FALSE)

  # If requested: return ONLY features (no scoring/ranking)
  if (return == "features") {
    return(list(
      features = metrics[order(symbol)],
      liquidity = liq[order(symbol)],
      config = cfg
    ))
  }

  # 3) Score + rank (default behavior)
  out <- af2_score_and_rank(metrics, cfg$score_weights)

  by_type <- split(out, out$asset_type)
  by_type <- lapply(by_type, function(x) x[order(x$rank_type)])

  list(
    full = out[order(rank_overall)],
    by_type = by_type,
    features = metrics[order(symbol)],
    liquidity = liq[order(symbol)],
    config = cfg
  )
}

