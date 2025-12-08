# v2/modules/05_screener/R/run_screener.R

af2_run_screener <- function(panel_adj,
                             config = NULL,
                             allow_unresolved = FALSE) {

  af2_require("data.table")

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
    if (nrow(sdt) > cfg$lookback_days) {
      sdt <- sdt[(.N - cfg$lookback_days + 1):.N]
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

  # 3) Score + rank
  out <- af2_score_and_rank(metrics, cfg$score_weights)

  by_type <- split(out, out$asset_type)
  by_type <- lapply(by_type, function(x) x[order(x$rank_type)])

  list(
    full = out[order(rank_overall)],
    by_type = by_type
  )

}
