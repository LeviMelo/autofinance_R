############################################################
# autofinance_backtest_core.R
# Loop temporal: screener + risk + portfolio -> curva de equity
############################################################

af_backtest_config_default <- list(
  rebalance_freq = "monthly",   # "monthly", "quarterly"
  lookback_years = 3L,
  screener_config = af_screener_config_default,
  risk_config     = af_risk_config_default,
  port_config     = af_port_default_config,
  rf_series_id    = "CDI"
)

af_generate_rebalance_dates <- function(start_date,
                                        end_date,
                                        rebalance_freq = "monthly") {
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  dates <- seq(from = start_date, to = end_date, by = "day")
  if (rebalance_freq == "monthly") {
    idx <- !duplicated(format(dates, "%Y-%m"))
  } else if (rebalance_freq == "quarterly") {
    qtr <- paste0(format(dates, "%Y"), "Q", lubridate::quarter(dates))
    idx <- !duplicated(qtr)
  } else {
    stop("Unsupported rebalance_freq.")
  }
  dates[idx]
}

af_backtest <- function(symbols_universe = NULL,
                        start_date,
                        end_date,
                        bt_config = af_backtest_config_default,
                        con = af_db_connect()) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("data.table")

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  # 1) define universo (se NULL, pega assets_meta ativos)
  if (is.null(symbols_universe)) {
    dt_meta <- data.table::as.data.table(
      DBI::dbGetQuery(con, "SELECT symbol FROM assets_meta WHERE active = 1")
    )
    symbols_universe <- dt_meta$symbol
  }

  # 2) constrói painel ajustado para todo período + lookback extra
  global_start <- start_date - 365 * bt_config$lookback_years
  panel <- af_build_adjusted_panel(con, symbols_universe, global_start, end_date)
  panel_ret <- af_compute_returns(panel)

  # 3) RF diário
  rf_series <- NULL
  if (!is.null(bt_config$rf_series_id)) {
    macro_rf <- af_get_macro_series(
      con,
      series_ids = bt_config$rf_series_id,
      start_date = global_start,
      end_date   = end_date
    )
    if (nrow(macro_rf) > 0L) {
      # assume macro_rf$value já em termos de retorno diário ou taxa diária;
      # você ajusta conforme seu SGS (se for %aa, converter).
      rf_series <- data.table::data.table(
        refdate = macro_rf$refdate,
        rf_daily = macro_rf$value
      )
      panel_ret <- rf_series[panel_ret, on = .(refdate = date)]
      panel_ret[is.na(rf_daily), rf_daily := 0]
      panel_ret[, excess_ret_simple := ret_simple - rf_daily]
    } else {
      panel_ret[, excess_ret_simple := ret_simple]
    }
  } else {
    panel_ret[, excess_ret_simple := ret_simple]
  }

  # 4) datas de rebalance
  reb_dates <- af_generate_rebalance_dates(start_date, end_date,
                                           bt_config$rebalance_freq)
  reb_dates <- reb_dates[reb_dates >= start_date & reb_dates <= end_date]

  equity <- data.table::data.table(
    date = seq(from = start_date, to = end_date, by = "day"),
    equity = NA_real_
  )
  equity[1L, equity := 1]  # começa com 1

  current_weights <- NULL
  current_value   <- 1
  trades <- data.table::data.table(
    date = as.Date(character(0)),
    symbol = character(0),
    weight = numeric(0)
  )

  for (i in seq_along(reb_dates)) {
    t_reb <- reb_dates[i]
    if (t_reb <= start_date) next

    # janela de lookback para screener
    # screener usa sua própria lógica de lookback; aqui só passamos data
    sc <- af_run_screener(ref_date = t_reb,
                          config = bt_config$screener_config,
                          con    = con)
    # escolhe top N por tipo ou simplesmente top X global
    metrics <- sc$full[order(rank_overall)]
    # por simplicidade: top 20 global
    selected <- head(metrics$symbol, 20)

    # dados de retornos até t_reb (para risk/portfolio)
    dt_window <- panel_ret[date <= t_reb & symbol %in% selected]

    port_res <- af_build_portfolio(
      panel_returns   = dt_window,
      selected_symbols = selected,
      port_config     = bt_config$port_config,
      metrics_table   = metrics,
      rf_daily_series = NULL,   # já usamos excess_ret_simple
      end_date        = t_reb
    )

    w_t <- port_res$weights
    w_t <- w_t[!is.na(w_t)]

    # registra trades (simplificado: target weights)
    trades <- rbind(
      trades,
      data.table::data.table(
        date   = t_reb,
        symbol = names(w_t),
        weight = as.numeric(w_t)
      ),
      fill = TRUE
    )

    # aplica retornos até próximo rebalance
    t_next <- if (i < length(reb_dates)) reb_dates[i + 1] else end_date
    dt_horizon <- panel_ret[
      date > t_reb & date <= t_next & symbol %in% names(w_t)
    ]
    if (nrow(dt_horizon) == 0L) next

    data.table::setorder(dt_horizon, date)
    # matriz de retornos por dia x ativo
    ret_wide <- data.table::dcast(
      dt_horizon,
      date ~ symbol,
      value.var = "excess_ret_simple"
    )
    dates_h <- ret_wide$date
    ret_mat <- as.matrix(ret_wide[, -1, with = FALSE])
    # garante colunas na ordem de w_t
    ret_mat <- ret_mat[, names(w_t), drop = FALSE]

    for (k in seq_along(dates_h)) {
      r_vec <- ret_mat[k, ]
      # retorno da carteira nesse dia
      r_p <- sum(w_t * r_vec, na.rm = TRUE)
      current_value <- current_value * (1 + r_p)
      equity[date == dates_h[k], equity := current_value]
    }
  }

  # preenche buracos por interpolação de último valor conhecido
  data.table::setorder(equity, date)
  equity[, equity := zoo::na.locf(equity, na.rm = FALSE)]
  equity[is.na(equity), equity := 1]

  # stats básicos
  ret_daily <- equity[, equity / data.table::shift(equity) - 1]
  ret_daily <- ret_daily[is.finite(ret_daily)]
  cagr <- (tail(equity$equity, 1) / head(equity$equity, 1))^(252 / length(ret_daily)) - 1
  vol  <- stats::sd(ret_daily, na.rm = TRUE) * sqrt(252)
  sharpe <- if (vol > 0) cagr / vol else NA_real_

  list(
    equity_curve = equity,
    trades       = trades,
    stats        = list(
      cagr   = cagr,
      vol    = vol,
      sharpe = sharpe
    )
  )
}
