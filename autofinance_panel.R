############################################################
# autofinance_panel.R
# Construção de painel ajustado + retornos + merge com macro
############################################################

af_get_prices_raw <- function(con, symbols, start_date, end_date) {
  syms_sql <- paste(sprintf("'%s'", symbols), collapse = ",")
  q <- sprintf("
    SELECT symbol, refdate, open, high, low, close, vol_fin, qty
    FROM prices_raw
    WHERE symbol IN (%s)
      AND refdate >= '%s'
      AND refdate <= '%s'
    ORDER BY symbol, refdate
  ", syms_sql, start_date, end_date)
  dt <- data.table::as.data.table(DBI::dbGetQuery(con, q))
  dt[, refdate := as.Date(refdate)]
  dt
}

af_get_corporate_actions_splits <- function(con, symbols, end_date) {
  syms_sql <- paste(sprintf("'%s'", symbols), collapse = ",")
  q <- sprintf("
    SELECT symbol, date, value
    FROM corporate_actions
    WHERE symbol IN (%s)
      AND type = 'SPLIT'
      AND date <= '%s'
    ORDER BY symbol, date
  ", syms_sql, end_date)
  dt <- data.table::as.data.table(DBI::dbGetQuery(con, q))
  dt[, date := as.Date(date)]
  dt
}

af_build_adjusted_panel <- function(con,
                                    symbols,
                                    start_date,
                                    end_date) {
  af_attach_packages("data.table")
  prices <- af_get_prices_raw(con, symbols, start_date, end_date)
  if (nrow(prices) == 0L) {
    stop("af_build_adjusted_panel: no price data found.")
  }
  splits <- af_get_corporate_actions_splits(con, symbols, end_date)

  data.table::setorder(prices, symbol, refdate)
  prices[, `:=`(
    open_adj  = open,
    high_adj  = high,
    low_adj   = low,
    close_adj = close,
    adj_factor = 1
  )]

  if (nrow(splits) > 0L) {
    data.table::setorder(splits, symbol, date)
    for (sym in unique(prices$symbol)) {
      dt_sym <- prices[symbol == sym]
      sp_sym <- splits[symbol == sym]
      if (nrow(sp_sym) == 0L) next
      # multiplicar fator para datas anteriores a cada split
      for (k in seq_len(nrow(sp_sym))) {
        s_date <- sp_sym$date[k]
        ratio  <- sp_sym$value[k]
        dt_sym[refdate < s_date, adj_factor := adj_factor * ratio]
      }
      dt_sym[, `:=`(
        open_adj  = open  * adj_factor,
        high_adj  = high  * adj_factor,
        low_adj   = low   * adj_factor,
        close_adj = close * adj_factor
      )]
      prices[symbol == sym] <- dt_sym
    }
  }

  prices[, `:=`(
    date = refdate
  )]

  prices
}

af_compute_returns <- function(panel_adj,
                               rf_series = NULL) {
  af_attach_packages("data.table")
  dt <- data.table::as.data.table(panel_adj)
  data.table::setorder(dt, symbol, date)

  dt[, ret_simple := close_adj / data.table::shift(close_adj) - 1,
     by = symbol]
  dt[, ret_log := log(close_adj / data.table::shift(close_adj)),
     by = symbol]

  if (!is.null(rf_series)) {
    # rf_series: data.table(refdate, rf_daily) onde rf_daily é retorno diário
    rf <- data.table::copy(rf_series)
    data.table::setnames(rf, c("refdate", "rf_daily"))
    rf[, refdate := as.Date(refdate)]
    dt <- rf[dt, on = .(refdate = date)]
    # se rf_daily estiver NA, assume 0
    dt[is.na(rf_daily), rf_daily := 0]
    dt[, excess_ret_simple := ret_simple - rf_daily]
  } else {
    dt[, excess_ret_simple := ret_simple]
  }

  dt
}

af_get_macro_series <- function(con, series_ids, start_date, end_date) {
  syms_sql <- paste(sprintf("'%s'", series_ids), collapse = ",")
  q <- sprintf("
    SELECT series_id, refdate, value
    FROM macro_series
    WHERE series_id IN (%s)
      AND refdate >= '%s'
      AND refdate <= '%s'
    ORDER BY series_id, refdate
  ", syms_sql, start_date, end_date)
  dt <- data.table::as.data.table(DBI::dbGetQuery(con, q))
  dt[, refdate := as.Date(refdate)]
  dt
}
