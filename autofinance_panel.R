############################################################
# autofinance_panel.R
# Constrói OHLC ajustado e retornos
############################################################

af_build_adjusted_panel <- function(con,
                                    symbols,
                                    start_date,
                                    end_date) {
  af_attach_packages(c("DBI", "data.table", "xts", "TTR", "quantmod"))
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  syms_sql <- paste(sprintf("'%s'", symbols), collapse = ",")

  q_prices <- sprintf("
    SELECT symbol, refdate, open, high, low, close, vol_fin, qty
    FROM prices_raw
    WHERE symbol IN (%s)
      AND refdate >= '%s'
      AND refdate <= '%s'
  ",
    syms_sql,
    format(start_date, "%Y-%m-%d"),
    format(end_date,   "%Y-%m-%d")
  )

  prices <- data.table::as.data.table(DBI::dbGetQuery(con, q_prices))
  if (!nrow(prices)) {
    stop("af_build_adjusted_panel: no prices in requested range.")
  }
  prices[, refdate := as.Date(refdate)]

  q_adj <- sprintf("
    SELECT symbol, date, type, value
    FROM adjustments
    WHERE symbol IN (%s)
      AND date >= '%s'
      AND date <= '%s'
  ",
    syms_sql,
    format(start_date - 365, "%Y-%m-%d"),  # um pouco antes, por segurança
    format(end_date, "%Y-%m-%d")
  )
  adj <- data.table::as.data.table(DBI::dbGetQuery(con, q_adj))
  if (nrow(adj)) {
    adj[, date := as.Date(date)]
  }

  out_list <- list()

  for (sym in symbols) {
    dt_sym <- prices[symbol == sym]
    if (!nrow(dt_sym)) next

    data.table::setorder(dt_sym, refdate)
    x <- xts::xts(
      x = cbind(
        Open  = dt_sym$open,
        High  = dt_sym$high,
        Low   = dt_sym$low,
        Close = dt_sym$close
      ),
      order.by = dt_sym$refdate
    )

    adj_sym <- if (nrow(adj)) adj[symbol == sym & type == "SPLIT"] else adj[0]
    if (nrow(adj_sym)) {
      spl <- xts::xts(adj_sym$value, order.by = adj_sym$date)
      # ratios via TTR::adjRatios; como os splits vieram de quantmod, a convenção bate
      ratios <- TTR::adjRatios(quantmod::Cl(x), splits = spl)
      x_adj  <- quantmod::adjustOHLC(x, ratios = ratios)
    } else {
      x_adj <- x
    }

    dt_out <- data.table::data.table(
      symbol    = sym,
      date      = as.Date(index(x_adj)),
      open_adj  = as.numeric(x_adj[, 1]),
      high_adj  = as.numeric(x_adj[, 2]),
      low_adj   = as.numeric(x_adj[, 3]),
      close_adj = as.numeric(x_adj[, 4])
    )

    # colar vol_fin/qty originais
    dt_out <- prices[symbol == sym][dt_out,
                                    on = .(symbol, refdate = date),
                                    .(symbol,
                                      date,
                                      open_adj,
                                      high_adj,
                                      low_adj,
                                      close_adj,
                                      vol_fin = i.vol_fin,
                                      qty     = i.qty)]

    out_list[[sym]] <- dt_out
  }

  panel <- data.table::rbindlist(out_list, fill = TRUE)
  data.table::setorder(panel, symbol, date)
  panel
}

af_compute_returns <- function(panel) {
  af_attach_packages("data.table")
  panel <- data.table::as.data.table(panel)
  if (!nrow(panel)) return(panel)

  data.table::setorder(panel, symbol, date)
  panel[, ret_simple := close_adj / data.table::shift(close_adj) - 1, by = symbol]
  # Por enquanto, excesso = próprio retorno; depois pluga RF se quiser
  panel[, excess_ret_simple := ret_simple]

  panel
}
