############################################################
# autofinance_panel.R
#
# Painel de preços ajustados + retornos
#
# Depende de:
#   - autofinance_config.R (AF_DB_PATH, af_attach_packages)
#   - Banco SQLite com:
#       prices_raw(symbol, refdate, open, high, low, close, vol_fin, qty)
#       adjustments(symbol, date, type, value)  -- pelo menos type == "SPLIT"
#
# Principais funções:
#   - af_load_prices_raw()
#   - af_load_adjustments()
#   - af_build_adjusted_panel()
#   - af_compute_returns()
############################################################

if (!exists("af_attach_packages")) {
  if (file.exists("autofinance_config.R")) {
    source("autofinance_config.R")
  } else {
    stop("autofinance_config.R not found; please source it before using panel.")
  }
}

af_attach_packages(c("data.table", "DBI", "RSQLite", "quantmod", "xts", "TTR"))

# ----------------------------------------------------------------------
# Helper: small internal connection helper (optional)
# Você pode continuar usando seu próprio af_db_connect(), se já existir.
# Aqui deixo um helper leve para casos em que você só tem AF_DB_PATH.
# ----------------------------------------------------------------------

af_panel_db_connect <- function(db_path = AF_DB_PATH) {
  RSQLite::dbConnect(RSQLite::SQLite(), db_path)
}

# ----------------------------------------------------------------------
# 1) Carregar preços crus (prices_raw) do DB
# ----------------------------------------------------------------------

# Retorna data.table:
#   symbol, refdate(Date), open, high, low, close, vol_fin, qty
af_load_prices_raw <- function(con = NULL,
                               symbols    = NULL,
                               start_date = NULL,
                               end_date   = NULL) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    stop("af_load_prices_raw: 'con' is not a valid DBI connection.")
  }

  where_clauses <- c()

  if (!is.null(symbols) && length(symbols) > 0) {
    syms_sql <- paste(DBI::dbQuoteString(con, symbols), collapse = ",")
    where_clauses <- c(where_clauses, sprintf("symbol IN (%s)", syms_sql))
  }

  if (!is.null(start_date)) {
    start_date <- as.character(as.Date(start_date))
    where_clauses <- c(where_clauses, sprintf("refdate >= '%s'", start_date))
  }

  if (!is.null(end_date)) {
    end_date <- as.character(as.Date(end_date))
    where_clauses <- c(where_clauses, sprintf("refdate <= '%s'", end_date))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  sql <- sprintf("
    SELECT symbol, refdate, open, high, low, close, vol_fin, qty
    FROM prices_raw
    %s
    ORDER BY symbol, refdate
  ", where_sql)

  dt <- data.table::as.data.table(DBI::dbGetQuery(con, sql))
  if (nrow(dt) == 0L) {
    warning("af_load_prices_raw: no rows returned for given filters.")
    return(dt)
  }

  dt[, refdate := as.Date(refdate)]
  dt[]
}

# ----------------------------------------------------------------------
# 2) Carregar ajustes (splits/dividends) do DB
# ----------------------------------------------------------------------

# Retorna data.table:
#   symbol, date(Date), type, value
af_load_adjustments <- function(con = NULL,
                                symbols    = NULL,
                                start_date = NULL,
                                end_date   = NULL,
                                types      = c("SPLIT")) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    stop("af_load_adjustments: 'con' is not a valid DBI connection.")
  }

  where_clauses <- c()

  if (!is.null(symbols) && length(symbols) > 0) {
    syms_sql <- paste(DBI::dbQuoteString(con, symbols), collapse = ",")
    where_clauses <- c(where_clauses, sprintf("symbol IN (%s)", syms_sql))
  }

  if (!is.null(start_date)) {
    start_date <- as.character(as.Date(start_date))
    where_clauses <- c(where_clauses, sprintf("date >= '%s'", start_date))
  }

  if (!is.null(end_date)) {
    end_date <- as.character(as.Date(end_date))
    where_clauses <- c(where_clauses, sprintf("date <= '%s'", end_date))
  }

  if (!is.null(types) && length(types) > 0) {
    tps_sql <- paste(DBI::dbQuoteString(con, types), collapse = ",")
    where_clauses <- c(where_clauses, sprintf("type IN (%s)", tps_sql))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  sql <- sprintf("
    SELECT symbol, date, type, value
    FROM adjustments
    %s
    ORDER BY symbol, date
  ", where_sql)

  dt <- data.table::as.data.table(DBI::dbGetQuery(con, sql))
  if (nrow(dt) == 0L) {
    # não é erro: pode simplesmente não haver splits no período
    dt[, date := as.Date(character())]
    return(dt)
  }

  dt[, date := as.Date(date)]
  dt[]
}

# ----------------------------------------------------------------------
# 3) Aplicar splits via quantmod::adjustOHLC (por símbolo)
# ----------------------------------------------------------------------

# prices_dt: data.table para UM símbolo:
#   refdate, open, high, low, close
# splits_dt: data.table para UM símbolo:
#   date, value (type=="SPLIT")
#
# Retorna data.table com colunas:
#   refdate, open_adj, high_adj, low_adj, close_adj
af_apply_splits_one_symbol <- function(prices_dt,
                                       splits_dt = NULL) {
  # prices_dt: data.table(refdate, open, high, low, close)
  # splits_dt: data.table(date, value) with split factors from Yahoo

  if (nrow(prices_dt) == 0L) return(NULL)

  # Garantir ordenação
  data.table::setorder(prices_dt, refdate)

  # Construir xts OHLC
  ohlc_mat <- as.matrix(prices_dt[, .(open, high, low, close)])
  colnames(ohlc_mat) <- c("Open", "High", "Low", "Close")
  ohlc_xts <- xts::xts(ohlc_mat, order.by = prices_dt$refdate)

  # Se houver splits para este ativo, construir série de razões diárias
  if (!is.null(splits_dt) && nrow(splits_dt) > 0L) {
    # 1) xts esparso com eventos de split (como getSplits retorna)
    split_xts <- xts::xts(
      x        = splits_dt$value,
      order.by = splits_dt$date
    )

    # 2) Construir fatores de ajuste diários à la CRSP (TTR::adjRatios)
    #    - 'splits' = fatores de split (0.5 para 2:1 etc.)
    #    - 'dividends' = NULL por enquanto (não estamos ajustando proventos)
    #    - 'close' = série de fechamento original
    ratios <- TTR::adjRatios(
      splits    = split_xts,
      dividends = NULL,
      close     = quantmod::Cl(ohlc_xts)
    )

    # 3) Usar apenas coluna de split; mesmo índice que ohlc_xts
    split_ratio <- ratios[, "Split"]

    ohlc_adj <- quantmod::adjustOHLC(
      ohlc_xts,
      use.Adjusted = FALSE,
      ratio        = split_ratio
    )
  } else {
    # Sem splits: apenas replica
    ohlc_adj <- ohlc_xts
  }

  out <- data.table::data.table(
    refdate   = as.Date(zoo::index(ohlc_adj)),
    open_adj  = as.numeric(ohlc_adj[, "Open"]),
    high_adj  = as.numeric(ohlc_adj[, "High"]),
    low_adj   = as.numeric(ohlc_adj[, "Low"]),
    close_adj = as.numeric(ohlc_adj[, "Close"])
  )

  out[]
}


# ----------------------------------------------------------------------
# 4) Construir painel ajustado para vários símbolos
# ----------------------------------------------------------------------

# Retorna data.table:
#   symbol, refdate, open_adj, high_adj, low_adj, close_adj,
#   vol_fin, qty
#
# Nota: vol_fin e qty vêm do prices_raw sem ajuste (o padrão mesmo).
#       Se quiser "volume ajustado", dá pra derivar depois.
af_build_adjusted_panel <- function(con = NULL,
                                    symbols    = NULL,
                                    start_date = NULL,
                                    end_date   = NULL) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  # 1) Carregar preços crus
  prices <- af_load_prices_raw(
    con        = con,
    symbols    = symbols,
    start_date = start_date,
    end_date   = end_date
  )
  if (nrow(prices) == 0L) {
    stop("af_build_adjusted_panel: no raw prices found for given filters.")
  }

  # 2) Carregar splits
  #    Usamos um buffer para garantir que splits anteriores à janela não sejam perdidos.
  start_adj <- if (!is.null(start_date)) as.Date(start_date) - 365L else NULL
  adj <- af_load_adjustments(
    con        = con,
    symbols    = unique(prices$symbol),
    start_date = start_adj,
    end_date   = end_date,
    types      = c("SPLIT")
  )

  # 3) Aplicar por símbolo
  out_list <- list()
  syms <- sort(unique(prices$symbol))

  for (sym in syms) {
    p_sym <- prices[symbol == sym]
    s_sym <- if (nrow(adj) > 0L) adj[symbol == sym & type == "SPLIT"] else NULL

    adj_sym <- af_apply_splits_one_symbol(
      prices_dt = p_sym[, .(refdate, open, high, low, close)],
      splits_dt = if (!is.null(s_sym) && nrow(s_sym) > 0L) s_sym[, .(date, value)] else NULL
    )

    if (!is.null(adj_sym) && nrow(adj_sym) > 0L) {
      # Juntar vol_fin e qty
      merged <- merge(
        p_sym[, .(refdate, vol_fin, qty)],
        adj_sym,
        by = "refdate",
        all.y = TRUE
      )

      merged[, symbol := sym]
      data.table::setcolorder(merged,
                              c("symbol", "refdate",
                                "open_adj", "high_adj", "low_adj", "close_adj",
                                "vol_fin", "qty"))

      out_list[[sym]] <- merged[]
    }
  }

  panel_adj <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  data.table::setorder(panel_adj, symbol, refdate)

  panel_adj[]
}

# ----------------------------------------------------------------------
# 5) Calcular retornos (simples e excessos)
# ----------------------------------------------------------------------

# panel_adj: data.table com:
#   symbol, refdate, close_adj (e, opcionalmente, outras colunas)
#
# rf_daily_dt (opcional): data.table com:
#   refdate, rf_daily  (taxa diária em decimal, ex: 0.00025 ~ 0.025%/dia)
#
# Retorna painel com colunas extras:
#   ret_simple, excess_ret_simple (se RF fornecido)
af_compute_returns <- function(panel_adj,
                               rf_daily_dt = NULL,
                               rf_col      = "rf_daily") {
  dt <- data.table::as.data.table(panel_adj)
  if (!all(c("symbol", "refdate", "close_adj") %in% names(dt))) {
    stop("af_compute_returns: panel_adj must contain 'symbol', 'refdate', 'close_adj'.")
  }

  data.table::setorder(dt, symbol, refdate)

  # Retorno simples por ativo
  dt[, close_lag := data.table::shift(close_adj, n = 1L, type = "lag"),
     by = symbol]

  dt[, ret_simple := ifelse(
    !is.na(close_lag) & close_lag != 0,
    (close_adj / close_lag) - 1,
    NA_real_
  )]

  dt[, close_lag := NULL]

  # Se RF fornecido, calcular excesso
  if (!is.null(rf_daily_dt)) {
    rf <- data.table::as.data.table(rf_daily_dt)
    if (!("refdate" %in% names(rf))) {
      stop("rf_daily_dt must contain 'refdate' column.")
    }
    if (!(rf_col %in% names(rf))) {
      stop(sprintf("rf_daily_dt must contain column '%s'.", rf_col))
    }
    rf[, refdate := as.Date(refdate)]

    # merge por data (mesma RF para todos ativos naquele dia)
    dt <- merge(
      dt,
      rf[, .(refdate, rf_daily = get(rf_col))],
      by = "refdate",
      all.x = TRUE
    )

    dt[, excess_ret_simple := ifelse(
      !is.na(ret_simple) & !is.na(rf_daily),
      ret_simple - rf_daily,
      NA_real_
    )]
  }

  dt[]
}
