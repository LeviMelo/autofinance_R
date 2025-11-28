############################################################
# autofinance_ingest_b3.R
# Sincronização COTAHIST -> prices_raw
############################################################

# ATENÇÃO:
# Aqui eu NÃO chuto a API do rb3, para não te ferrar com função errada.
# Você pluga seu fetch com rb3 dentro de `af_fetch_cotahist_year()`.

af_fetch_cotahist_year <- function(year) {
  # TODO: implemente usando rb3::... com sua pipeline atual.
  # Deve retornar data.table com colunas:
  # symbol, refdate (Date), open, high, low, close, vol_fin, qty
  stop("Implemente af_fetch_cotahist_year(year) usando rb3 na sua máquina.")
}

af_sync_b3 <- function(con = af_db_connect(),
                       years = NULL,
                       verbose = TRUE) {
  on.exit(af_db_disconnect(con), add = TRUE)
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' required.")
  }
  data.table::setDTthreads(1L)

  if (is.null(years)) {
    max_ref <- af_db_get_max_refdate_prices(con)
    if (is.na(max_ref)) {
      # DB vazio -> pega últimos N anos ou toda a série, a seu critério
      current_year <- as.integer(format(Sys.Date(), "%Y"))
      years <- current_year
    } else {
      last_year <- as.integer(substr(max_ref, 1, 4))
      current_year <- as.integer(format(Sys.Date(), "%Y"))
      years <- seq.int(last_year, current_year)
    }
  }

  for (y in years) {
    if (verbose) message("af_sync_b3: fetching COTAHIST year ", y)
    dt <- af_fetch_cotahist_year(y)
    if (!inherits(dt, "data.table")) data.table::setDT(dt)
    if (!all(c("symbol", "refdate", "open", "high", "low", "close", "vol_fin", "qty") %in% names(dt))) {
      stop("af_fetch_cotahist_year must return symbol, refdate, open, high, low, close, vol_fin, qty")
    }
    af_db_insert_prices_raw(con, dt)
  }

  invisible(TRUE)
}
