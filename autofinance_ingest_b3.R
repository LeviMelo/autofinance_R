############################################################
# autofinance_ingest_b3.R
# Sincronização COTAHIST -> prices_raw
############################################################

# ATENÇÃO:
# Aqui eu NÃO chuto a API do rb3, para não te ferrar com função errada.
# Você pluga seu fetch com rb3 dentro de `af_fetch_cotahist_year()`.

af_fetch_cotahist_year <- function(year) {
  af_attach_packages(c("rb3", "data.table", "dplyr"))
  
  message(sprintf("Downloading COTAHIST for %s...", year))
  
  # rb3::cotahist_get returns a tibble/data.frame
  df_raw <- tryCatch({
    rb3::cotahist_get(year, type = "yearly")
  }, error = function(e) {
    warning(sprintf("Failed to fetch year %s: %s", year, e$message))
    return(NULL)
  })
  
  if (is.null(df_raw) || nrow(df_raw) == 0) return(data.table::data.table())
  
  dt <- data.table::as.data.table(df_raw)
  
  # Filter standard market (Lote Padrão = 2) or Fractional (Lote Fracionário = 10) if desired.
  # Usually standard market (cod_bdi == '02') is what we want for liquid assets.
  # We will keep everything and let the screener filter by liquidity/type later, 
  # BUT standardizing ticker names is crucial (remove trailing spaces).
  
  dt[, symbol := trimws(symbol)]
  
  # Select and Rename columns to match prices_raw schema:
  # symbol, refdate, open, high, low, close, vol_fin, qty
  
  # Map rb3 columns:
  # refdate -> refdate
  # open -> open
  # high -> high
  # low -> low
  # close -> close
  # volume -> vol_fin (financial volume)
  # quantity -> qty (share quantity)
  
  # Ensure dates are strings for SQLite (ISO 8601)
  dt_out <- dt[, .(
    symbol  = symbol,
    refdate = as.Date(refdate),
    open    = as.numeric(open),
    high    = as.numeric(high),
    low     = as.numeric(low),
    close   = as.numeric(close),
    vol_fin = as.numeric(volume),
    qty     = as.numeric(quantity)
  )]
  
  # Remove invalid rows
  dt_out <- dt_out[!is.na(symbol) & !is.na(refdate) & !is.na(close)]
  
  return(dt_out)
}

af_sync_b3 <- function(con = af_db_connect(),
                       years = NULL,
                       verbose = TRUE) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("data.table")
  
  if (is.null(years)) {
    # Check DB for max date
    max_date_str <- tryCatch({
      DBI::dbGetQuery(con, "SELECT MAX(refdate) as d FROM prices_raw")$d
    }, error = function(e) NA)
    
    current_year <- as.integer(format(Sys.Date(), "%Y"))
    
    if (is.na(max_date_str) || is.null(max_date_str)) {
      # Bootstrap: Start from 2015 (or config default) if empty
      years <- 2015:current_year
    } else {
      last_year <- as.integer(substr(max_date_str, 1, 4))
      years <- last_year:current_year
    }
  }
  
  for (y in years) {
    if (verbose) message("af_sync_b3: processing year ", y)
    dt <- af_fetch_cotahist_year(y)
    
    if (nrow(dt) > 0) {
      if (verbose) message("  inserting ", nrow(dt), " rows...")
      af_db_insert_prices_raw(con, dt)
    } else {
      if (verbose) message("  no data found for ", y)
    }
  }
  
  invisible(TRUE)
}
