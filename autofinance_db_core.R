############################################################
# autofinance_db_core.R
############################################################

af_db_connect <- function(db_path = AF_DB_PATH) {
  af_attach_packages(c("DBI", "RSQLite"))
  con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(con, "PRAGMA journal_mode = WAL;")
  DBI::dbExecute(con, "PRAGMA synchronous = NORMAL;")
  DBI::dbExecute(con, "PRAGMA foreign_keys = ON;")
  con
}

af_db_disconnect <- function(con) {
  if (!is.null(con) && DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
  }
  invisible(TRUE)
}

af_db_init <- function(con = af_db_connect()) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("DBI")

  # assets_meta
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS assets_meta (
      symbol TEXT PRIMARY KEY,
      asset_type TEXT,
      sector TEXT,
      active INTEGER,
      last_update_splits TEXT,
      last_update_divs TEXT
    )
  ")

  # prices_raw = fita B3
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS prices_raw (
      symbol TEXT,
      refdate TEXT,
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      vol_fin REAL,
      qty REAL,
      PRIMARY KEY (symbol, refdate)
    ) WITHOUT ROWID
  ")

  # adjustments = corporate actions da Yahoo
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS adjustments (
      symbol TEXT,
      date TEXT,
      type TEXT,   -- 'SPLIT' ou 'DIVIDEND'
      value REAL,
      PRIMARY KEY (symbol, date, type)
    ) WITHOUT ROWID
  ")

  # macro_series = SGS / IBOV / USD etc.
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS macro_series (
      series_id TEXT,
      refdate   TEXT,
      value     REAL,
      PRIMARY KEY (series_id, refdate)
    ) WITHOUT ROWID
  ")

  invisible(TRUE)
}

af_db_insert_prices_raw <- function(con, dt) {
  af_attach_packages(c("DBI", "data.table"))
  dt <- data.table::as.data.table(dt)
  if (!nrow(dt)) return(invisible(TRUE))

  # Normalizar colunas
  if (!"refdate" %in% names(dt)) {
    stop("af_db_insert_prices_raw: 'refdate' column missing.")
  }
  dt[, refdate := as.character(as.Date(refdate))]

  cols <- c("symbol", "refdate", "open", "high", "low", "close", "vol_fin", "qty")
  missing <- setdiff(cols, names(dt))
  if (length(missing) > 0L) {
    stop("af_db_insert_prices_raw: missing columns: ", paste(missing, collapse = ", "))
  }

  sql <- "
    INSERT OR REPLACE INTO prices_raw
    (symbol, refdate, open, high, low, close, vol_fin, qty)
    VALUES (:symbol, :refdate, :open, :high, :low, :close, :vol_fin, :qty)
  "

  DBI::dbBegin(con)
  DBI::dbExecute(con, sql, params = dt[, ..cols])
  DBI::dbCommit(con)
  invisible(TRUE)
}

af_db_insert_adjustments <- function(con, dt) {
  af_attach_packages(c("DBI", "data.table"))
  dt <- data.table::as.data.table(dt)
  if (!nrow(dt)) return(invisible(TRUE))

  required <- c("symbol", "date", "type", "value")
  missing  <- setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop("af_db_insert_adjustments: missing columns: ", paste(missing, collapse = ", "))
  }

  dt[, date := as.character(as.Date(date))]

  sql <- "
    INSERT OR REPLACE INTO adjustments
    (symbol, date, type, value)
    VALUES (:symbol, :date, :type, :value)
  "

  DBI::dbBegin(con)
  DBI::dbExecute(con, sql, params = dt[, ..required])
  DBI::dbCommit(con)
  invisible(TRUE)
}

af_db_insert_macro_series <- function(con, dt) {
  af_attach_packages(c("DBI", "data.table"))
  dt <- data.table::as.data.table(dt)
  if (!nrow(dt)) return(invisible(TRUE))

  required <- c("series_id", "refdate", "value")
  missing  <- setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop("af_db_insert_macro_series: missing columns: ", paste(missing, collapse = ", "))
  }

  dt[, refdate := as.character(as.Date(refdate))]

  sql <- "
    INSERT OR REPLACE INTO macro_series
    (series_id, refdate, value)
    VALUES (:series_id, :refdate, :value)
  "

  DBI::dbBegin(con)
  DBI::dbExecute(con, sql, params = dt[, ..required])
  DBI::dbCommit(con)
  invisible(TRUE)
}

af_db_upsert_assets_meta <- function(con, dt) {
  af_attach_packages(c("DBI", "data.table"))
  dt <- data.table::as.data.table(dt)
  if (!nrow(dt)) return(invisible(TRUE))

  cols <- c("symbol", "asset_type", "sector", "active",
            "last_update_splits", "last_update_divs")
  missing <- setdiff(cols, names(dt))
  if (length(missing) > 0L) {
    stop("af_db_upsert_assets_meta: missing columns: ", paste(missing, collapse = ", "))
  }

  sql <- "
    INSERT OR REPLACE INTO assets_meta
    (symbol, asset_type, sector, active, last_update_splits, last_update_divs)
    VALUES (:symbol, :asset_type, :sector, :active, :last_update_splits, :last_update_divs)
  "

  DBI::dbBegin(con)
  DBI::dbExecute(con, sql, params = dt[, ..cols])
  DBI::dbCommit(con)
  invisible(TRUE)
}

af_db_get_symbols <- function(con) {
  af_attach_packages(c("DBI", "data.table"))
  res <- DBI::dbGetQuery(con, "SELECT DISTINCT symbol FROM prices_raw")
  data.table::as.data.table(res)$symbol
}
