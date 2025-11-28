############################################################
# autofinance_db_core.R
# Conexão, inicialização e helpers do SQLite
############################################################

af_db_connect <- function(db_path = af_get_db_path()) {
  if (!requireNamespace("RSQLite", quietly = TRUE) ||
      !requireNamespace("DBI", quietly = TRUE)) {
    stop("Packages 'RSQLite' and 'DBI' are required.")
  }
  con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(con, "PRAGMA journal_mode = WAL;")
  DBI::dbExecute(con, "PRAGMA synchronous = NORMAL;")
  DBI::dbExecute(con, "PRAGMA cache_size = 10000;")
  con
}

af_db_disconnect <- function(con) {
  if (!is.null(con) && DBI::dbIsValid(con)) {
    RSQLite::dbDisconnect(con)
  }
}

af_db_init <- function(con = af_db_connect()) {
  on.exit(af_db_disconnect(con), add = TRUE)

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS assets_meta (
      symbol TEXT PRIMARY KEY,
      asset_type TEXT,
      sector TEXT,
      active INTEGER,
      last_update_splits TEXT,
      last_update_divs   TEXT
    );
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS prices_raw (
      symbol  TEXT,
      refdate TEXT,
      open    REAL,
      high    REAL,
      low     REAL,
      close   REAL,
      vol_fin REAL,
      qty     INTEGER,
      PRIMARY KEY (symbol, refdate)
    ) WITHOUT ROWID;
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS corporate_actions (
      symbol TEXT,
      date   TEXT,
      type   TEXT,  -- 'SPLIT' or 'DIVIDEND'
      value  REAL,
      PRIMARY KEY (symbol, date, type)
    );
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS macro_series (
      series_id TEXT,
      refdate   TEXT,
      value     REAL,
      PRIMARY KEY (series_id, refdate)
    );
  ")

  invisible(TRUE)
}

af_db_insert_prices_raw <- function(con, dt) {
  stopifnot("symbol" %in% names(dt), "refdate" %in% names(dt))
  dt$refdate <- as.character(dt$refdate)
  DBI::dbBegin(con)
  ok <- FALSE
  try({
    DBI::dbExecute(
      con,
      "INSERT OR REPLACE INTO prices_raw
       (symbol, refdate, open, high, low, close, vol_fin, qty)
       VALUES (:symbol, :refdate, :open, :high, :low, :close, :vol_fin, :qty)",
      params = dt
    )
    ok <- TRUE
  }, silent = TRUE)
  if (ok) DBI::dbCommit(con) else DBI::dbRollback(con)
  invisible(ok)
}

af_db_insert_corporate_actions <- function(con, dt) {
  # dt: symbol, date, type, value
  dt$date <- as.character(dt$date)
  DBI::dbBegin(con)
  ok <- FALSE
  try({
    DBI::dbExecute(
      con,
      "INSERT OR REPLACE INTO corporate_actions
       (symbol, date, type, value)
       VALUES (:symbol, :date, :type, :value)",
      params = dt
    )
    ok <- TRUE
  }, silent = TRUE)
  if (ok) DBI::dbCommit(con) else DBI::dbRollback(con)
  invisible(ok)
}

af_db_insert_macro_series <- function(con, dt) {
  # dt: series_id, refdate, value
  dt$refdate <- as.character(dt$refdate)
  DBI::dbBegin(con)
  ok <- FALSE
  try({
    DBI::dbExecute(
      con,
      "INSERT OR REPLACE INTO macro_series
       (series_id, refdate, value)
       VALUES (:series_id, :refdate, :value)",
      params = dt
    )
    ok <- TRUE
  }, silent = TRUE)
  if (ok) DBI::dbCommit(con) else DBI::dbRollback(con)
  invisible(ok)
}

af_db_get_max_refdate_prices <- function(con) {
  res <- DBI::dbGetQuery(con, "SELECT MAX(refdate) AS max_refdate FROM prices_raw")
  if (is.na(res$max_refdate[1])) return(NA_character_)
  res$max_refdate[1]
}
