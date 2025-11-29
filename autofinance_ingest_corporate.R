############################################################
# autofinance_ingest_corporate.R
# Splits & dividendos via quantmod -> adjustments
############################################################

af_symbol_to_yahoo <- function(symbol) {
  paste0(symbol, ".SA")
}

af_yahoo_to_symbol <- function(yahoo_symbol) {
  sub("\\.SA$", "", yahoo_symbol)
}

af_sync_yahoo_splits <- function(con = NULL,
                                 symbols = NULL,
                                 from_default = as.Date("2000-01-01"),
                                 verbose = TRUE) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    stop("af_sync_yahoo_splits: 'con' is not a valid DBI connection.")
  }

  af_attach_packages(c("DBI", "data.table", "quantmod"))

  af_db_init(con)

  meta <- data.table::as.data.table(
    DBI::dbGetQuery(con, "SELECT symbol, last_update_splits FROM assets_meta")
  )

  if (!is.null(symbols)) {
    meta <- meta[symbol %in% symbols]
  }

  if (!nrow(meta)) {
    stop("af_sync_yahoo_splits: no symbols in assets_meta.")
  }

  results <- list()

  for (i in seq_len(nrow(meta))) {
    sym <- meta$symbol[i]
    last_upd <- meta$last_update_splits[i]
    ysym <- af_symbol_to_yahoo(sym)

    from_date <- if (is.na(last_upd) || last_upd == "" || is.null(last_upd)) {
      from_default
    } else {
      as.Date(last_upd) - 5L  # pequena folga
    }

    if (verbose) message("Splits: ", sym, " (", ysym, ") from ", from_date, "...")

    sp <- tryCatch(
      quantmod::getSplits(ysym, from = from_date, auto.assign = FALSE),
      error = function(e) {
        if (verbose) message("  error fetching splits for ", sym, ": ", conditionMessage(e))
        NULL
      }
    )

    if (is.null(sp) || nrow(sp) == 0L) {
      # mesmo sem splits, atualizamos last_update_splits
      new_last <- format(Sys.Date(), "%Y-%m-%d")
      DBI::dbExecute(
        con,
        "UPDATE assets_meta SET last_update_splits = ? WHERE symbol = ?",
        params = list(new_last, sym)
      )
      next
    }

    dt_sp <- data.table::data.table(
      symbol = sym,
      date   = as.Date(index(sp)),
      type   = "SPLIT",
      value  = as.numeric(sp[, 1])
    )
    results[[sym]] <- dt_sp

    new_last <- format(max(dt_sp$date), "%Y-%m-%d")
    DBI::dbExecute(
      con,
      "UPDATE assets_meta SET last_update_splits = ? WHERE symbol = ?",
      params = list(new_last, sym)
    )
  }

  if (length(results) > 0L) {
    dt_all <- data.table::rbindlist(results, fill = TRUE)
    af_db_insert_adjustments(con, dt_all)
  }

  invisible(TRUE)
}

af_sync_yahoo_dividends <- function(con = NULL,
                                    symbols = NULL,
                                    from_default = as.Date("2000-01-01"),
                                    verbose = TRUE) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  af_attach_packages(c("DBI", "data.table", "quantmod"))

  af_db_init(con)

  meta <- data.table::as.data.table(
    DBI::dbGetQuery(con, "SELECT symbol, last_update_divs FROM assets_meta")
  )

  if (!is.null(symbols)) {
    meta <- meta[symbol %in% symbols]
  }

  if (!nrow(meta)) {
    stop("af_sync_yahoo_dividends: no symbols in assets_meta.")
  }

  results <- list()

  for (i in seq_len(nrow(meta))) {
    sym <- meta$symbol[i]
    last_upd <- meta$last_update_divs[i]
    ysym <- af_symbol_to_yahoo(sym)

    from_date <- if (is.na(last_upd) || last_upd == "" || is.null(last_upd)) {
      from_default
    } else {
      as.Date(last_upd) - 5L
    }

    if (verbose) message("Dividends: ", sym, " (", ysym, ") from ", from_date, "...")

    dv <- tryCatch(
      quantmod::getDividends(ysym, from = from_date),
      error = function(e) NULL
    )
    if (is.null(dv) || nrow(dv) == 0L) {
      new_last <- format(Sys.Date(), "%Y-%m-%d")
      DBI::dbExecute(
        con,
        "UPDATE assets_meta SET last_update_divs = ? WHERE symbol = ?",
        params = list(new_last, sym)
      )
      next
    }

    dt_dv <- data.table::data.table(
      symbol = sym,
      date   = as.Date(index(dv)),
      type   = "DIVIDEND",
      value  = as.numeric(dv[, 1])
    )
    results[[sym]] <- dt_dv

    new_last <- format(max(dt_dv$date), "%Y-%m-%d")
    DBI::dbExecute(
      con,
      "UPDATE assets_meta SET last_update_divs = ? WHERE symbol = ?",
      params = list(new_last, sym)
    )
  }

  if (length(results) > 0L) {
    dt_all <- data.table::rbindlist(results, fill = TRUE)
    af_db_insert_adjustments(con, dt_all)
  }

  invisible(TRUE)
}
