############################################################
# autofinance_ingest_splits.R
# Sincronização de splits (e futuramente dividendos) via quantmod
############################################################

af_sync_splits <- function(con = NULL,
                           symbols = NULL,
                           max_age_days = 7L,
                           force = FALSE,
                           verbose = TRUE,
                           investable_only = TRUE,
                           mode = c("investable", "all", "suspects"),
                           suspect_threshold = 0.2) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    stop("af_sync_splits: 'con' is not a valid DBI connection.")
  }
  mode <- match.arg(mode)
  af_attach_packages(c("data.table", "quantmod"))
  dt_assets <- data.table::as.data.table(
    DBI::dbGetQuery(con, "SELECT symbol, active, last_update_splits FROM assets_meta")
  )

  if (!is.null(symbols)) {
    dt_assets <- dt_assets[symbol %in% symbols]
  } else {
    if (mode == "investable") {
      dt_assets <- dt_assets[active == 1L]
    } else if (mode == "suspects") {
      # Heuristic: recent unadjusted big moves
      lookback_start <- Sys.Date() - 365
      px <- data.table::as.data.table(DBI::dbGetQuery(
        con,
        sprintf("
          SELECT symbol, refdate, close
          FROM prices_raw
          WHERE refdate >= '%s'
        ", format(lookback_start, "%Y-%m-%d"))
      ))
      if (nrow(px)) {
        px[, refdate := as.Date(refdate)]
        data.table::setorder(px, symbol, refdate)
        px[, ret := close / data.table::shift(close) - 1, by = symbol]
        diag_dt <- px[, .(max_abs_ret = max(abs(ret), na.rm = TRUE)), by = symbol]
        suspects <- diag_dt[max_abs_ret >= suspect_threshold, symbol]
        dt_assets <- dt_assets[symbol %in% suspects]
      } else {
        dt_assets <- dt_assets[0]
      }
    } else {
      # mode == "all"
      dt_assets <- dt_assets
    }
    if (investable_only) {
      dt_assets <- dt_assets[active == 1L]
    }
  }

  today <- Sys.Date()
  dt_assets[, last_date := as.Date(last_update_splits)]
  if (!force) {
    dt_assets <- dt_assets[is.na(last_date) | (today - last_date) > max_age_days]
  }

  if (nrow(dt_assets) == 0L) {
    if (verbose) message("af_sync_splits: nothing to update.")
    return(invisible(TRUE))
  }

  all_symbols_checked <- character(0)

  for (sym in dt_assets$symbol) {
    ysym <- paste0(sym, ".SA")
    if (verbose) message("af_sync_splits: ", sym, " (", ysym, ")")
    splits_xts <- NULL
    # basic backoff for Yahoo 429/tempo issues
    fetch_splits <- function() {
      tryCatch({
        suppressWarnings(
          quantmod::getSplits(ysym, auto.assign = FALSE)
        )
      }, error = function(e) {
        msg <- conditionMessage(e)
        if (grepl("429|Too Many Requests", msg, ignore.case = TRUE)) {
          if (verbose) message("  Yahoo rate limit, sleeping 60s before retry...")
          Sys.sleep(60)
          tryCatch({
            suppressWarnings(
              quantmod::getSplits(ysym, auto.assign = FALSE)
            )
          }, error = function(e2) {
            if (verbose) message("  retry failed for ", sym, ": ", conditionMessage(e2))
            NULL
          })
        } else {
          if (verbose) message("  error fetching splits for ", sym, ": ", msg)
          NULL
        }
      })
    }
    splits_xts <- fetch_splits()

    if (!is.null(splits_xts) && NROW(splits_xts) > 0) {
      dt_s <- data.table::data.table(
        symbol = sym,
        date   = as.Date(zoo::index(splits_xts)),
        type   = "SPLIT",
        value  = as.numeric(splits_xts[, 1])
      )
      # FIX: Changed from af_db_insert_corporate_actions to matches db_core
      af_db_insert_adjustments(con, dt_s)
    } else {
      if (verbose) message("  no splits found for ", sym, " (using unadjusted prices).")
    }

    # Atualiza metadata para não ficar rechecando (User Logic Preserved)
    DBI::dbExecute(
      con,
      "INSERT OR REPLACE INTO assets_meta
       (symbol, asset_type, sector, active, last_update_splits, last_update_divs)
       VALUES (
         :symbol,
         COALESCE((SELECT asset_type FROM assets_meta WHERE symbol = :symbol), NULL),
         COALESCE((SELECT sector     FROM assets_meta WHERE symbol = :symbol), NULL),
         COALESCE((SELECT active     FROM assets_meta WHERE symbol = :symbol), 1),
         :last_update_splits,
         COALESCE((SELECT last_update_divs FROM assets_meta WHERE symbol = :symbol), NULL)
       )",
      params = list(
        symbol = sym,
        last_update_splits = as.character(today)
      )
    )

    all_symbols_checked <- c(all_symbols_checked, sym)
    Sys.sleep(0.2) # educado com Yahoo
  }

  invisible(all_symbols_checked)
}
