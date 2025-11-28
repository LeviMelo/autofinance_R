############################################################
# autofinance_ingest_splits.R
# Sincronização de splits (e futuramente dividendos) via quantmod
############################################################

af_sync_splits <- function(con = af_db_connect(),
                           symbols = NULL,
                           max_age_days = 7L,
                           force = FALSE,
                           verbose = TRUE) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages(c("data.table", "quantmod"))
  dt_assets <- data.table::as.data.table(
    DBI::dbGetQuery(con, "SELECT symbol, active, last_update_splits FROM assets_meta")
  )

  if (!is.null(symbols)) {
    dt_assets <- dt_assets[symbol %in% symbols]
  } else {
    dt_assets <- dt_assets[active == 1L]
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
    try({
      suppressWarnings(
        splits_xts <- quantmod::getSplits(ysym, auto.assign = FALSE)
      )
    }, silent = TRUE)

    if (!is.null(splits_xts) && NROW(splits_xts) > 0) {
      dt_s <- data.table::data.table(
        symbol = sym,
        date   = as.Date(zoo::index(splits_xts)),
        type   = "SPLIT",
        value  = as.numeric(splits_xts[, 1])
      )
      af_db_insert_corporate_actions(con, dt_s)
    }

    # Atualiza metadata para não ficar rechecando
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
