############################################################
# autofinance_ingest_macro.R
# Sincronização de séries macro do BCB SGS -> macro_series
############################################################

# Aqui também não chuto o wrapper exato; você pode usar rbcb, GetBCBData, etc.
# Forneço uma função abstrata para buscar do BCB.

af_fetch_bcb_series <- function(series_id, start_date, end_date) {
  # TODO: implemente usando rbcb::get_series() ou GetBCBData::gbcbd_get() etc.
  # Deve retornar data.table(series_id, refdate, value)
  stop("Implemente af_fetch_bcb_series(series_id, start_date, end_date) com seu wrapper SGS.")
}

af_sync_macro <- function(con = af_db_connect(),
                          series_ids,
                          start_date = "2000-01-01",
                          end_date = as.character(Sys.Date()),
                          verbose = TRUE) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("data.table")

  for (sid in series_ids) {
    if (verbose) message("af_sync_macro: series ", sid)
    dt <- af_fetch_bcb_series(sid, start_date, end_date)
    if (!inherits(dt, "data.table")) data.table::setDT(dt)
    if (!all(c("series_id", "refdate", "value") %in% names(dt))) {
      stop("af_fetch_bcb_series must return series_id, refdate, value")
    }
    af_db_insert_macro_series(con, dt)
  }
  invisible(TRUE)
}
