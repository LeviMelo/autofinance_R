############################################################
# autofinance_ingest_macro.R
# BCB SGS -> macro_series
############################################################

af_fetch_sgs_series <- function(series_id,
                                start_date,
                                end_date) {
  af_attach_packages(c("httr", "jsonlite", "data.table"))

  start_str <- format(as.Date(start_date), "%d/%m/%Y")
  end_str   <- format(as.Date(end_date),   "%d/%m/%Y")

  url <- sprintf(
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%d/dados?formato=json&dataInicial=%s&dataFinal=%s",
    as.integer(series_id),
    start_str,
    end_str
  )

  resp <- httr::GET(url)
  if (httr::http_error(resp)) {
    stop("af_fetch_sgs_series: HTTP error for series ", series_id)
  }

  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  js  <- jsonlite::fromJSON(txt, simplifyDataFrame = TRUE)
  dt  <- data.table::as.data.table(js)
  if (!nrow(dt)) {
    return(data.table::data.table(
      series_id = integer(0),
      refdate   = as.Date(character(0)),
      value     = numeric(0)
    ))
  }

  # campos padrão: data, valor
  dt[, refdate := as.Date(data, format = "%d/%m/%Y")]
  dt[, value   := as.numeric(gsub(",", ".", valor))]

  out <- dt[, .(series_id = as.integer(series_id),
                refdate,
                value)]
  out
}

af_sync_macro_series <- function(con = af_db_connect(),
                                 series_ids,
                                 start_date,
                                 end_date,
                                 overwrite = FALSE,
                                 verbose = TRUE) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_db_init(con)
  af_attach_packages(c("DBI", "data.table"))

  series_ids <- unique(as.integer(series_ids))

  for (sid in series_ids) {
    if (verbose) message("SGS series ", sid, " from ", start_date, " to ", end_date, "...")
    dt <- af_fetch_sgs_series(sid, start_date, end_date)
    if (!nrow(dt)) {
      if (verbose) message("  no data for series ", sid)
      next
    }
    af_db_insert_macro_series(con, dt)
  }

  invisible(TRUE)
}

af_get_macro_series <- function(con,
                                series_ids,
                                start_date,
                                end_date) {
  af_attach_packages(c("DBI", "data.table"))
  series_ids <- unique(as.character(series_ids))

  q <- sprintf("
    SELECT series_id, refdate, value
    FROM macro_series
    WHERE series_id IN (%s)
      AND refdate >= '%s'
      AND refdate <= '%s'
  ",
    paste(sprintf("'%s'", series_ids), collapse = ","),
    format(as.Date(start_date), "%Y-%m-%d"),
    format(as.Date(end_date),   "%Y-%m-%d")
  )

  dt <- data.table::as.data.table(DBI::dbGetQuery(con, q))
  if (!nrow(dt)) return(dt)
  dt[, refdate := as.Date(refdate)]
  dt
}
