# Fetch a daily window with rb3, then return *lazy* dataset handle

af2_b3_fetch_daily_lazy <- function(start_date, end_date,
                                    cfg = NULL, verbose = TRUE,
                                    force_download = FALSE,
                                    reprocess = FALSE,
                                    throttle = TRUE) {

  cfg <- cfg %||% af2_get_config()
  af2_b3_init_rb3(cfg, verbose = verbose)

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  # Now uses proper B3 bizdays if available
  refdates <- af2_make_bizdays_seq(start_date, end_date)

  if (!length(refdates)) {
    stop("af2_b3_fetch_daily_lazy: empty refdate sequence.")
  }

  if (verbose) {
    af2_log("AF2_B3:", "fetch_marketdata daily window: ",
            as.character(start_date), " to ", as.character(end_date),
            " (", length(refdates), " dates)")
  }

  ok <- TRUE
  tryCatch({
    rb3::fetch_marketdata(
      "b3-cotahist-daily",
      refdate = refdates,
      throttle = isTRUE(throttle),
      force_download = isTRUE(force_download),
      reprocess = isTRUE(reprocess)
    )
  }, error = function(e) {
    ok <<- FALSE
    af2_log("AF2_B3:", "fetch_marketdata daily failed: ", conditionMessage(e))
  })

  if (!ok) {
    for (d in refdates) {
      tryCatch({
        meta <- rb3::download_marketdata("b3-cotahist-daily", refdate = d)
        rb3::read_marketdata(meta)
      }, error = function(e2) {
        if (verbose) af2_log("AF2_B3:", "daily fallback failed for ", d, ": ", conditionMessage(e2))
      })
    }
  }

  df_lazy <- tryCatch({
    rb3::cotahist_get("daily")
  }, error = function(e) {
    stop("cotahist_get('daily') failed: ", conditionMessage(e))
  })

  df_lazy |>
    dplyr::filter(.data$refdate >= start_date, .data$refdate <= end_date)
}