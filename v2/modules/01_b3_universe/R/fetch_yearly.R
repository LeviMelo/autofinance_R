# v2/modules/01_b3_universe/R/fetch_yearly.R
# Fetch one year with rb3, then return *lazy* dataset handle

af2_b3_fetch_yearly_lazy <- function(year, cfg = NULL, verbose = TRUE,
                                     force_download = FALSE,
                                     reprocess = FALSE) {
  cfg <- cfg %||% af2_get_config()
  af2_b3_init_rb3(cfg, verbose = verbose)

  year <- as.integer(year)

  if (verbose) af2_log("AF2_B3:", "fetch_marketdata yearly: ", year)

  ok <- TRUE
  tryCatch({
    rb3::fetch_marketdata(
      "b3-cotahist-yearly",
      year = year,
      throttle = TRUE,
      force_download = isTRUE(force_download),
      reprocess = isTRUE(reprocess)
    )
  }, error = function(e) {
    ok <<- FALSE
    af2_log("AF2_B3:", "fetch_marketdata failed for ", year, ": ", conditionMessage(e))
  })

  if (!ok) {
    # Fallback path
    tryCatch({
      meta <- rb3::download_marketdata("b3-cotahist-yearly", year = year)
      rb3::read_marketdata(meta)
      af2_log("AF2_B3:", "fallback download/read OK for ", year)
    }, error = function(e2) {
      stop("rb3 fallback download/read failed for ", year, ": ", conditionMessage(e2))
    })
  }

  df_lazy <- tryCatch({
    rb3::cotahist_get("yearly")
  }, error = function(e) {
    stop("cotahist_get('yearly') failed: ", conditionMessage(e))
  })

  # Always filter by year lazily
  df_lazy <- df_lazy |> dplyr::filter(lubridate::year(.data$refdate) == year)

  df_lazy
}
