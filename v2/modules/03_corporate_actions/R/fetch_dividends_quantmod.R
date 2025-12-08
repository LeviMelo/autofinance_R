# v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R

af2_ca_fetch_dividends_one <- function(yahoo_symbol,
                                       from = "2018-01-01",
                                       to = Sys.Date(),
                                       verbose = FALSE,
                                       split.adjust = TRUE) {
  if (is.na(yahoo_symbol) || !nzchar(yahoo_symbol)) return(NULL)

  x <- tryCatch(
    quantmod::getDividends(
      yahoo_symbol,
      from = from,
      to = to,
      auto.assign = FALSE,
      verbose = verbose,
      split.adjust = split.adjust
    ),
    error = function(e) {
      if (verbose) af2_log("AF2_CA:", "getDividends failed for ", yahoo_symbol, ": ", conditionMessage(e))
      NULL
    }
  )

  if (is.null(x)) return(NULL)
  if (!inherits(x, "xts")) return(NULL)
  if (NROW(x) == 0) return(NULL)

  dt <- data.table::data.table(
    yahoo_symbol = yahoo_symbol,
    refdate = as.Date(zoo::index(x)),
    action_type = "dividend",
    value = as.numeric(zoo::coredata(x)),
    source = "yahoo"
  )

  dt[is.finite(value) & value != 0 & !is.na(refdate)]
}