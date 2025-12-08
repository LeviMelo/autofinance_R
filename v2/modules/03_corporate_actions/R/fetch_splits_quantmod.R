# v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R

af2_ca_fetch_splits_one <- function(yahoo_symbol,
                                    from = "2018-01-01",
                                    to = Sys.Date(),
                                    verbose = FALSE) {
  if (is.na(yahoo_symbol) || !nzchar(yahoo_symbol)) return(NULL)

  x <- tryCatch(
    quantmod::getSplits(
      yahoo_symbol,
      from = from,
      to = to,
      auto.assign = FALSE,
      verbose = verbose
    ),
    error = function(e) {
      if (verbose) af2_log("AF2_CA:", "getSplits failed for ", yahoo_symbol, ": ", conditionMessage(e))
      NULL
    }
  )

  # quantmod returns NA if no split data
  if (is.null(x)) return(NULL)
  if (!inherits(x, "xts")) return(NULL)
  if (NROW(x) == 0) return(NULL)
  if (length(x) == 1 && is.na(as.numeric(x))) return(NULL)

  # xts index = date, coredata = ratio
  dt <- data.table::data.table(
    yahoo_symbol = yahoo_symbol,
    refdate = as.Date(zoo::index(x)),
    action_type = "split",
    value = as.numeric(zoo::coredata(x)),
    source = "yahoo"
  )

  dt[is.finite(value) & !is.na(refdate)]
}