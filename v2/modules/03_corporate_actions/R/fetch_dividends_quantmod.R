# v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R
source("v2/modules/03_corporate_actions/R/yahoo_retry.R")

af2_ca_fetch_dividends_one <- function(yahoo_symbol,
                                       from = "2018-01-01",
                                       to = Sys.Date(),
                                       verbose = FALSE,
                                       split.adjust = TRUE) {
  if (is.na(yahoo_symbol) || !nzchar(yahoo_symbol)) return(NULL)

  div_fun <- quantmod::getDividends
  fml <- names(formals(div_fun))

  call_args <- list(
    Symbols = yahoo_symbol,
    from = from,
    to = to,
    auto.assign = FALSE,
    verbose = verbose
  )

  if ("split.adjust" %in% fml) {
    call_args$split.adjust <- split.adjust
  }

  x <- af2_ca_with_retry(
    function() do.call(div_fun, call_args),
    max_tries = 4L,
    base_sleep = 1.5,
    verbose = verbose
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