# v2/modules/03_corporate_actions/R/fetch_events_yahoo_chart.R
# Fetch splits + dividends in ONE call via Yahoo chart endpoint.
# Returns data.table with schema:
# yahoo_symbol, refdate, action_type (split|dividend), value, source

source("v2/modules/03_corporate_actions/R/yahoo_retry.R")

af2_ca_fetch_events_yahoo_chart_one <- function(yahoo_symbol,
                                               from = "2018-01-01",
                                               to = Sys.Date(),
                                               verbose = FALSE) {
  if (is.na(yahoo_symbol) || !nzchar(yahoo_symbol)) return(NULL)

  af2_ca_require(c("data.table", "jsonlite", "curl"))

  from <- as.Date(from)
  to   <- as.Date(to)

  # Yahoo wants unix seconds
  p1 <- as.integer(as.POSIXct(from, tz = "UTC"))
  p2 <- as.integer(as.POSIXct(to + 1, tz = "UTC"))  # inclusive-ish

  url <- sprintf(
    "https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&events=div%%7Csplit",
    utils::URLencode(yahoo_symbol, reserved = TRUE),
    p1, p2
  )

  h <- curl::new_handle()
  curl::handle_setheaders(h,
    "User-Agent" = "Mozilla/5.0",
    "Accept" = "application/json"
  )

  fetch_once <- function() {
    resp <- curl::curl_fetch_memory(url, handle = h)
    status <- resp$status_code
    txt <- rawToChar(resp$content)

    if (status == 429) stop("HTTP 429 rate limit", call. = FALSE)
    if (status >= 400) stop(paste0("HTTP ", status, " on yahoo chart"), call. = FALSE)

    jsonlite::fromJSON(txt, simplifyVector = FALSE)
  }

  js <- af2_ca_with_retry(fetch_once, max_tries = 4L, base_sleep = 1.5, verbose = verbose)
  if (is.null(js)) return(NULL)

  res <- js$chart$result
  if (is.null(res) || !length(res)) return(NULL)

  ev <- res[[1]]$events
  if (is.null(ev)) return(NULL)

  out_list <- list()

  # --- dividends ---
  divs <- ev$dividends
  if (!is.null(divs) && length(divs)) {
    # divs often keyed by timestamp strings; values are objects with $date, $amount
    div_dt <- data.table::rbindlist(lapply(divs, function(d) {
      if (is.null(d$date) || is.null(d$amount)) return(NULL)
      data.table::data.table(
        yahoo_symbol = yahoo_symbol,
        refdate = as.Date(as.POSIXct(as.integer(d$date), origin = "1970-01-01", tz = "UTC")),
        action_type = "dividend",
        value = as.numeric(d$amount),
        source = "yahoo"
      )
    }), use.names = TRUE, fill = TRUE)

    if (nrow(div_dt)) out_list[["dividend"]] <- div_dt
  }

  # --- splits ---
  splits <- ev$splits
  if (!is.null(splits) && length(splits)) {
    split_dt <- data.table::rbindlist(lapply(splits, function(s) {
      if (is.null(s$date) || is.null(s$numerator) || is.null(s$denominator)) return(NULL)

      num <- as.numeric(s$numerator)
      den <- as.numeric(s$denominator)
      if (!is.finite(num) || !is.finite(den) || num <= 0 || den <= 0) return(NULL)

      # PRICE FACTOR = denominator/numerator (e.g. 2-for-1 => 1/2 = 0.5; 1-for-20 reverse => 20/1 = 20)
      pf <- den / num

      data.table::data.table(
        yahoo_symbol = yahoo_symbol,
        refdate = as.Date(as.POSIXct(as.integer(s$date), origin = "1970-01-01", tz = "UTC")),
        action_type = "split",
        value = as.numeric(pf),
        source = "yahoo"
      )
    }), use.names = TRUE, fill = TRUE)

    if (nrow(split_dt)) out_list[["split"]] <- split_dt
  }

  out <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  if (!nrow(out)) return(NULL)

  out <- out[is.finite(value) & !is.na(refdate)]
  out
}
