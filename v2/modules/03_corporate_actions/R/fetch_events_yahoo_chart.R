# v2/modules/03_corporate_actions/R/fetch_events_yahoo_chart.R
source("v2/modules/03_corporate_actions/R/yahoo_retry.R")

af2_ca_fetch_events_yahoo_chart_one <- function(yahoo_symbol,
                                               from = "2018-01-01",
                                               to = Sys.Date(),
                                               verbose = FALSE) {
  # Default empty table structure
  empty_dt <- data.table::data.table(
      yahoo_symbol = character(),
      refdate = as.Date(character()),
      action_type = character(),
      value = numeric(),
      source = character()
  )

  if (is.na(yahoo_symbol) || !nzchar(yahoo_symbol)) return(empty_dt)

  # Check dependencies (Crucial for parallel workers)
  if (!requireNamespace("jsonlite", quietly = TRUE) || 
      !requireNamespace("curl", quietly = TRUE)) {
      if(verbose) message("AF2_CA_ERR: jsonlite or curl missing on worker for ", yahoo_symbol)
      return(empty_dt)
  }

  af2_ca_require(c("data.table", "jsonlite", "curl"))

  from <- as.Date(from)
  to   <- as.Date(to)
  p1 <- as.integer(as.POSIXct(from, tz = "UTC"))
  p2 <- as.integer(as.POSIXct(to + 1, tz = "UTC"))

  # Corrected URL (Plural splits)
  url <- sprintf(
    "https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d&events=div%%7Csplits",
    utils::URLencode(yahoo_symbol, reserved = TRUE),
    p1, p2
  )

  h <- curl::new_handle()
  curl::handle_setheaders(h, "User-Agent" = "Mozilla/5.0", "Accept" = "application/json")

  fetch_once <- function() {
    resp <- curl::curl_fetch_memory(url, handle = h)
    status <- resp$status_code
    txt <- rawToChar(resp$content)
    
    if (status == 429) stop("HTTP 429", call. = FALSE)
    if (status == 404) return(NULL) # Symbol not found
    if (status >= 400) stop(paste0("HTTP ", status), call. = FALSE)
    
    # Safely parse JSON
    tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error=function(e) NULL)
  }

  js <- af2_ca_with_retry(fetch_once, max_tries = 4L, verbose = verbose)
  
  # Handle failures gracefully
  if (is.null(js)) {
      if(verbose) message("AF2_CA_WARN: No JSON returned for ", yahoo_symbol)
      return(empty_dt)
  }

  res <- js$chart$result[[1]]
  if (is.null(res)) return(empty_dt)
  ev <- res$events
  if (is.null(ev)) return(empty_dt)

  out_list <- list()

  # Process Dividends
  if (!is.null(ev$dividends)) {
    out_list[["dividend"]] <- data.table::rbindlist(lapply(ev$dividends, function(d) {
      if (is.null(d$date) || is.null(d$amount)) return(NULL)
      data.table::data.table(
        yahoo_symbol = yahoo_symbol,
        refdate = as.Date(as.POSIXct(as.integer(d$date), origin = "1970-01-01", tz = "UTC")),
        action_type = "dividend",
        value = as.numeric(d$amount),
        source = "yahoo"
      )
    }), fill=TRUE)
  }

  # Process Splits
  if (!is.null(ev$splits)) {
    out_list[["split"]] <- data.table::rbindlist(lapply(ev$splits, function(s) {
      if (is.null(s$date) || is.null(s$numerator) || is.null(s$denominator)) return(NULL)
      pf <- as.numeric(s$denominator) / as.numeric(s$numerator)
      data.table::data.table(
        yahoo_symbol = yahoo_symbol,
        refdate = as.Date(as.POSIXct(as.integer(s$date), origin = "1970-01-01", tz = "UTC")),
        action_type = "split",
        value = pf,
        source = "yahoo"
      )
    }), fill=TRUE)
  }

  out <- data.table::rbindlist(out_list, fill = TRUE)
  
  if (nrow(out) > 0) {
      out <- out[is.finite(value) & !is.na(refdate)]
      # REMOVED: The erroneous 100x scaling fix. 
      # BRIM11 and others need the raw value.
      return(out)
  }
  
  return(empty_dt)
}