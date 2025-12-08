# v2/modules/03_corporate_actions/R/yahoo_retry.R

af2_ca_is_rate_limit_error <- function(msg) {
  if (is.null(msg) || !nzchar(msg)) return(FALSE)
  grepl("429|Too Many Requests|rate limit", msg, ignore.case = TRUE)
}

af2_ca_with_retry <- function(fun,
                              max_tries = 4L,
                              base_sleep = 1.5,
                              jitter = 0.2,
                              verbose = FALSE) {

  max_tries <- as.integer(max_tries)
  if (is.na(max_tries) || max_tries < 1L) max_tries <- 1L

  for (k in seq_len(max_tries)) {
    out <- tryCatch(fun(), error = function(e) e)

    if (!inherits(out, "error")) return(out)

    msg <- conditionMessage(out)

    # Only backoff if it looks like rate limiting
    if (!af2_ca_is_rate_limit_error(msg)) {
      if (verbose) af2_log("AF2_CA:", "Non-429 error: ", msg)
      return(NULL)
    }

    # Backoff
    if (k < max_tries) {
      sleep_s <- base_sleep * (2^(k - 1))
      sleep_s <- sleep_s * runif(1, 1 - jitter, 1 + jitter)
      if (verbose) {
        af2_log("AF2_CA:", "429 detected. Retry ", k, "/", max_tries,
                " sleeping ~", round(sleep_s, 2), "s")
      }
      Sys.sleep(sleep_s)
    } else {
      if (verbose) af2_log("AF2_CA:", "429 persisted after retries.")
    }
  }

  NULL
}
