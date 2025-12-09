# v2/modules/05_screener/R/compute_metrics.R

af2_compute_symbol_metrics <- function(dt_sym, horizons_days) {
  af2_require("data.table")
  dt_sym <- data.table::as.data.table(dt_sym)
  data.table::setorder(dt_sym, refdate)

  n <- nrow(dt_sym)
  if (n < 30L) return(NULL)

  # Returns from adjusted final price
  dt_sym[, close_lag := data.table::shift(close_adj_final, 1L), by = symbol]
  dt_sym[, ret_simple := ifelse(
    !is.na(close_lag) & close_lag != 0,
    (close_adj_final / close_lag) - 1,
    NA_real_
  )]
  dt_sym[, close_lag := NULL]

  last_idx <- n
  out <- list(symbol = dt_sym$symbol[1])

  # Momentum + short/long vol
  # PATCH 2025-12-08: robust horizon indexing + NA-safe return calc
  safe_price_ret <- function(p0, p1) {
    if (is.na(p0) || is.na(p1)) return(NA_real_)
    if (!is.finite(p0) || !is.finite(p1)) return(NA_real_)
    if (p0 == 0) return(NA_real_)
    (p1 / p0) - 1
  }

  for (h in horizons_days) {
    h <- as.integer(h)

    # Need at least h+1 points to compute a price-to-price horizon
    if (h > 1L && (last_idx - h) >= 1L) {

      idx0 <- last_idx - h
      p0 <- dt_sym$close_adj_final[idx0]
      p1 <- dt_sym$close_adj_final[last_idx]

      out[[paste0("ret_", h, "d")]] <- safe_price_ret(p0, p1)

      # Vol over last h observations of daily returns
      idx_start <- max(1L, last_idx - h + 1L)
      rwin <- dt_sym$ret_simple[idx_start:last_idx]
      out[[paste0("vol_", h, "d")]] <- stats::sd(rwin, na.rm = TRUE) * sqrt(252)

    } else {
      # Explicitly populate missing horizons to avoid silent shape drift
      out[[paste0("ret_", h, "d")]] <- NA_real_
      out[[paste0("vol_", h, "d")]] <- NA_real_
    }
  }

  # Drawdown + Ulcer (NA-robust)
  prices <- dt_sym$close_adj_final
  if (requireNamespace("zoo", quietly = TRUE)) {
    prices <- zoo::na.locf(prices, na.rm = FALSE)
  }

  cummax_p <- cummax(prices)
  dd <- prices / cummax_p - 1
  out$max_dd <- suppressWarnings(min(dd, na.rm = TRUE))
  out$ulcer_index <- sqrt(mean((dd * 100)^2, na.rm = TRUE))

  # Amihud-like illiquidity
  if (!"turnover" %in% names(dt_sym) && "vol_fin" %in% names(dt_sym)) {
    dt_sym[, turnover := vol_fin]
  }
  valid <- is.finite(dt_sym$ret_simple) & is.finite(dt_sym$turnover) & dt_sym$turnover > 0
  out$amihud <- if (any(valid)) {
    mean(abs(dt_sym$ret_simple[valid]) / dt_sym$turnover[valid], na.rm = TRUE)
  } else NA_real_

  data.table::as.data.table(out)
}
