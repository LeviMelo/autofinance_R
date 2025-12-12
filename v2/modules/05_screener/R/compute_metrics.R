# v2/modules/05_screener/R/compute_metrics.R
# Feature engineering for one symbol (feature-vector output).
# Uses adjusted FINAL OHLC whenever available.

af2_compute_symbol_features <- function(dt_sym, horizons_days) {
  af2_require(c("data.table"))

  dt <- data.table::as.data.table(dt_sym)
  data.table::setorder(dt, refdate)

  n_obs <- nrow(dt)
  if (n_obs < 30L) return(NULL)

  # Ensure required columns exist
  af2_assert_cols(dt, c("symbol", "refdate", "close_adj_final"), name = "panel_adj(symbol)")

  # Normalize liquidity column name
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }
  if (!"turnover" %in% names(dt)) dt[, turnover := NA_real_]

  # Try to use OHLC adjusted final; fall back to close-only where needed
  if (!"open_adj_final" %in% names(dt)) dt[, open_adj_final := NA_real_]
  if (!"high_adj_final" %in% names(dt)) dt[, high_adj_final := NA_real_]
  if (!"low_adj_final"  %in% names(dt)) dt[, low_adj_final  := NA_real_]

  # Coverage
  n_valid <- sum(is.finite(dt$close_adj_final) & !is.na(dt$close_adj_final))
  coverage <- if (n_obs > 0) n_valid / n_obs else NA_real_

  # Daily returns (close-close)
  dt[, close_prev := data.table::shift(close_adj_final, 1L)]
  dt[, ret_cc_log := data.table::fifelse(
    is.finite(close_adj_final) & is.finite(close_prev) & close_adj_final > 0 & close_prev > 0,
    log(close_adj_final / close_prev),
    NA_real_
  )]
  dt[, ret_cc_simple := data.table::fifelse(
    is.finite(close_adj_final) & is.finite(close_prev) & close_prev != 0,
    (close_adj_final / close_prev) - 1,
    NA_real_
  )]

  # Open gap (open vs prev close)
  dt[, gap_oc := data.table::fifelse(
    is.finite(open_adj_final) & is.finite(close_prev) & open_adj_final > 0 & close_prev > 0,
    (open_adj_final / close_prev) - 1,
    NA_real_
  )]

  # Intraday range (high-low)
  dt[, range_hl_log := data.table::fifelse(
    is.finite(high_adj_final) & is.finite(low_adj_final) &
      high_adj_final > 0 & low_adj_final > 0 & high_adj_final >= low_adj_final,
    log(high_adj_final / low_adj_final),
    NA_real_
  )]
  dt[, range_hl_pct := data.table::fifelse(
    is.finite(high_adj_final) & is.finite(low_adj_final) & is.finite(close_adj_final) &
      close_adj_final > 0 & high_adj_final >= low_adj_final,
    (high_adj_final - low_adj_final) / close_adj_final,
    NA_real_
  )]

  # Candle body (|close-open| / open)
  dt[, body_pct := data.table::fifelse(
    is.finite(open_adj_final) & is.finite(close_adj_final) & open_adj_final > 0,
    abs(close_adj_final - open_adj_final) / open_adj_final,
    NA_real_
  )]

  # True range % (ATR-like)
  dt[, tr := NA_real_]
  dt[is.finite(high_adj_final) & is.finite(low_adj_final),
     tr := high_adj_final - low_adj_final]
  dt[is.finite(tr) & is.finite(close_prev),
     tr := pmax(
       tr,
       abs(high_adj_final - close_prev),
       abs(low_adj_final - close_prev),
       na.rm = TRUE
     )]
  dt[, tr_pct := data.table::fifelse(
    is.finite(tr) & is.finite(close_prev) & close_prev > 0,
    tr / close_prev,
    NA_real_
  )]

  out <- list(
    symbol = dt$symbol[1],
    end_date = dt$refdate[n_obs],
    end_refdate = dt$refdate[n_obs],
    n_obs = as.integer(n_obs),
    n_valid = as.integer(n_valid),
    coverage = as.numeric(coverage)
  )

  # Helper: safe horizon return using row index (business-day count proxy)
  safe_price_ret <- function(p0, p1) {
    if (!is.finite(p0) || !is.finite(p1)) return(NA_real_)
    if (p0 == 0) return(NA_real_)
    (p1 / p0) - 1
  }

  safe_finite <- function(x) x[is.finite(x)]

  safe_mean <- function(x) {
    x <- safe_finite(x)
    if (!length(x)) return(NA_real_)
    mean(x)
  }
  
  safe_median <- function(x) {
    x <- safe_finite(x)
    if (!length(x)) return(NA_real_)
    stats::median(x)
  }
  
  safe_min <- function(x) {
    x <- safe_finite(x)
    if (!length(x)) return(NA_real_)
    min(x)
  }
  
  safe_max <- function(x) {
    x <- safe_finite(x)
    if (!length(x)) return(NA_real_)
    max(x)
  }
  
  safe_sd <- function(x) {
    x <- safe_finite(x)
    if (length(x) < 2L) return(NA_real_)
    stats::sd(x)
  }

  # Multi-horizon features
  last_idx <- n_obs
  for (h in horizons_days) {
    h <- as.integer(h)

    # Return over horizon (close->close)
    if (h > 1L && (last_idx - h) >= 1L) {
      p0 <- dt$close_adj_final[last_idx - h]
      p1 <- dt$close_adj_final[last_idx]
      out[[paste0("ret_", h, "d")]] <- safe_price_ret(p0, p1)

      # Close-close vol (annualized)
      idx_start <- max(1L, last_idx - h + 1L)
      rwin <- dt$ret_cc_log[idx_start:last_idx]
      out[[paste0("vol_cc_", h, "d")]] <- safe_sd(rwin) * sqrt(252)
      out[[paste0("vol_", h, "d")]] <- out[[paste0("vol_cc_", h, "d")]]
      
      # Parkinson volatility from HL range (annualized)
      x <- dt$range_hl_log[idx_start:last_idx]
      if (sum(is.finite(x)) >= 5L) {
        out[[paste0("vol_pk_", h, "d")]] <- sqrt(mean(x^2, na.rm = TRUE) / (4 * log(2))) * sqrt(252)
      } else {
        out[[paste0("vol_pk_", h, "d")]] <- NA_real_
      }

      # Garman–Klass volatility (annualized) - requires O,H,L,C
      o <- dt$open_adj_final[idx_start:last_idx]
      hi <- dt$high_adj_final[idx_start:last_idx]
      lo <- dt$low_adj_final[idx_start:last_idx]
      c <- dt$close_adj_final[idx_start:last_idx]
      ok <- is.finite(o) & is.finite(hi) & is.finite(lo) & is.finite(c) &
        o > 0 & hi > 0 & lo > 0 & c > 0 & hi >= lo
      if (sum(ok) >= 5L) {
        log_hl <- log(hi[ok] / lo[ok])
        log_co <- log(c[ok] / o[ok])
        sig2 <- mean(0.5 * (log_hl^2) - (2 * log(2) - 1) * (log_co^2), na.rm = TRUE)
        out[[paste0("vol_gk_", h, "d")]] <- sqrt(pmax(sig2, 0)) * sqrt(252)
      } else {
        out[[paste0("vol_gk_", h, "d")]] <- NA_real_
      }

      # Intraday range stats
      rp <- dt$range_hl_pct[idx_start:last_idx]
      out[[paste0("range_mean_", h, "d")]] <- safe_mean(rp)
      out[[paste0("range_max_", h, "d")]]  <- safe_max(rp)

      # Gap stats
      gp <- dt$gap_oc[idx_start:last_idx]
      out[[paste0("gap_min_", h, "d")]] <- safe_min(gp)
      out[[paste0("gap_max_", h, "d")]] <- safe_max(gp)
      out[[paste0("gap_abs_med_", h, "d")]] <- safe_median(abs(gp))

      # ATR-like stats
      trp <- dt$tr_pct[idx_start:last_idx]
      out[[paste0("tr_mean_", h, "d")]] <- safe_mean(trp)

      # Body stats
      bp <- dt$body_pct[idx_start:last_idx]
      out[[paste0("body_mean_", h, "d")]] <- safe_mean(bp)

    } else {
      # Populate missing horizons to keep schema stable
      out[[paste0("ret_", h, "d")]] <- NA_real_
      out[[paste0("vol_cc_", h, "d")]] <- NA_real_
      out[[paste0("vol_", h, "d")]] <- NA_real_
      out[[paste0("vol_pk_", h, "d")]] <- NA_real_
      out[[paste0("vol_gk_", h, "d")]] <- NA_real_
      out[[paste0("range_mean_", h, "d")]] <- NA_real_
      out[[paste0("range_max_", h, "d")]] <- NA_real_
      out[[paste0("gap_min_", h, "d")]] <- NA_real_
      out[[paste0("gap_max_", h, "d")]] <- NA_real_
      out[[paste0("gap_abs_med_", h, "d")]] <- NA_real_
      out[[paste0("tr_mean_", h, "d")]] <- NA_real_
      out[[paste0("body_mean_", h, "d")]] <- NA_real_
    }
  }

  # Drawdown + ulcer (NA-robust)
  prices <- dt$close_adj_final
  idx <- which(is.finite(prices) & !is.na(prices) & prices > 0)
  
  if (length(idx) >= 2L) {
    p <- prices[idx]
    cm <- cummax(p)
    dd <- p / cm - 1
  
    out$max_dd <- min(dd)  # dd is finite here
    out$ulcer_index <- sqrt(mean((dd * 100)^2))
  } else {
    out$max_dd <- NA_real_
    out$ulcer_index <- NA_real_
  }

  # Liquidity features
  traded_flag <- is.finite(dt$close_adj_final) & !is.na(dt$close_adj_final)
  out$median_turnover <- stats::median(dt$turnover, na.rm = TRUE)
  out$days_traded_ratio <- mean(traded_flag, na.rm = TRUE)

  # Amihud-like illiquidity
  valid <- is.finite(dt$ret_cc_simple) & is.finite(dt$turnover) & dt$turnover > 0
  out$amihud <- if (any(valid)) {
    mean(abs(dt$ret_cc_simple[valid]) / dt$turnover[valid], na.rm = TRUE)
  } else NA_real_

  data.table::as.data.table(out)
}

# -------------------------------------------------------------------
# Compatibility alias (Module 05 expects this name)
# -------------------------------------------------------------------
af2_compute_symbol_metrics <- function(dt_sym, horizons_days) {
  af2_compute_symbol_features(dt_sym, horizons_days)
}
