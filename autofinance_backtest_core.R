############################################################
# autofinance_backtest_core.R
# Time-series backtest: screener + risk + portfolio
############################################################

if (!exists("af_attach_packages")) {
  if (file.exists("autofinance_config.R")) {
    source("autofinance_config.R")
  } else {
    stop("autofinance_config.R not found; please source it before using backtest.")
  }
}

af_attach_packages(c("data.table", "lubridate", "zoo"))

if (!exists("af_run_screener")) {
  if (file.exists("autofinance_screener.R")) {
    source("autofinance_screener.R")
  } else {
    stop("autofinance_screener.R not found; required for backtest.")
  }
}

if (!exists("af_risk_build")) {
  if (file.exists("autofinance_risk_models.R")) {
    source("autofinance_risk_models.R")
  } else {
    stop("autofinance_risk_models.R not found; required for backtest.")
  }
}

if (!exists("af_build_portfolio")) {
  if (file.exists("autofinance_portfolio_engine.R")) {
    source("autofinance_portfolio_engine.R")
  } else {
    stop("autofinance_portfolio_engine.R not found; required for backtest.")
  }
}


# Compute rebalance dates (e.g. last trading day of each month)
af_bt_compute_rebalance_dates <- function(panel,
                                          start_date,
                                          end_date,
                                          rebalance_freq = "monthly",
                                          lookback_years = 3L) {
  stopifnot("refdate" %in% names(panel))
  dt <- data.table::copy(panel)
  dt[, refdate := as.Date(refdate)]

  dates <- sort(unique(dt$refdate))
  dates <- dates[dates >= start_date & dates <= end_date]
  if (!length(dates)) stop("No trading dates in panel for given backtest range.")

  dt <- data.table::data.table(refdate = dates)

  if (rebalance_freq == "monthly") {
    dt[, `:=`(
      year  = lubridate::year(refdate),
      month = lubridate::month(refdate)
    )]
    reb <- dt[, .(reb_date = max(refdate)), by = .(year, month)][order(reb_date), reb_date]
  } else if (rebalance_freq == "quarterly") {
    dt[, `:=`(
      year    = lubridate::year(refdate),
      quarter = lubridate::quarter(refdate)
    )]
    reb <- dt[, .(reb_date = max(refdate)), by = .(year, quarter)][order(reb_date), reb_date]
  } else if (rebalance_freq == "weekly") {
    dt[, `:=`(
      year = lubridate::year(refdate),
      week = lubridate::isoweek(refdate)
    )]
    reb <- dt[, .(reb_date = max(refdate)), by = .(year, week)][order(reb_date), reb_date]
  } else {
    stop("Unsupported rebalance_freq: ", rebalance_freq)
  }

  min_reb_date <- start_date %m+% lubridate::years(lookback_years)
  reb <- reb[reb >= min_reb_date & reb <= end_date]
  reb
}

# Select symbols from screener result according to config
af_bt_select_symbols <- function(screener_res, screener_config) {
  # Accept either a data.table or the full table from af_run_screener
  if (is.null(screener_res)) return(character(0))
  if (is.list(screener_res) && "full" %in% names(screener_res)) {
    screener_res <- screener_res$full
  }
  if (!nrow(screener_res)) return(character(0))
  if (!"score" %in% names(screener_res)) {
    stop("screener_res must have a 'score' column.")
  }

  res <- data.table::copy(screener_res)
  data.table::setorder(res, -score)

  # Per-type selection if requested and asset_type available
  top_by_type <- screener_config$top_n_by_type
  if (!is.null(top_by_type) && "asset_type" %in% names(res)) {
    sel <- character(0)
    for (tp in names(top_by_type)) {
      k <- as.integer(top_by_type[[tp]])
      if (is.na(k) || k <= 0) next
      tmp <- res[asset_type == tp][1:k, symbol]
      sel <- c(sel, tmp)
    }
    return(unique(sel))
  }

  # Simple Top-N
  top_n <- screener_config$top_n
  if (is.null(top_n) || top_n <= 0) {
    top_n <- min(20L, nrow(res))
  } else {
    top_n <- min(as.integer(top_n), nrow(res))
  }

  res[1:top_n, symbol]
}

# Compute portfolio-level performance statistics
af_bt_compute_stats <- function(equity_dt, returns_dt) {
  eq    <- equity_dt$equity
  dates <- equity_dt$refdate
  if (length(eq) < 2L) {
    return(list())
  }

  # Prefer the logged portfolio returns if available
  r <- returns_dt$port_ret
  r <- r[!is.na(r)]
  if (length(r) < 2L) {
    # fallback: derive from equity
    r <- diff(eq) / head(eq, -1L)
  }

  n_days <- length(r)
  if (n_days < 2L) {
    return(list())
  }

  horizon_years <- as.numeric(difftime(tail(dates, 1L),
                                       head(dates, 1L),
                                       units = "days")) / 365.25
  horizon_years <- max(horizon_years, 1e-9)

  # CAGR from equity
  cagr <- (tail(eq, 1L) / head(eq, 1L))^(1 / horizon_years) - 1

  # Annualized mean/vol (assuming daily freq)
  mean_daily <- mean(r, na.rm = TRUE)
  sd_daily   <- stats::sd(r, na.rm = TRUE)
  ann_return <- mean_daily * 252
  ann_vol    <- sd_daily * sqrt(252)
  sharpe     <- if (ann_vol > 0) ann_return / ann_vol else NA_real_

  # Drawdown
  cummax_eq <- cummax(eq)
  dd <- eq / cummax_eq - 1
  max_dd <- min(dd, na.rm = TRUE)

  # Ulcer index (using fractional drawdown)
  ulcer <- sqrt(mean((dd)^2, na.rm = TRUE))

  list(
    start_date    = head(dates, 1L),
    end_date      = tail(dates, 1L),
    cagr          = cagr,
    ann_return    = ann_return,
    ann_vol       = ann_vol,
    sharpe        = sharpe,
    max_drawdown  = max_dd,
    ulcer_index   = ulcer
  )
}

# Main backtest function
af_backtest <- function(panel,
                        screener_config,
                        risk_config,
                        port_config,
                        rebalance_freq = "monthly",
                        lookback_years = 3L,
                        start_date = NULL,
                        end_date   = NULL) {
  stopifnot("symbol"  %in% names(panel),
            "refdate" %in% names(panel))

  dt <- data.table::copy(panel)
  dt[, refdate := as.Date(refdate)]

  # --- FIX: Dynamic column selection ---
  if ("excess_ret_simple" %in% names(dt)) {
    ret_col <- "excess_ret_simple"
  } else if ("ret_simple" %in% names(dt)) {
    ret_col <- "ret_simple"
  } else {
    stop("Panel must contain 'excess_ret_simple' or 'ret_simple'.")
  }

  all_dates <- sort(unique(dt$refdate))
  if (is.null(start_date)) start_date <- min(all_dates)
  if (is.null(end_date))   end_date   <- max(all_dates)
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  # Keep enough history for initial lookback
  dt <- dt[refdate >= (start_date - lubridate::years(lookback_years)) &
           refdate <= end_date]

  # Dates actually used for equity curve
  dates_bt <- sort(unique(dt[refdate >= start_date & refdate <= end_date, refdate]))
  nD <- length(dates_bt)
  if (nD < 2L) {
    stop("Not enough dates in backtest window.")
  }

  reb_dates <- af_bt_compute_rebalance_dates(
    panel         = dt,
    start_date    = start_date,
    end_date      = end_date,
    rebalance_freq = rebalance_freq,
    lookback_years = lookback_years
  )
  if (!length(reb_dates)) {
    stop("No rebalance dates found. Check lookback_years and date range.")
  }

  # Initialize equity and logs
  equity <- rep(NA_real_, nD)
  names(equity) <- as.character(dates_bt)
  equity[1L] <- 1

  returns_log <- data.table::data.table(
    refdate = dates_bt,
    port_ret = NA_real_
  )
  weights_log <- list()

  current_weights <- NULL

  for (k in seq_along(reb_dates)) {
    t_reb <- reb_dates[k]

    # Lookback window for screener/risk
    window_start <- t_reb %m-% lubridate::years(lookback_years)
    panel_window <- dt[refdate >= window_start & refdate <= t_reb]

    # 1) Screener on full universe in the window
    scr_t <- af_run_screener(
      panel      = panel_window,
      config     = screener_config,
      as_of_date = t_reb
    )

    scr_table <- if (is.list(scr_t) && "full" %in% names(scr_t)) scr_t$full else scr_t
    selected <- af_bt_select_symbols(scr_table, screener_config)
    if (!length(selected)) {
      warning(sprintf("No symbols selected at rebalance %s. Keeping previous weights.",
                      as.character(t_reb)))
      if (is.null(current_weights)) next
    } else {
      # 2) Risk model on selected subset
      extra_args <- risk_config$extra_args
      if (is.null(extra_args)) extra_args <- list()
      risk_t <- do.call(
        af_risk_build,
        c(
          list(
            panel        = panel_window,
            symbols      = selected,
            end_date     = t_reb,
            window_years = risk_config$window_years,
            cov_method   = risk_config$cov_method,
            mu_method    = risk_config$mu_method
          ),
          extra_args
        )
      )

      # 3) Portfolio optimization
      port_t <- af_build_portfolio(
        mu     = risk_t$mu,
        Sigma  = risk_t$Sigma,
        config = port_config
      )
      current_weights <- port_t$weights

      weights_log[[as.character(t_reb)]] <- data.table::data.table(
        refdate = t_reb,
        symbol  = names(current_weights),
        weight  = as.numeric(current_weights)
      )
    }

    # 4) Apply these weights from next day until next rebalance
    idx_reb <- which(dates_bt == t_reb)
    if (!length(idx_reb)) next  # e.g. t_reb not an actual trading date

    idx_start <- idx_reb + 1L
    if (k < length(reb_dates)) {
      idx_end <- which(dates_bt == reb_dates[k + 1L])
    } else {
      idx_end <- nD
    }
    if (idx_start > idx_end) next
    if (is.null(current_weights)) next  # no portfolio yet

    for (i in idx_start:idx_end) {
      d <- dates_bt[i]
      day_ret <- dt[refdate == d & symbol %in% names(current_weights),
                    .(symbol, ret = get(ret_col))]

      if (nrow(day_ret) == 0L) {
        port_ret_d <- 0
      } else {
        data.table::setkey(day_ret, symbol)
        w_vec   <- current_weights[day_ret$symbol]
        ret_vec <- day_ret$ret
        port_ret_d <- sum(w_vec * ret_vec, na.rm = TRUE)
      }

      returns_log[refdate == d, port_ret := port_ret_d]

      if (i == 1L) {
        equity[i] <- 1 * (1 + port_ret_d)
      } else {
        if (is.na(equity[i - 1L])) {
          equity[i - 1L] <- equity[i - 2L]
        }
        equity[i] <- equity[i - 1L] * (1 + port_ret_d)
      }
    }
  }

  # Fill any remaining NAs in equity curve (e.g. before first rebalance)
  equity <- zoo::na.locf(equity, fromLast = FALSE, na.rm = FALSE)
  equity_dt <- data.table::data.table(
    refdate = dates_bt,
    equity  = equity
  )

  weights_dt <- if (length(weights_log)) {
    data.table::rbindlist(weights_log, use.names = TRUE, fill = TRUE)
  } else {
    data.table::data.table(refdate = as.Date(NA),
                           symbol  = NA_character_,
                           weight  = NA_real_)
  }

  stats <- af_bt_compute_stats(equity_dt, returns_log)

  list(
    equity_curve    = equity_dt,
    returns         = returns_log,
    weights         = weights_dt,
    rebalance_dates = reb_dates,
    stats           = stats
  )
}
