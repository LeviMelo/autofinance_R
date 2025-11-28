############################################################
# autofinance_screener.R
# Métricas cross-section + ranking por classe
############################################################

af_screener_config_default <- list(
  lookback_days   = 252L,
  horizons_days   = c(21L, 63L, 126L, 252L),
  min_liquidity   = 5e5,
  min_days_traded = 0.8,
  ibov_series_id  = "IBOV",
  usd_series_id   = "USD_BR",
  # Weights are now on *factors*, not raw metrics
  score_weights   = list(
    mom_short      = 0.35,   # ret_21d
    mom_medium     = 0.30,   # ret_63d
    mom_long       = 0.15,   # ret_126d/252d
    risk_vol       = -0.15,  # vol_252d
    risk_drawdown  = -0.20,  # max_dd
    risk_ulcer     = -0.10,  # ulcer_index
    risk_tail      = -0.10,  # cvar_95
    liq_turnover   = 0.10,   # median_vol_fin
    liq_depth      = 0.05,   # days_traded_ratio
    beta_abs       = -0.05   # |beta_ibov|
  )
)

af_compute_basic_liquidity_filter <- function(con,
                                              min_liquidity,
                                              min_days_traded,
                                              lookback_start,
                                              ref_date) {
  af_attach_packages("data.table")
  q <- sprintf("
    SELECT symbol, refdate, vol_fin, qty
    FROM prices_raw
    WHERE refdate >= '%s' AND refdate <= '%s'
  ", lookback_start, ref_date)
  dt <- data.table::as.data.table(DBI::dbGetQuery(con, q))
  if (nrow(dt) == 0L) {
    return(data.table::data.table(
      symbol           = character(0),
      median_vol_fin   = numeric(0),
      days_traded_ratio = numeric(0)
    ))
  }

  dt[, refdate := as.Date(refdate)]
  data.table::setorder(dt, symbol, refdate)

  liq <- dt[, .(
    median_vol_fin    = stats::median(vol_fin, na.rm = TRUE),
    days_traded_ratio = mean(qty > 0, na.rm = TRUE)
  ), by = symbol]

  liq[is.na(median_vol_fin),    median_vol_fin := 0]
  liq[is.na(days_traded_ratio), days_traded_ratio := 0]

  # Mantém todas as colunas; o filtro é feito no screener
  liq
}


af_compute_symbol_metrics <- function(dt_sym,
                                      horizons_days,
                                      factor_returns = NULL) {
  # dt_sym: data.table(date, close_adj, ret_simple, excess_ret_simple, vol_fin, qty)
  # horizons_days: vetor de janelas (em n obs)
  # factor_returns: list com vetores alinhados: ibov, usd, etc. (opcional)

  n <- nrow(dt_sym)
  if (n < 10L) return(NULL)  # pouco dado

  # vamos trabalhar só com a porção final (já passada pelo af_run_screener)
  # assume dt_sym já está limitado ao lookback desejado.

  # Retornos/momentum
  last_idx <- n
  metrics <- list(symbol = dt_sym$symbol[1])

  for (h in horizons_days) {
    if (h <= n) {
      ret_window <- prod(1 + dt_sym$excess_ret_simple[(last_idx - h + 1):last_idx], na.rm = TRUE) - 1
      metrics[[paste0("ret_", h, "d")]] <- ret_window
      vol_window <- stats::sd(dt_sym$excess_ret_simple[(last_idx - h + 1):last_idx], na.rm = TRUE) * sqrt(252)
      metrics[[paste0("vol_", h, "d")]] <- vol_window
    }
  }

  # Drawdown & Ulcer
  prices <- dt_sym$close_adj
  cummax_p <- cummax(prices)
  dd <- prices / cummax_p - 1
  max_dd <- min(dd, na.rm = TRUE)
  ui <- sqrt(mean((dd * 100)^2, na.rm = TRUE))  # UI em porcentagem

  underwater <- dd < 0
  if (any(underwater, na.rm = TRUE)) {
    rleid <- data.table::rleid(underwater)
    lengths <- dt_sym[, .N, by = rleid][underwater[match(rleid, rleid)] == TRUE]$N
    avg_underwater <- mean(lengths, na.rm = TRUE)
  } else {
    avg_underwater <- 0
  }

  metrics$max_dd <- max_dd
  metrics$ulcer_index <- ui
  metrics$avg_time_underwater <- avg_underwater

  # Liquidez Amihud
  # |ret| / vol_fin, média
  valid <- dt_sym$vol_fin > 0 & !is.na(dt_sym$ret_simple)
  if (any(valid)) {
    amihud <- mean(abs(dt_sym$ret_simple[valid]) / dt_sym$vol_fin[valid], na.rm = TRUE)
  } else {
    amihud <- NA_real_
  }
  metrics$amihud <- amihud

  # Skew/Kurt
  x <- dt_sym$excess_ret_simple
  x <- x[is.finite(x)]
  if (length(x) > 10L) {
    m <- mean(x, na.rm = TRUE)
    s <- stats::sd(x, na.rm = TRUE)
    if (s > 0) {
      skew <- mean(((x - m) / s)^3, na.rm = TRUE)
      kurt <- mean(((x - m) / s)^4, na.rm = TRUE)
    } else {
      skew <- NA_real_
      kurt <- NA_real_
    }
  } else {
    skew <- kurt <- NA_real_
  }
  metrics$skew <- skew
  metrics$kurt <- kurt

  # VaR/CVaR 95%
  if (length(x) > 10L) {
    q <- stats::quantile(x, 0.05, na.rm = TRUE)
    metrics$var_95 <- q
    metrics$cvar_95 <- mean(x[x <= q], na.rm = TRUE)
  } else {
    metrics$var_95 <- NA_real_
    metrics$cvar_95 <- NA_real_
  }

  # Betas simples vs fatores se fornecidos
  if (!is.null(factor_returns)) {
    fr <- factor_returns
    # assume vetores já alinhados em mesmo tamanho n
    safe_beta <- function(a, b) {
      ok <- is.finite(a) & is.finite(b)
      if (sum(ok) < 10L) return(NA_real_)
      v <- stats::var(b[ok])
      if (v <= 0) return(NA_real_)
      stats::cov(a[ok], b[ok]) / v
    }
    safe_corr <- function(a, b) {
      ok <- is.finite(a) & is.finite(b)
      if (sum(ok) < 10L) return(NA_real_)
      stats::cor(a[ok], b[ok])
    }
    if (!is.null(fr$ibov)) {
      metrics$beta_ibov <- safe_beta(dt_sym$excess_ret_simple, fr$ibov)
      metrics$corr_ibov <- safe_corr(dt_sym$excess_ret_simple, fr$ibov)
    }
    if (!is.null(fr$usd)) {
      metrics$beta_usd <- safe_beta(dt_sym$excess_ret_simple, fr$usd)
      metrics$corr_usd <- safe_corr(dt_sym$excess_ret_simple, fr$usd)
    }
  }

  data.table::as.data.table(metrics)
}

af_run_screener <- function(ref_date = Sys.Date(),
                            config = af_screener_config_default,
                            con = af_db_connect()) {
  on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("data.table")

  ref_date <- as.Date(ref_date)
  lookback_days <- config$lookback_days
  # vamos puxar um pouco mais de janela para garantir N observações úteis
  lookback_start <- ref_date - lookback_days * 2

  # 1) filtro de liquidez com prices_raw
  liq_all <- af_compute_basic_liquidity_filter(
    con             = con,
    min_liquidity   = config$min_liquidity,
    min_days_traded = config$min_days_traded,
    lookback_start  = lookback_start,
    ref_date        = ref_date
  )

  # aplica filtro de liquidez mas guarda métricas
  liq <- liq_all[
    median_vol_fin    >= config$min_liquidity &
      days_traded_ratio >= config$min_days_traded
  ]

  if (nrow(liq) == 0L) {
    stop("af_run_screener: no liquid symbols found.")
  }
  symbols_liq <- liq$symbol


  # 2) painel ajustado + retornos
  panel <- af_build_adjusted_panel(con, symbols_liq, lookback_start, ref_date)
  panel_ret <- af_compute_returns(panel)

  # 3) fatores macro (ex: IBOV, USD) – assumindo que você salvou como retornos ou preços
  macro_needed <- unique(c(config$ibov_series_id, config$usd_series_id))
  macro_needed <- macro_needed[!is.na(macro_needed)]
  macro <- if (length(macro_needed) > 0) {
    af_get_macro_series(con, macro_needed, lookback_start, ref_date)
  } else {
    data.table::data.table()
  }

  factor_returns <- list()
  if (nrow(macro) > 0L) {
    macro_wide <- data.table::dcast(macro, refdate ~ series_id, value.var = "value")
    macro_wide <- macro_wide[order(refdate)]
    # aqui assumo que já são retornos; se forem níveis, você converte
    if (!is.null(config$ibov_series_id) && config$ibov_series_id %in% names(macro_wide)) {
      factor_returns$ibov <- macro_wide[[config$ibov_series_id]]
    }
    if (!is.null(config$usd_series_id) && config$usd_series_id %in% names(macro_wide)) {
      factor_returns$usd <- macro_wide[[config$usd_series_id]]
    }
  }

  # 4) limitar para última janela exata de lookback_days por símbolo
  data.table::setorder(panel_ret, symbol, date)
  metrics_list <- list()
  for (sym in unique(panel_ret$symbol)) {
    dt_sym <- panel_ret[symbol == sym]
    # pega últimos lookback_days observações
    if (nrow(dt_sym) > lookback_days) {
      dt_sym <- dt_sym[(.N - lookback_days + 1):.N]
    }
    fr_sym <- NULL
    if (length(factor_returns) > 0L) {
      fr_sym <- lapply(factor_returns, function(x) {
        if (length(x) >= nrow(dt_sym)) tail(x, nrow(dt_sym)) else NA_real_
      })
    }
    m <- af_compute_symbol_metrics(
      dt_sym,
      horizons_days = config$horizons_days,
      factor_returns = fr_sym
    )
    if (!is.null(m)) metrics_list[[sym]] <- m
  }

  metrics <- data.table::rbindlist(metrics_list, fill = TRUE)

  # 5) anexar asset_type da assets_meta
  meta <- data.table::as.data.table(
    DBI::dbGetQuery(con, "SELECT symbol, asset_type FROM assets_meta")
  )
  metrics <- meta[metrics, on = .(symbol)]

  # 6) escore simples por weights
  w <- config$score_weights
  metrics[, score := 0]
  for (nm in names(w)) {
    if (!nm %in% names(metrics)) next
    x <- metrics[[nm]]
    # padroniza z-score
    if (all(is.na(x))) next
    mu <- mean(x, na.rm = TRUE); s <- stats::sd(x, na.rm = TRUE)
    if (s == 0 || is.na(s)) next
    z <- (x - mu) / s
    metrics[, score := score + w[[nm]] * z]
  }

  # 7) ranking por asset_type
  metrics[, rank_overall := rank(-score, ties.method = "first")]
  metrics[, rank_type    := rank(-score, ties.method = "first"), by = asset_type]

  list(
    full = metrics,
    by_type = split(metrics[order(asset_type, rank_type)], metrics$asset_type)
  )
}
