############################################################
# autofinance_screener.R  (VERSÃO ATUALIZADA)
# Métricas cross-section + ranking por classe
############################################################

af_screener_config_default <- list(
  lookback_days   = 252L,
  horizons_days   = c(21L, 63L, 126L, 252L),
  min_liquidity   = 5e5,
  min_days_traded = 0.8,        # 80% dos dias com negócio
  ibov_series_id  = "IBOV",     # se você salvar IBOV em macro_series
  usd_series_id   = "USD_BR",   # idem
  # Peso alto em momentum médio/longo; curto com peso menor.
  # Penalização forte de drawdown/ulcer; moderada de vol, beta, liquidez ruim.
  score_weights   = list(
    # Momentum (quanto maior melhor)
    ret_21d        = +0.3,
    ret_63d        = +0.6,
    ret_126d       = +0.9,
    ret_252d       = +1.0,

    # Risco / estabilidade (quanto menor melhor)
    vol_21d        = -0.4,
    vol_252d       = -0.7,
    max_dd         = -0.7,
    ulcer_index    = -0.8,
    avg_time_underwater = -0.3,

    # Liquidez (Amihud alto = pior, então peso negativo)
    amihud         = -0.5,

    # Sistema (se disponível)
    beta_ibov      = -0.2,  # penaliza beta de mercado muito alto
    beta_usd       = +0.1   # leve prêmio para hedge em dólar (opcional)
    # Se quiser incluir skew/kurt/var/cvar: adicione aqui, mas pense na direção.
  )
)

############################################################
# Filtro de liquidez em cima de prices_raw
############################################################

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
  if (nrow(dt) == 0L) return(data.table::data.table(symbol = character(0)))

  dt[, refdate := as.Date(refdate)]
  data.table::setorder(dt, symbol, refdate)

  liq <- dt[, .(
    median_vol_fin    = stats::median(vol_fin, na.rm = TRUE),
    days_traded_ratio = mean(qty > 0, na.rm = TRUE)
  ), by = symbol]

  liq[is.na(median_vol_fin),    median_vol_fin := 0]
  liq[is.na(days_traded_ratio), days_traded_ratio := 0]

  liq_filtered <- liq[
    median_vol_fin    >= min_liquidity &
      days_traded_ratio >= min_days_traded
  ]

  liq_filtered
}

############################################################
# Métricas por ativo (multi-horizonte + risco completo)
############################################################

af_compute_symbol_metrics <- function(dt_sym,
                                      horizons_days,
                                      factor_returns = NULL) {
  # dt_sym: data.table(date, close_adj, ret_simple, excess_ret_simple, vol_fin, qty)
  # horizons_days: vetor de janelas (em nº de observações)
  # factor_returns: list com vetores alinhados: ibov, usd, etc. (opcional)

  n <- nrow(dt_sym)
  if (n < 20L) return(NULL)  # pouco dado

  last_idx <- n
  metrics <- list(symbol = dt_sym$symbol[1])

  # -------------------------
  # 1) Retornos / Momentum
  # -------------------------
  for (h in horizons_days) {
    if (h <= n) {
      idx <- (last_idx - h + 1):last_idx
      ret_window <- prod(1 + dt_sym$excess_ret_simple[idx], na.rm = TRUE) - 1
      metrics[[paste0("ret_", h, "d")]] <- ret_window
      vol_window <- stats::sd(dt_sym$excess_ret_simple[idx], na.rm = TRUE) * sqrt(252)
      metrics[[paste0("vol_", h, "d")]] <- vol_window
    }
  }

  # -------------------------
  # 2) Drawdown, Ulcer, underwater
  # -------------------------
  prices <- dt_sym$close_adj
  cummax_p <- cummax(prices)
  dd <- prices / cummax_p - 1
  max_dd <- min(dd, na.rm = TRUE)

  ui <- sqrt(mean((dd * 100)^2, na.rm = TRUE))  # Ulcer Index em pontos percentuais

  underwater <- dd < 0
  if (any(underwater, na.rm = TRUE)) {
    rleid <- data.table::rleid(underwater)
    seg <- data.table::data.table(
      rleid = rleid,
      underwater = underwater
    )[, .N, by = .(rleid, underwater)]
    lengths <- seg[underwater == TRUE]$N
    avg_underwater <- mean(lengths, na.rm = TRUE)
  } else {
    avg_underwater <- 0
  }

  metrics$max_dd              <- max_dd
  metrics$ulcer_index         <- ui
  metrics$avg_time_underwater <- avg_underwater

  # -------------------------
  # 3) Liquidez Amihud
  # -------------------------
  valid <- dt_sym$vol_fin > 0 & !is.na(dt_sym$ret_simple)
  if (any(valid)) {
    amihud <- mean(abs(dt_sym$ret_simple[valid]) / dt_sym$vol_fin[valid], na.rm = TRUE)
  } else {
    amihud <- NA_real_
  }
  metrics$amihud <- amihud

  # -------------------------
  # 4) Skew, Kurt, VaR, CVaR
  # -------------------------
  x <- dt_sym$excess_ret_simple
  x <- x[is.finite(x)]
  if (length(x) > 20L) {
    m <- mean(x, na.rm = TRUE)
    s <- stats::sd(x, na.rm = TRUE)
    if (s > 0) {
      skew <- mean(((x - m) / s)^3, na.rm = TRUE)
      kurt <- mean(((x - m) / s)^4, na.rm = TRUE)
    } else {
      skew <- NA_real_; kurt <- NA_real_
    }
    q <- stats::quantile(x, 0.05, na.rm = TRUE)
    var_95  <- q
    cvar_95 <- mean(x[x <= q], na.rm = TRUE)
  } else {
    skew <- kurt <- NA_real_
    var_95 <- cvar_95 <- NA_real_
  }
  metrics$skew    <- skew
  metrics$kurt    <- kurt
  metrics$var_95  <- var_95
  metrics$cvar_95 <- cvar_95

  # -------------------------
  # 5) Betas / correlações vs fatores se fornecidos
  # -------------------------
  if (!is.null(factor_returns)) {
    fr <- factor_returns
    safe_beta <- function(a, b) {
      ok <- is.finite(a) & is.finite(b)
      if (sum(ok) < 20L) return(NA_real_)
      v <- stats::var(b[ok])
      if (v <= 0) return(NA_real_)
      stats::cov(a[ok], b[ok]) / v
    }
    safe_corr <- function(a, b) {
      ok <- is.finite(a) & is.finite(b)
      if (sum(ok) < 20L) return(NA_real_)
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

############################################################
# Função principal: af_run_screener()
############################################################

af_run_screener <- function(panel = NULL,
                            ref_date = Sys.Date(),
                            as_of_date = NULL,
                            config = af_screener_config_default,
                            con = af_db_connect()) {
  # If a panel is provided, operate purely in-memory (Option A).
  # Otherwise, pull from DB (legacy path).
  own_con <- is.null(panel)
  if (own_con && !is.null(con)) on.exit(af_db_disconnect(con), add = TRUE)
  af_attach_packages("data.table")

  ref_date <- as.Date(if (is.null(as_of_date)) ref_date else as_of_date)
  lookback_days <- config$lookback_days
  lookback_start <- ref_date - lookback_days * 2

  if (is.null(panel)) {
    # 1) filtro de liquidez com prices_raw
    liq <- af_compute_basic_liquidity_filter(
      con             = con,
      min_liquidity   = config$min_liquidity,
      min_days_traded = config$min_days_traded,
      lookback_start  = lookback_start,
      ref_date        = ref_date
    )
    if (nrow(liq) == 0L) {
      stop("af_run_screener: no liquid symbols found.")
    }
    symbols_liq <- liq$symbol

    # 2) painel ajustado + retornos
    panel <- af_build_adjusted_panel(con, symbols_liq, lookback_start, ref_date)
    panel_ret <- af_compute_returns(panel)
  } else {
    panel_ret <- data.table::as.data.table(panel)
    if (!all(c("symbol", "refdate") %in% names(panel_ret))) {
      stop("panel must contain symbol/refdate columns.")
    }
    panel_ret[, refdate := as.Date(refdate)]
    panel_ret <- panel_ret[refdate >= lookback_start & refdate <= ref_date]
    # ensure returns exist
    if (!("ret_simple" %in% names(panel_ret))) {
      panel_ret <- af_compute_returns(panel_ret)
    }
  }

  # 3) fatores macro (ex: IBOV, USD) - DB path only
  macro_needed <- unique(c(config$ibov_series_id, config$usd_series_id))
  macro_needed <- macro_needed[!is.na(macro_needed)]
  macro <- if (length(macro_needed) > 0 && own_con) {
    af_get_macro_series(con, macro_needed, lookback_start, ref_date)
  } else {
    data.table::data.table()
  }

  factor_returns <- list()
  if (nrow(macro) > 0L) {
    macro_wide <- data.table::dcast(macro, refdate ~ series_id, value.var = "value")
    macro_wide <- macro_wide[order(refdate)]
    # convert levels to returns (pct change) if possible
    for (sid in macro_needed) {
      if (sid %in% names(macro_wide)) {
        vals <- macro_wide[[sid]]
        rts <- c(NA_real_, diff(vals) / head(vals, -1))
        nm <- if (grepl("IBOV", sid, ignore.case = TRUE)) {
          "ibov"
        } else if (grepl("USD", sid, ignore.case = TRUE)) {
          "usd"
        } else {
          tolower(sid)
        }
        factor_returns[[nm]] <- rts
      }
    }
  }

  # 4) limitar para última janela exata de lookback_days por símbolo
  data.table::setorder(panel_ret, symbol, refdate)
  metrics_list <- list()
  for (sym in unique(panel_ret$symbol)) {
    dt_sym <- panel_ret[symbol == sym]
    # pega últimos lookback_days observações (ou todas se < lookback_days)
    if (nrow(dt_sym) > lookback_days) {
      dt_sym <- dt_sym[(.N - lookback_days + 1):.N]
    }
    # fall back to ret_simple if excess not available
    if (!("excess_ret_simple" %in% names(dt_sym))) {
      dt_sym[, excess_ret_simple := ret_simple]
    }
    fr_sym <- NULL
    if (length(factor_returns) > 0L) {
      fr_sym <- lapply(factor_returns, function(x) {
        if (length(x) >= nrow(dt_sym)) tail(x, nrow(dt_sym)) else rep(NA_real_, nrow(dt_sym))
      })
    }
    m <- af_compute_symbol_metrics(
      dt_sym,
      horizons_days  = config$horizons_days,
      factor_returns = fr_sym
    )
    if (!is.null(m)) metrics_list[[sym]] <- m
  }

  metrics <- data.table::rbindlist(metrics_list, fill = TRUE)

  # 5) anexar asset_type da assets_meta (se existir, DB path)
  if (own_con) {
    meta <- data.table::as.data.table(
      DBI::dbGetQuery(con, "SELECT symbol, asset_type FROM assets_meta")
    )
    metrics <- meta[metrics, on = .(symbol)]
  }

  # 6) escore com z-score por métrica
  w <- config$score_weights
  metrics[, score := 0]

  for (nm in names(w)) {
    if (!nm %in% names(metrics)) next
    x <- metrics[[nm]]
    if (all(is.na(x))) next
    mu <- mean(x, na.rm = TRUE)
    s  <- stats::sd(x, na.rm = TRUE)
    if (is.na(s) || s == 0) next
    z <- (x - mu) / s
    metrics[, score := score + w[[nm]] * z]
  }

  # 7) ranking geral e por tipo
  metrics[, rank_overall := rank(-score, ties.method = "first")]
  if ("asset_type" %in% names(metrics)) {
    metrics[, rank_type := rank(-score, ties.method = "first"), by = asset_type]
  } else {
    metrics[, rank_type := NA_integer_]
  }

  list(
    full    = metrics[order(rank_overall)],
    by_type = if ("asset_type" %in% names(metrics)) split(metrics[order(asset_type, rank_type)], metrics$asset_type) else NULL
  )
}
