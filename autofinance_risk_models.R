############################################################
# autofinance_risk_models.R
# Estimativa de Σ e μ (sample, shrink, GARCH/DCC, VAR)
############################################################

af_risk_config_default <- list(
  cov_method   = "sample",      # "sample", "shrinkage", "garch_dcc"
  mu_method    = "mean",        # "mean", "momentum", "var"
  window_years = 3,
  rf_series_id = "CDI",
  shrink_lambda = 0.2,
  var_lags      = 1
)

af_risk_prepare_window <- function(panel_returns,
                                   end_date,
                                   window_years) {
  af_attach_packages("data.table")
  end_date <- as.Date(end_date)
  start_date <- end_date - 365 * window_years
  dt <- data.table::as.data.table(panel_returns)
  dt <- dt[date >= start_date & date <= end_date]
  if (nrow(dt) == 0L) stop("af_risk_prepare_window: no data in window.")
  dt
}

af_cov_sample <- function(ret_mat) {
  stats::cov(ret_mat, use = "pairwise.complete.obs")
}

af_cov_shrink_diag <- function(ret_mat, lambda = 0.2) {
  S <- stats::cov(ret_mat, use = "pairwise.complete.obs")
  diagS <- diag(diag(S))
  lambda * diagS + (1 - lambda) * S
}

af_cov_garch_dcc <- function(ret_mat) {
  if (!requireNamespace("rugarch", quietly = TRUE) ||
      !requireNamespace("rmgarch", quietly = TRUE)) {
    stop("Packages 'rugarch' and 'rmgarch' required for garch_dcc.")
  }
  # ret_mat: matrix T x N
  uspec <- rugarch::ugarchspec(
    variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
    mean.model = list(armaOrder = c(0, 0), include.mean = TRUE),
    distribution.model = "norm"
  )
  mspec <- rugarch::multispec(replicate(ncol(ret_mat), uspec))
  dccspec <- rmgarch::dccspec(
    uspec = mspec,
    dccOrder = c(1, 1),
    distribution = "mvnorm"
  )
  fit <- rmgarch::dccfit(dccspec, data = ret_mat)
  cov_arr <- rmgarch::rcov(fit)  # N x N x T
  # pega última matriz de covariância
  cov_last <- cov_arr[, , dim(cov_arr)[3]]
  cov_last
}

af_mu_mean <- function(ret_mat, rf_daily = NULL) {
  # ret_mat: T x N de retornos simples ou excess_ret_simple
  # se rf_daily não for NULL e ret_mat for bruto, subtrair
  if (!is.null(rf_daily)) {
    if (length(rf_daily) >= nrow(ret_mat)) {
      rf <- rf_daily[(length(rf_daily) - nrow(ret_mat) + 1):length(rf_daily)]
      ret_mat <- ret_mat - matrix(rf, nrow = length(rf), ncol = ncol(ret_mat))
    }
  }
  mu_daily <- colMeans(ret_mat, na.rm = TRUE)
  mu_annual <- (1 + mu_daily)^252 - 1
  mu_annual
}

af_mu_from_momentum <- function(metrics_table,
                                symbols) {
  # simples: pega ret_252d (ou máximo disponível) do screener e reescala
  dt <- metrics_table[symbol %in% symbols]
  if (!"ret_252d" %in% names(dt)) {
    stop("metrics_table must contain ret_252d for momentum-based mu.")
  }
  ret <- dt$ret_252d
  names(ret) <- dt$symbol
  ret
}

af_mu_var_assets <- function(ret_mat, lags = 1L) {
  if (!requireNamespace("vars", quietly = TRUE)) {
    stop("Package 'vars' required for VAR μ.")
  }
  df <- as.data.frame(ret_mat)
  colnames(df) <- paste0("A", seq_len(ncol(df)))
  var_model <- vars::VAR(df, p = lags, type = "const")
  pred <- predict(var_model, n.ahead = 1)
  mu <- sapply(pred$fcst, function(x) x[1, "fcst"])
  # daily forecast -> convertemos para anual approx: (1+μ)^252-1??
  # mas μ aqui já é retorno esperado 1-step; deixamos como diário e convertimos depois:
  mu_annual <- (1 + mu)^252 - 1
  mu_annual
}

af_risk_estimate <- function(panel_returns,
                             end_date,
                             config = af_risk_config_default,
                             metrics_table = NULL,
                             rf_daily_series = NULL) {
  af_attach_packages("data.table")
  dt <- af_risk_prepare_window(panel_returns, end_date, config$window_years)
  # panel_returns deve ter: symbol, date, excess_ret_simple (ou ret_simple)
  data.table::setorder(dt, date)
  # pivot para matriz T x N
  ret_wide <- data.table::dcast(
    dt,
    date ~ symbol,
    value.var = "excess_ret_simple"
  )
  dates <- ret_wide$date
  ret_mat <- as.matrix(ret_wide[, -1, with = FALSE])

  # Σ
  cov_method <- match.arg(config$cov_method, c("sample", "shrinkage", "garch_dcc"))
  Sigma <- switch(
    cov_method,
    "sample"   = af_cov_sample(ret_mat),
    "shrinkage" = af_cov_shrink_diag(ret_mat, lambda = config$shrink_lambda),
    "garch_dcc" = af_cov_garch_dcc(ret_mat)
  )

  # μ
  mu_method <- match.arg(config$mu_method, c("mean", "momentum", "var"))
  if (mu_method == "mean") {
    mu <- af_mu_mean(ret_mat, rf_daily = rf_daily_series)
  } else if (mu_method == "momentum") {
    if (is.null(metrics_table)) {
      stop("metrics_table required for mu_method = 'momentum'")
    }
    symbols <- colnames(ret_mat)
    mu_vec <- af_mu_from_momentum(metrics_table, symbols)
    # reordenar na ordem das colunas de ret_mat
    mu <- mu_vec[match(symbols, names(mu_vec))]
  } else if (mu_method == "var") {
    mu <- af_mu_var_assets(ret_mat, lags = config$var_lags)
  }

  names(mu) <- colnames(ret_mat)
  rownames(Sigma) <- colnames(ret_mat)
  colnames(Sigma) <- colnames(ret_mat)

  list(
    mu = mu,
    Sigma = Sigma,
    dates = dates
  )
}
