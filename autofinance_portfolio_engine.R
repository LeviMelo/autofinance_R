############################################################
# autofinance_portfolio_engine.R
# Construção de portfólio com restrições
############################################################

af_port_default_config <- list(
  cov_method    = "sample",
  mu_method     = "mean",
  window_years  = 3,
  mode          = "min_var",    # "equal", "inv_vol", "min_var", "mean_var", "max_sharpe"
  long_only     = TRUE,
  w_max         = 0.20,
  leverage_max  = 1.0,
  group_constraints = NULL,     # ex: list(FII_min = 0.2, BDR_max = 0.3)
  risk_aversion = 5             # para mean_var
)

af_port_equal_weights <- function(symbols) {
  n <- length(symbols)
  rep(1 / n, n)
}

af_port_inv_vol <- function(Sigma) {
  sig <- sqrt(diag(Sigma))
  inv <- 1 / sig
  inv[!is.finite(inv)] <- 0
  if (sum(inv) == 0) return(rep(1 / length(inv), length(inv)))
  inv / sum(inv)
}

af_quadprog_solve <- function(Sigma,
                              mu = NULL,
                              mode = c("min_var", "mean_var", "max_sharpe"),
                              long_only = TRUE,
                              w_max = 1,
                              leverage_max = 1,
                              risk_aversion = 5,
                              rf_rate = 0) {
  mode <- match.arg(mode)
  if (!requireNamespace("quadprog", quietly = TRUE)) {
    stop("Package 'quadprog' required for quadratic optimization.")
  }

  N <- nrow(Sigma)
  Dmat <- 2 * Sigma
  # Objetivo: 1/2 w' D w - d' w
  if (mode == "min_var") {
    dvec <- rep(0, N)
  } else if (mode == "mean_var") {
    # max (w'μ - λ w'Σw) → min (λ w'Σw - w'μ)
    # transformado em padrão quadprog
    dvec <- mu
    # o fator λ entra implicitamente pela escala de Σ; aqui simplificado
    Dmat <- 2 * risk_aversion * Sigma
  } else if (mode == "max_sharpe") {
    # approx: max Sharpe ≈ max ( (w'μ - rf)/σ ) → usar heurística:
    # max (w' (μ - rf)) com restrição de var <= 1? Complicado.
    # Simples: treat as mean_var com μtilde = μ - rf e λ pequeno
    dvec <- mu - rf_rate
    Dmat <- 2 * risk_aversion * Sigma
  }

  # Restrições: A^T w >= b
  # 1) soma w_i = 1 → (1,...,1) w = 1 → tratamos como igualdade, representado como duas desigualdades:
  # (1,...,1) w >= 1 e (-1,...,-1) w >= -1
  Aeq <- matrix(1, nrow = N, ncol = 1)
  beq <- 1

  # 2) w_i >= 0 se long_only
  A_ineq <- NULL
  b_ineq <- NULL
  if (long_only) {
    A_ineq <- diag(N)
    b_ineq <- rep(0, N)
  }

  # 3) w_i <= w_max → -w_i >= -w_max
  if (!is.null(w_max) && w_max < 1) {
    A_ineq2 <- -diag(N)
    b_ineq2 <- rep(-w_max, N)
    if (is.null(A_ineq)) {
      A_ineq <- A_ineq2
      b_ineq <- b_ineq2
    } else {
      A_ineq <- rbind(A_ineq, A_ineq2)
      b_ineq <- c(b_ineq, b_ineq2)
    }
  }

  # 4) leverage_max: sum(|w_i|) <= L (simplificado: sum(w_i) <= L se long_only)
  # já temos sum(w_i) = 1, então leverage_max = 1 está implícito. Se >1, ignoramos aqui.

  # Montagem final de A e b (quadprog: Amat, bvec, meq)
  if (!is.null(A_ineq)) {
    A_all <- cbind(Aeq, -Aeq, t(A_ineq))
    b_all <- c(beq, -beq, b_ineq)
    meq <- 1  # primeira coluna (Aeq) tratada como igualdade? quadprog usa 'meq' primeiras colunas de Amat como igualdades
  } else {
    A_all <- cbind(Aeq, -Aeq)
    b_all <- c(beq, -beq)
    meq <- 1
  }

  res <- quadprog::solve.QP(
    Dmat = Dmat,
    dvec = dvec,
    Amat = A_all,
    bvec = b_all,
    meq = meq
  )

  w <- res$solution
  names(w) <- rownames(Sigma)
  w
}

af_build_portfolio <- function(panel_returns,
                               selected_symbols,
                               port_config = af_port_default_config,
                               metrics_table = NULL,
                               rf_daily_series = NULL,
                               end_date = max(panel_returns$date)) {
  af_attach_packages("data.table")
  dt <- data.table::as.data.table(panel_returns)
  dt <- dt[symbol %in% selected_symbols]
  if (nrow(dt) == 0L) stop("af_build_portfolio: no data for selected_symbols.")

  risk_cfg <- af_risk_config_default
  risk_cfg$cov_method <- port_config$cov_method
  risk_cfg$mu_method  <- port_config$mu_method

  risk_est <- af_risk_estimate(
    panel_returns  = dt,
    end_date       = end_date,
    config         = risk_cfg,
    metrics_table  = metrics_table,
    rf_daily_series = rf_daily_series
  )

  mu    <- risk_est$mu
  Sigma <- risk_est$Sigma

  mode <- match.arg(port_config$mode,
                    c("equal", "inv_vol", "min_var", "mean_var", "max_sharpe"))

  if (mode == "equal") {
    w <- af_port_equal_weights(names(mu))
  } else if (mode == "inv_vol") {
    w <- af_port_inv_vol(Sigma)
  } else {
    w <- af_quadprog_solve(
      Sigma        = Sigma,
      mu           = mu,
      mode         = mode,
      long_only    = port_config$long_only,
      w_max        = port_config$w_max,
      leverage_max = port_config$leverage_max,
      risk_aversion = port_config$risk_aversion
    )
  }

  # garantir soma 1 (numérica)
  w[w < 0 & port_config$long_only] <- 0
  if (sum(w) > 0) w <- w / sum(w)

  list(
    weights = w,
    cov_mat = Sigma,
    mu_vec  = mu
  )
}
