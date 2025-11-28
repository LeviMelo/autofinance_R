############################################################
# autofinance_portfolio_engine.R
#
# Pure portfolio construction layer:
#   Input : mu (expected returns), Sigma (covariance matrix),
#           port_config (constraints / mode)
#   Output: weights vector + some metadata
#
# Modes:
#   - "equal"     : equal weight
#   - "inv_vol"   : 1/σ weighting
#   - "min_var"   : minimum-variance QP
#   - "mean_var"  : Markowitz (risk aversion)
#   - "max_sharpe": approximated via QP on (mu - rf)
############################################################

if (!exists("af_attach_packages")) {
  if (file.exists("autofinance_config.R")) {
    source("autofinance_config.R")
  } else {
    stop("autofinance_config.R not found; please source it before portfolio engine.")
  }
}

# Precisamos ao menos de quadprog para os problemas quadráticos
af_attach_packages(c("quadprog"))

# -------------------------------------------------------------------
# Config default
# -------------------------------------------------------------------

af_port_default_config <- list(
  mode         = "min_var",  # "equal", "inv_vol", "min_var", "mean_var", "max_sharpe"
  long_only    = TRUE,
  w_max        = 0.20,       # máx 20% por ativo
  leverage_max = 1.0,        # sum(w) = 1; sem alavancagem
  risk_aversion = 5,         # para mean_var / max_sharpe (λ)
  rf_daily      = 0          # taxa livre de risco diária (pode vir do CDI / SELIC)
)

# -------------------------------------------------------------------
# Utilitários básicos de pesos
# -------------------------------------------------------------------

af_port_equal_weights <- function(symbols) {
  n <- length(symbols)
  if (n == 0L) stop("af_port_equal_weights: empty symbol set.")
  rep(1 / n, n)
}

af_port_inv_vol <- function(Sigma) {
  if (!is.matrix(Sigma)) stop("af_port_inv_vol: Sigma must be a matrix.")
  sig <- sqrt(diag(Sigma))
  inv <- 1 / sig
  inv[!is.finite(inv)] <- 0
  if (sum(inv) <= 0) {
    # Fallback: equal weights
    n <- length(sig)
    return(rep(1 / n, n))
  }
  inv / sum(inv)
}

# -------------------------------------------------------------------
# Resolver QP com quadprog (min_var / mean_var / max_sharpe)
# -------------------------------------------------------------------

af_quadprog_solve <- function(Sigma,
                              mu           = NULL,
                              mode         = c("min_var", "mean_var", "max_sharpe"),
                              long_only    = TRUE,
                              w_max        = 1,
                              leverage_max = 1,
                              risk_aversion = 5,
                              rf_daily     = 0) {
  mode <- match.arg(mode)

  if (!is.matrix(Sigma)) stop("af_quadprog_solve: Sigma must be a matrix.")
  N <- nrow(Sigma)
  if (N != ncol(Sigma)) stop("af_quadprog_solve: Sigma must be square.")

  # quadprog: solve.QP(Dmat, dvec, Amat, bvec, meq)
  # Objetivo canônico: min 1/2 w' D w - d' w
  Dmat <- 2 * Sigma

  if (mode == "min_var") {
    dvec <- rep(0, N)
  } else {
    if (is.null(mu) || length(mu) != N) {
      stop("af_quadprog_solve: mu must be length N for mean_var / max_sharpe.")
    }
    mu <- as.numeric(mu)

    if (mode == "mean_var") {
      # max (w'μ - λ w'Σw) ≈ min (λ w'Σw - w'μ)
      # → Dmat = 2 λ Σ, dvec = μ
      Dmat <- 2 * risk_aversion * Sigma
      dvec <- mu
    } else if (mode == "max_sharpe") {
      # Heurística: tratar Sharpe ~ (w'(μ - rf)) com penalização de risco
      # via λ -> similar a mean_var com μtilde = μ - rf
      mu_tilde <- mu - rf_daily
      Dmat <- 2 * risk_aversion * Sigma
      dvec <- mu_tilde
    }
  }

  # Restrições lineares: A^T w >= b (quadprog usa essa convenção)
  # 1) soma w_i = 1 -> representamos como igualdade via meq
  Aeq <- matrix(1, nrow = N, ncol = 1)
  beq <- 1

  # 2) w_i >= 0 se long_only
  A_ineq <- NULL
  b_ineq <- NULL
  if (long_only) {
    A_ineq <- diag(N)
    b_ineq <- rep(0, N)
  }

  # 3) w_i <= w_max -> -w_i >= -w_max
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

  # 4) leverage_max: com long_only e sum(w) = 1, leverage_max = 1 já está embutido.
  # Se futuramente tiver shorts, aqui precisa de restrição extra na soma(|w_i|).

  # Montagem Amat / bvec
  if (!is.null(A_ineq)) {
    # Igualdade vira as primeiras colunas de Amat, com meq = 1
    Amat <- cbind(Aeq, -Aeq, t(A_ineq))
    bvec <- c(beq, -beq, b_ineq)
    meq  <- 1
  } else {
    Amat <- cbind(Aeq, -Aeq)
    bvec <- c(beq, -beq)
    meq  <- 1
  }

  res <- quadprog::solve.QP(
    Dmat = Dmat,
    dvec = dvec,
    Amat = Amat,
    bvec = bvec,
    meq  = meq
  )

  w <- as.numeric(res$solution)
  names(w) <- rownames(Sigma)
  w
}

# -------------------------------------------------------------------
# Função principal: mu, Sigma -> pesos
# -------------------------------------------------------------------

af_build_portfolio <- function(mu,
                               Sigma,
                               config = af_port_default_config) {

  if (is.null(Sigma) || !is.matrix(Sigma)) {
    stop("af_build_portfolio: Sigma must be a covariance matrix.")
  }

  # Se mu for NULL e o modo precisar de mu, erro informativo
  mode <- match.arg(config$mode,
                    c("equal", "inv_vol", "min_var", "mean_var", "max_sharpe"))

  symbols <- colnames(Sigma)
  if (is.null(symbols)) {
    symbols <- paste0("Asset", seq_len(nrow(Sigma)))
    colnames(Sigma) <- rownames(Sigma) <- symbols
  }

  # Para equal / inv_vol, mu pode ser NULL
  if (!is.null(mu)) {
    if (length(mu) != nrow(Sigma)) {
      stop("af_build_portfolio: length(mu) must equal nrow(Sigma).")
    }
    names(mu) <- symbols
  }

  if (mode == "equal") {
    w <- af_port_equal_weights(symbols)
  } else if (mode == "inv_vol") {
    w <- af_port_inv_vol(Sigma)
  } else {
    w <- af_quadprog_solve(
      Sigma         = Sigma,
      mu            = mu,
      mode          = mode,
      long_only     = config$long_only,
      w_max         = config$w_max,
      leverage_max  = config$leverage_max,
      risk_aversion = config$risk_aversion,
      rf_daily      = config$rf_daily
    )
  }

  # Se long_only, garante w_i >= 0 numérica
  if (config$long_only) {
    w[w < 0] <- 0
  }

  # Normaliza para somar 1
  if (sum(w) > 0) {
    w <- w / sum(w)
  } else {
    # fallback defensivo: equal weights
    w <- af_port_equal_weights(symbols)
  }

  names(w) <- symbols

  list(
    weights = w,
    Sigma   = Sigma,
    mu      = mu,
    config  = config
  )
}
