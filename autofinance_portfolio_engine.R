############################################################
# autofinance_portfolio_engine.R
# Portfolio construction given μ, Σ and constraints
############################################################

source("autofinance_config.R")
af_attach_packages(c("data.table", "quadprog"))

# ----------------------------------------------------------
# 1. Default config
# ----------------------------------------------------------

af_port_default_config <- list(
  mode          = "min_var",  # "equal", "inv_vol", "min_var", "mean_var", "max_sharpe"
  long_only     = TRUE,
  w_max         = 0.20,       # per-asset cap
  leverage_max  = 1.0,        # currently only meaningful if we allow shorts in future
  risk_aversion = 5,          # λ for mean-variance; higher = more risk-averse
  rf_annual     = 0,          # annual RF used for Sharpe in reporting

  # Group constraints: list of entries like:
  # list(
  #   name    = "FIIs",
  #   symbols = c("HGLG11", "KNRI11"),
  #   min     = 0.10,   # optional
  #   max     = 0.40    # optional
  # )
  group_constraints = NULL
)

# Small helper: make sure config has defaults if user passes a partial list
af_port_merge_config <- function(config = list()) {
  cfg <- af_port_default_config
  if (!is.null(config) && length(config) > 0L) {
    for (nm in names(config)) {
      cfg[[nm]] <- config[[nm]]
    }
  }
  cfg
}

# ----------------------------------------------------------
# 2. Simple heuristic portfolios: equal-weight, inverse-vol
# ----------------------------------------------------------

af_port_equal_weights <- function(symbols) {
  n <- length(symbols)
  if (n == 0L) stop("af_port_equal_weights: length(symbols) == 0")
  rep(1 / n, n)
}

af_port_inv_vol <- function(Sigma) {
  if (is.null(dim(Sigma))) stop("af_port_inv_vol: Sigma must be a matrix.")
  sig <- sqrt(diag(Sigma))
  inv <- 1 / sig
  inv[!is.finite(inv)] <- 0
  if (sum(inv) <= 0) {
    # fall back to equal weight
    return(rep(1 / length(inv), length(inv)))
  }
  inv / sum(inv)
}

# ----------------------------------------------------------
# 3. Build linear constraints for quadprog
# ----------------------------------------------------------
# quadprog solves:
#   min 1/2 w' D w - d' w
#   s.t. A^T w >= b
#
# We represent:
# - sum(w) = 1       → two inequalities: sum(w) >= 1  AND  -sum(w) >= -1
# - w_i >= 0         → diag(N)
# - w_i <= w_max     → -w_i >= -w_max
# - group min        → sum_{i in G} w_i >= min_G
# - group max        → -sum_{i in G} w_i >= -max_G
#
# The first "meq" columns in Amat are treated as equalities by quadprog,
# so we keep sum(w) = 1 as equality.

af_port_build_constraints <- function(symbols,
                                      long_only     = TRUE,
                                      w_max         = 1,
                                      group_constraints = NULL) {
  N <- length(symbols)
  if (N == 0L) stop("af_port_build_constraints: no symbols")

  # Equality: sum(w) = 1
  Aeq <- matrix(1, nrow = N, ncol = 1)
  beq <- 1

  # Inequalities
  A_ineq <- NULL
  b_ineq <- NULL

  # 1) long_only → w_i >= 0
  if (long_only) {
    A_ineq <- diag(N)
    b_ineq <- rep(0, N)
  }

  # 2) w_i <= w_max → -w_i >= -w_max
  if (!is.null(w_max) && is.finite(w_max) && w_max < 1) {
    A2 <- -diag(N)
    b2 <- rep(-w_max, N)
    if (is.null(A_ineq)) {
      A_ineq <- A2
      b_ineq <- b2
    } else {
      A_ineq <- rbind(A_ineq, A2)
      b_ineq <- c(b_ineq, b2)
    }
  }

  # 3) Group constraints
  # Each entry in group_constraints: list(name, symbols, min, max)
  if (!is.null(group_constraints) && length(group_constraints) > 0L) {
    for (gc in group_constraints) {
      if (is.null(gc$symbols) || length(gc$symbols) == 0L) next

      idx <- match(gc$symbols, symbols)
      idx <- idx[!is.na(idx)]
      if (length(idx) == 0L) next

      # min: sum w_i >= min
      if (!is.null(gc$min) && is.finite(gc$min)) {
        a <- rep(0, N)
        a[idx] <- 1
        if (is.null(A_ineq)) {
          A_ineq <- matrix(a, nrow = 1)
          b_ineq <- gc$min
        } else {
          A_ineq <- rbind(A_ineq, a)
          b_ineq <- c(b_ineq, gc$min)
        }
      }

      # max: sum w_i <= max → -sum w_i >= -max
      if (!is.null(gc$max) && is.finite(gc$max)) {
        a <- rep(0, N)
        a[idx] <- -1
        if (is.null(A_ineq)) {
          A_ineq <- matrix(a, nrow = 1)
          b_ineq <- -gc$max
        } else {
          A_ineq <- rbind(A_ineq, a)
          b_ineq <- c(b_ineq, -gc$max)
        }
      }
    }
  }

  # Build final Amat, bvec, meq
  if (!is.null(A_ineq)) {
    # Amat: columns are constraint vectors
    Amat <- cbind(Aeq, t(A_ineq))
    bvec <- c(beq, b_ineq)
    meq  <- 1L   # first column (Aeq) is equality
  } else {
    Amat <- Aeq
    bvec <- beq
    meq  <- 1L
  }

  list(Amat = Amat, bvec = bvec, meq = meq)
}

# ----------------------------------------------------------
# 4. Core QP solver wrapper
# ----------------------------------------------------------

af_port_quadprog_solve <- function(Sigma,
                                   mu           = NULL,
                                   mode         = c("min_var", "mean_var", "max_sharpe"),
                                   long_only    = TRUE,
                                   w_max        = 1,
                                   leverage_max = 1,
                                   risk_aversion = 5,
                                   rf_annual     = 0,
                                   group_constraints = NULL) {

  mode <- match.arg(mode)
  if (!requireNamespace("quadprog", quietly = TRUE)) {
    stop("Package 'quadprog' is required for quadratic optimization.")
  }

  if (is.null(dim(Sigma))) stop("Sigma must be a covariance matrix.")
  N <- nrow(Sigma)
  if (ncol(Sigma) != N) stop("Sigma must be square.")
  if (is.null(rownames(Sigma))) {
    rownames(Sigma) <- paste0("A", seq_len(N))
    colnames(Sigma) <- rownames(Sigma)
  }

  # Dmat = 2 * Σ (scaled later depending on mode)
  Dmat <- 2 * Sigma

  # dvec depends on mode:
  #   min_var:  dvec = 0
  #   mean_var: dvec = μ_tilde (we want to maximize w'μ - λ w'Σw)
  #   max_sharpe: treat like mean_var with μ_tilde = μ - rf_daily
  if (mode == "min_var") {
    dvec <- rep(0, N)
  } else {
    if (is.null(mu)) stop("mu must be provided for mode != 'min_var'")
    mu <- as.numeric(mu)
    if (length(mu) != N) stop("length(mu) must match nrow(Sigma)")
    names(mu) <- rownames(Sigma)

    # risk_aversion acts as λ on Σ
    if (mode == "mean_var") {
      Dmat <- 2 * risk_aversion * Sigma
      dvec <- mu
    } else if (mode == "max_sharpe") {
      # Approximate with μ_tilde = μ - rf
      # We assume rf_annual given; convert to daily for μ vector:
      rf_daily <- (1 + rf_annual)^(1 / 252) - 1
      mu_tilde <- mu - rf_daily
      Dmat <- 2 * risk_aversion * Sigma
      dvec <- mu_tilde
    } else {
      stop("Unknown mode in af_port_quadprog_solve.")
    }
  }

  # Build constraints
  constr <- af_port_build_constraints(
    symbols            = rownames(Sigma),
    long_only          = long_only,
    w_max              = w_max,
    group_constraints  = group_constraints
  )

  res <- quadprog::solve.QP(
    Dmat = Dmat,
    dvec = dvec,
    Amat = constr$Amat,
    bvec = constr$bvec,
    meq  = constr$meq
  )

  w <- res$solution
  names(w) <- rownames(Sigma)

  # Clean very small negatives (numeric noise) if long-only
  if (long_only) {
    w[w < 0] <- 0
  }

  # Normalize to sum 1
  s <- sum(w)
  if (s > 0) w <- w / s

  w
}

# ----------------------------------------------------------
# 5. Public API: af_build_portfolio (μ, Σ → weights)
# ----------------------------------------------------------

af_build_portfolio <- function(mu,
                               Sigma,
                               config = af_port_default_config) {
  cfg <- af_port_merge_config(config)

  # Basic checks
  if (is.null(dim(Sigma))) stop("af_build_portfolio: Sigma must be a matrix.")
  N <- nrow(Sigma)
  if (is.null(mu) || length(mu) != N) {
    stop("af_build_portfolio: mu must be numeric vector of length nrow(Sigma).")
  }
  if (is.null(rownames(Sigma))) {
    rownames(Sigma) <- paste0("A", seq_len(N))
    colnames(Sigma) <- rownames(Sigma)
  }

  mode <- match.arg(cfg$mode, c("equal", "inv_vol", "min_var", "mean_var", "max_sharpe"))

  if (mode == "equal") {
    w <- af_port_equal_weights(rownames(Sigma))
  } else if (mode == "inv_vol") {
    w <- af_port_inv_vol(Sigma)
  } else {
    w <- af_port_quadprog_solve(
      Sigma            = Sigma,
      mu               = mu,
      mode             = mode,
      long_only        = cfg$long_only,
      w_max            = cfg$w_max,
      leverage_max     = cfg$leverage_max,
      risk_aversion    = cfg$risk_aversion,
      rf_annual        = cfg$rf_annual,
      group_constraints = cfg$group_constraints
    )
  }

  # Ex-ante stats
  mu_vec <- as.numeric(mu)
  names(mu_vec) <- rownames(Sigma)
  exp_ret_daily <- sum(w * mu_vec)
  exp_ret_annual <- (1 + exp_ret_daily)^252 - 1

  port_var <- as.numeric(t(w) %*% Sigma %*% w)
  port_vol_daily <- sqrt(port_var)
  port_vol_annual <- port_vol_daily * sqrt(252)

  rf_ann <- cfg$rf_annual
  rf_d  <- (1 + rf_ann)^(1 / 252) - 1
  exp_sharpe <- if (port_vol_daily > 0) {
    (exp_ret_daily - rf_d) / port_vol_daily
  } else {
    NA_real_
  }

  list(
    weights          = w,
    mu               = mu_vec,
    Sigma            = Sigma,
    exp_ret_daily    = exp_ret_daily,
    exp_ret_annual   = exp_ret_annual,
    exp_vol_daily    = port_vol_daily,
    exp_vol_annual   = port_vol_annual,
    exp_sharpe_daily = exp_sharpe
  )
}
