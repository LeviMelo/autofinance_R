############################################################
# autofinance_risk_models.R
#
# Risk model layer: Σ (covariance) and μ (expected returns)
# Methods:
#   Covariance:
#     - "sample"      : sample covariance matrix
#     - "shrinkage"   : simple shrink-to-diagonal
#     - "garch_dcc"   : GARCH(1,1) + DCC covariance at t_end
#
#   Expected returns:
#     - "hist_mean"   : historical average daily return
#     - "momentum"    : last-H-days average return
#     - "var"         : VAR-based 1-step-ahead forecast
#
# All returns here are DAILY. No annualization inside this module.
############################################################

# --- Config / packages -----------------------------------------------------

if (!exists("af_attach_packages")) {
  if (file.exists("autofinance_config.R")) {
    source("autofinance_config.R")
  } else {
    stop("autofinance_config.R not found; please source it before using risk models.")
  }
}

af_attach_packages(c("data.table"))

# --- Utility: prepare wide return matrix -----------------------------------

# panel: data.table with at least columns: symbol, refdate, ret_simple,
#        optionally excess_ret_simple.
#
# symbols: optional character vector of tickers to include.
# end_date: optional Date or "YYYY-MM-DD"; if NULL use max(panel$refdate).
# window_years: how many years back from end_date to include.
# use_excess: if TRUE and column 'excess_ret_simple' exists, use it; otherwise 'ret_simple'.
# min_obs_ratio: minimum fraction of non-NA observations required per asset.
#
# Returns:
#   list(
#     returns    : matrix (T x N) of daily returns,
#     dates      : Date vector (length T),
#     symbols    : character vector of N symbols (columns of matrix),
#     start_date : Date (min date used),
#     end_date   : Date (max date used)
#   )

af_risk_prepare_returns <- function(panel,
                                   symbols       = NULL,
                                   end_date      = NULL,
                                   window_years  = 3,
                                   use_excess    = TRUE,
                                   min_obs_ratio = 0.7) {

  panel <- data.table::as.data.table(panel)

  if (!all(c("symbol", "refdate") %in% names(panel))) {
    stop("panel must contain at least 'symbol' and 'refdate' columns.")
  }

  # Choose returns column
  col_ret <- NULL
  if (use_excess && "excess_ret_simple" %in% names(panel)) {
    col_ret <- "excess_ret_simple"
  } else if ("ret_simple" %in% names(panel)) {
    col_ret <- "ret_simple"
  } else {
    stop("panel must contain 'ret_simple' or 'excess_ret_simple'.")
  }

  # Filter symbols
  if (!is.null(symbols)) {
    panel <- panel[symbol %in% symbols]
  }

  if (nrow(panel) == 0) {
    stop("No rows in panel after symbol filter.")
  }

  # Normalize refdate to Date
  panel[, refdate := as.Date(refdate)]

  # Determine end_date
  if (is.null(end_date)) {
    end_date <- max(panel$refdate, na.rm = TRUE)
  } else {
    end_date <- as.Date(end_date)
  }

  # Determine start_date from window_years
  # Simple approximation: 365 * years. If you want trading days adjust later.
  start_date <- end_date - as.integer(365 * window_years)

  panel <- panel[refdate >= start_date & refdate <= end_date]

  if (nrow(panel) == 0) {
    stop("No data in panel for the requested window.")
  }

  # Wide matrix: rows = dates, cols = symbols, values = returns
  wide <- data.table::dcast(
    panel,
    refdate ~ symbol,
    value.var      = col_ret,
    fun.aggregate  = mean,
    fill           = NA_real_
  )

  dates <- as.Date(wide$refdate)
  R_mat <- as.matrix(wide[, -1, with = FALSE])
  colnames(R_mat) <- names(wide)[-1]

  # Drop assets with too many NAs
  n_obs <- length(dates)
  non_na_counts <- colSums(!is.na(R_mat))
  keep <- non_na_counts >= min_obs_ratio * n_obs

  if (!any(keep)) {
    stop("All assets dropped by min_obs_ratio filter; relax constraints or extend window.")
  }

  drops <- setdiff(colnames(R_mat), colnames(R_mat)[keep])
  if (length(drops) > 0) {
    message("Dropping ", length(drops), " assets due to insufficient data: ",
            paste(drops, collapse = ", "))
  }

  R_mat <- R_mat[, keep, drop = FALSE]

  list(
    returns    = R_mat,
    dates      = dates,
    symbols    = colnames(R_mat),
    start_date = min(dates),
    end_date   = max(dates)
  )
}

# --- Covariance estimation --------------------------------------------------

# returns_mat: numeric matrix (T x N) of daily returns (already filtered).
# method:
#   - "sample"
#   - "shrinkage"
#   - "garch_dcc"
#
# shrink_lambda: weight for shrinkage to diagonal (0=no shrink, 1=only diagonal).
# shrink_target: "diagonal" or "identity"
#
# For garch_dcc:
#   - if garch_spec/dcc_spec are NULL, default specs are created inside.

af_cov_estimate <- function(returns_mat,
                            method         = c("sample", "shrinkage", "garch_dcc"),
                            shrink_lambda  = 0.1,
                            shrink_target  = c("diagonal", "identity"),
                            garch_spec     = NULL,
                            dcc_spec       = NULL) {

  method        <- match.arg(method)
  shrink_target <- match.arg(shrink_target)

  if (!is.matrix(returns_mat)) {
    stop("returns_mat must be a numeric matrix.")
  }

  if (ncol(returns_mat) < 2L) {
    stop("Need at least 2 assets to estimate covariance.")
  }

  # Remove rows that are all NA
  all_na_rows <- apply(returns_mat, 1L, function(x) all(is.na(x)))
  returns_mat <- returns_mat[!all_na_rows, , drop = FALSE]

  if (nrow(returns_mat) < 10L) {
    stop("Too few observations after removing NA rows for covariance estimation.")
  }

  if (method == "sample") {
    Sigma <- stats::cov(returns_mat, use = "pairwise.complete.obs")
    return(list(
      Sigma  = Sigma,
      method = "sample",
      meta   = list(
        n_obs    = nrow(returns_mat),
        n_assets = ncol(returns_mat)
      )
    ))
  }

  if (method == "shrinkage") {
    S <- stats::cov(returns_mat, use = "pairwise.complete.obs")
    p <- ncol(S)

    if (shrink_target == "diagonal") {
      F <- diag(diag(S), nrow = p, ncol = p)
    } else {
      avg_var <- mean(diag(S), na.rm = TRUE)
      F <- diag(avg_var, nrow = p, ncol = p)
    }

    lambda <- max(min(shrink_lambda, 1), 0)
    Sigma  <- (1 - lambda) * S + lambda * F

    return(list(
      Sigma  = Sigma,
      method = "shrinkage",
      meta   = list(
        n_obs         = nrow(returns_mat),
        n_assets      = p,
        shrink_lambda = lambda,
        shrink_target = shrink_target
      )
    ))
  }

  if (method == "garch_dcc") {
    af_attach_packages(c("rugarch", "rmgarch"))

    # DCC cannot handle NAs; drop rows with any NA
    complete_rows <- stats::complete.cases(returns_mat)
    R_cc <- returns_mat[complete_rows, , drop = FALSE]

    if (nrow(R_cc) < 30L) {
      stop("Too few complete observations for GARCH-DCC estimation.")
    }

    N <- ncol(R_cc)

    # Default univariate GARCH(1,1) spec if not provided
    if (is.null(garch_spec)) {
      garch_spec <- rugarch::ugarchspec(
        variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
        mean.model     = list(armaOrder = c(0, 0), include.mean = TRUE),
        distribution.model = "norm"
      )
    }

    # Multivariate spec
    uspec  <- rmgarch::multispec(replicate(N, garch_spec))
    if (is.null(dcc_spec)) {
      dcc_spec <- rmgarch::dccspec(
        uspec     = uspec,
        dccOrder  = c(1, 1),
        distribution = "mvnorm"
      )
    }

    dcc_fit <- rmgarch::dccfit(
      spec = dcc_spec,
      data = R_cc
    )

    rcov_arr <- rmgarch::rcov(dcc_fit)  # array (N x N x T)
    t_last   <- dim(rcov_arr)[3L]
    Sigma    <- rcov_arr[, , t_last, drop = TRUE]

    dimnames(Sigma) <- list(colnames(R_cc), colnames(R_cc))

    return(list(
      Sigma  = Sigma,
      method = "garch_dcc",
      meta   = list(
        n_obs          = nrow(R_cc),
        n_assets       = N,
        convergence    = dcc_fit@mfit$convergence,
        last_index_row = t_last
      )
    ))
  }

  stop("Unknown covariance method: ", method)
}

# --- Expected returns estimation -------------------------------------------

# returns_mat: numeric matrix (T x N) of daily returns.
# method:
#   - "hist_mean" : μ_j = mean(r_j)
#   - "momentum"  : μ_j = mean of last 'momentum_window' observations
#   - "var"       : μ_j = 1-step-ahead forecast from VAR(p)
#
# macro_xts: placeholder for future factor-based VAR; currently optional/unused.
#
# Returns:
#   list(mu = numeric vector length N, method=..., meta=list(...))

af_mu_estimate <- function(returns_mat,
                           method          = c("hist_mean", "momentum", "var"),
                           momentum_window = 63L,
                           var_lag         = 1L,
                           macro_xts       = NULL) {

  method <- match.arg(method)

  if (!is.matrix(returns_mat)) {
    stop("returns_mat must be a numeric matrix.")
  }

  N <- ncol(returns_mat)
  if (N < 1L) {
    stop("No assets in returns_mat.")
  }

  # Remove rows that are all NA
  all_na_rows <- apply(returns_mat, 1L, function(x) all(is.na(x)))
  R <- returns_mat[!all_na_rows, , drop = FALSE]

  if (nrow(R) < 10L) {
    stop("Too few observations after removing all-NA rows for μ estimation.")
  }

  # Helper: per-column last non-NA index
  last_non_na_idx <- function(x) {
    idx <- which(!is.na(x))
    if (length(idx) == 0L) return(NA_integer_)
    tail(idx, 1L)
  }

  if (method == "hist_mean") {
    mu <- colMeans(R, na.rm = TRUE)
    return(list(
      mu     = as.numeric(mu),
      method = "hist_mean",
      meta   = list(
        n_obs    = nrow(R),
        n_assets = N
      )
    ))
  }

  if (method == "momentum") {
    w <- max(1L, as.integer(momentum_window))
    Tn <- nrow(R)

    mu <- numeric(N)
    names(mu) <- colnames(R)

    for (j in seq_len(N)) {
      x <- R[, j]
      idx_last <- last_non_na_idx(x)
      if (is.na(idx_last)) {
        mu[j] <- NA_real_
      } else {
        idx_start <- max(1L, idx_last - w + 1L)
        window_vals <- x[idx_start:idx_last]
        mu[j] <- mean(window_vals, na.rm = TRUE)
      }
    }

    return(list(
      mu     = mu,
      method = "momentum",
      meta   = list(
        n_obs            = nrow(R),
        n_assets         = N,
        momentum_window  = w
      )
    ))
  }

  if (method == "var") {
    af_attach_packages("vars")

    # VAR cannot handle NAs; drop rows with any NA
    R_cc <- R[stats::complete.cases(R), , drop = FALSE]

    if (nrow(R_cc) < (var_lag + 5L)) {
      stop("Too few complete observations for VAR with lag = ", var_lag)
    }

    # Fit VAR(p)
    var_fit <- vars::VAR(
      R_cc,
      p    = var_lag,
      type = "const"
    )

    # 1-step-ahead forecast
    fcst <- stats::predict(var_fit, n.ahead = 1L)

    # fcst$fcst is a list, one element per series
    mu <- numeric(N)
    names(mu) <- colnames(R_cc)

    for (j in seq_len(N)) {
      series_name <- colnames(R_cc)[j]
      comp <- fcst$fcst[[series_name]]
      mu[j] <- comp[1L, "fcst"]
    }

    return(list(
      mu     = mu,
      method = "var",
      meta   = list(
        n_obs      = nrow(R_cc),
        n_assets   = N,
        var_lag    = var_lag,
        var_call   = var_fit$call
      )
    ))
  }

  stop("Unknown μ method: ", method)
}

# --- High-level wrapper: af_risk_build -------------------------------------

# This is the main entry point the portfolio/backtest engine will call.
#
# panel: data.table with symbol, refdate, ret_simple / excess_ret_simple.
# symbols: optional subset.
# end_date, window_years, use_excess, min_obs_ratio: same semantics as prepare_returns.
#
# cov_method: "sample", "shrinkage", "garch_dcc"
# mu_method : "hist_mean", "momentum", "var"
#
# Extra args are forwarded to af_cov_estimate / af_mu_estimate via ...,
# so you can pass shrink_lambda, momentum_window, var_lag, etc.
#
# Returns:
#   list(
#     mu        : numeric vector (N),
#     Sigma     : matrix (N x N),
#     symbols   : character (N),
#     start_date: Date,
#     end_date  : Date,
#     cov_meta  : list(...),
#     mu_meta   : list(...)
#   )

af_risk_build <- function(panel,
                          symbols       = NULL,
                          end_date      = NULL,
                          window_years  = 3,
                          use_excess    = TRUE,
                          min_obs_ratio = 0.7,
                          cov_method    = c("sample", "shrinkage", "garch_dcc"),
                          mu_method     = c("hist_mean", "momentum", "var"),
                          ...) {

  cov_method <- match.arg(cov_method)
  mu_method  <- match.arg(mu_method)

  prep <- af_risk_prepare_returns(
    panel          = panel,
    symbols        = symbols,
    end_date       = end_date,
    window_years   = window_years,
    use_excess     = use_excess,
    min_obs_ratio  = min_obs_ratio
  )

  R_mat   <- prep$returns
  sym_ret <- prep$symbols

  # Covariance
  cov_res <- af_cov_estimate(
    returns_mat    = R_mat,
    method         = cov_method,
    ...
  )

  # Expected returns
  mu_res <- af_mu_estimate(
    returns_mat    = R_mat,
    method         = mu_method,
    ...
  )

  # Align dimensions
  if (length(mu_res$mu) != ncol(cov_res$Sigma)) {
    stop("Dimension mismatch between μ and Σ; check returns matrix handling.")
  }

  names(mu_res$mu) <- sym_ret
  dimnames(cov_res$Sigma) <- list(sym_ret, sym_ret)

  list(
    mu         = mu_res$mu,
    Sigma      = cov_res$Sigma,
    symbols    = sym_ret,
    start_date = prep$start_date,
    end_date   = prep$end_date,
    cov_meta   = cov_res$meta,
    mu_meta    = mu_res$meta,
    cov_method = cov_method,
    mu_method  = mu_method
  )
}
