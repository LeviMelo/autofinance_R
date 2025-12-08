# v2/modules/03_corporate_actions/R/select_candidates.R
# B3-only heuristics to decide which symbols deserve Yahoo corporate actions.

af2_ca_select_candidates <- function(universe_raw,
                                     cfg = NULL,
                                     verbose = TRUE) {

  af2_require("data.table")
  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)

  # Universe raw must be B3-grade data
  # Allow either turnover or vol_fin
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }

  af2_assert_cols(
    dt,
    c("symbol", "refdate", "close", "turnover", "asset_type"),
    name = "universe_raw(for ca prefilter)"
  )

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, asset_type := tolower(trimws(as.character(asset_type)))]
  dt[, refdate := as.Date(refdate)]

  data.table::setorder(dt, symbol, refdate)

  # -------------------------------
  # 0) Cheap liquidity gate
  # -------------------------------
  dt[, traded_flag := is.finite(close) & !is.na(close)]

  liq <- dt[, .(
    median_turnover = stats::median(turnover, na.rm = TRUE),
    days_traded_ratio = mean(traded_flag, na.rm = TRUE)
  ), by = .(symbol, asset_type)]

  liq[is.na(median_turnover), median_turnover := 0]
  liq[is.na(days_traded_ratio), days_traded_ratio := 0]

  liq_ok <- liq[
    median_turnover >= cfg$min_turnover &
      days_traded_ratio >= cfg$min_days_traded_ratio
  ]

  if (!nrow(liq_ok)) {
    if (verbose) af2_log("AF2_CA_PREF:", "No symbols pass liquidity prefilter.")
    return(character())
  }

  dt <- dt[symbol %in% liq_ok$symbol]

  # -------------------------------
  # 1) Restrict to recent window
  # -------------------------------
  recent_days <- as.integer(cfg$ca_prefilter_recent_days %||% 252L)
  if (!is.na(recent_days) && recent_days > 0) {
    dt <- dt[, tail(.SD, recent_days), by = symbol]
  }

  # -------------------------------
  # 2) One-day gap detector
  # -------------------------------
  dt[, close_lag := data.table::shift(close, 1L), by = symbol]
  dt[, ret_1d := data.table::fifelse(
    is.finite(close_lag) & close_lag > 0,
    (close / close_lag) - 1,
    NA_real_
  )]
  dt[, close_lag := NULL]

  gaps <- dt[, .(
    min_ret_1d = suppressWarnings(min(ret_1d, na.rm = TRUE))
  ), by = .(symbol, asset_type)]

  thr <- list(
    equity = cfg$ca_prefilter_gap_equity %||% -0.20,
    fii    = cfg$ca_prefilter_gap_fii    %||% -0.12,
    etf    = cfg$ca_prefilter_gap_etf    %||% -0.15,
    bdr    = cfg$ca_prefilter_gap_bdr    %||% -0.20
  )

  gaps[, thr := vapply(
    asset_type,
    function(tp) thr[[tp]] %||% -0.20,
    numeric(1)
  )]

  gaps_flag <- gaps[min_ret_1d <= thr, unique(symbol)]

  # -------------------------------
  # 3) Cheap momentum gate
  # -------------------------------
  get_ret_h <- function(x, h) {
    n <- length(x)
    if (n < (h + 1L)) return(NA_real_)
    p0 <- x[n - h]
    p1 <- x[n]
    if (!is.finite(p0) || p0 <= 0 || !is.finite(p1)) return(NA_real_)
    (p1 / p0) - 1
  }

  mom <- dt[, .(
    ret_21d = get_ret_h(close, 21L),
    ret_63d = get_ret_h(close, 63L)
  ), by = .(symbol, asset_type)]

  mom[, mom_score := data.table::fifelse(
    is.finite(ret_63d), ret_63d, ret_21d
  )]

  mom <- mom[order(-mom_score)]

  top_overall_n <- as.integer(cfg$ca_prefilter_top_n_overall %||% 200L)
  top_by_type_n <- as.integer(cfg$ca_prefilter_top_n_by_type %||% 50L)
  max_cand_n    <- as.integer(cfg$ca_prefilter_max_candidates %||% 300L)

  top_overall <- head(mom$symbol, top_overall_n)
  top_by_type <- mom[, head(symbol, top_by_type_n), by = asset_type]$V1

  cands <- unique(c(gaps_flag, top_overall, top_by_type))

  # If we exceed max cap, prioritize gap-flag + momentum order
  if (length(cands) > max_cand_n) {
    mom_order <- mom$symbol
    cands <- unique(c(gaps_flag, mom_order))
    cands <- head(cands, max_cand_n)
  }

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Liquidity ok symbols=", nrow(liq_ok))
    af2_log("AF2_CA_PREF:", "Gap-flag symbols=", length(gaps_flag))
    af2_log("AF2_CA_PREF:", "Candidates final=", length(cands))
  }

  sort(unique(cands))
}
