# v2/modules/03_corporate_actions/R/select_candidates.R

af2_ca_select_candidates <- function(universe_raw, cfg = NULL, verbose = TRUE) {

  af2_require(c("data.table"))
  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)

  # Allow either turnover or vol_fin
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }

  af2_assert_cols(
    dt,
    c("symbol", "refdate", "close", "turnover", "asset_type"),
    name = "universe_raw(prefilter)"
  )

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, asset_type := tolower(trimws(as.character(asset_type)))]
  dt[, refdate := as.Date(refdate)]
  dt[, close := as.numeric(close)]
  dt[, turnover := as.numeric(turnover)]
  
  # -------------------------------
  # 1) Define Windows
  # -------------------------------
  end_date <- max(dt$refdate, na.rm = TRUE)
  
  # Liquidity Window (Keep short to capture current active set)
  liq_window_days <- as.integer(cfg$ca_prefilter_liq_window_days %||% 63L)
  if (!is.finite(liq_window_days) || liq_window_days < 20L) liq_window_days <- 63L
  
  liq_start <- end_date - ceiling(liq_window_days * 1.6)
  
  # -------------------------------
  # 2) Set A: Dividend Candidates (The "Broad" Set)
  # -------------------------------
  # Rule: If it is liquid enough to be in the screener, we MUST fetch its dividends.
  # We do not use gaps to detect dividends.
  
  dt_liq <- dt[refdate >= liq_start & refdate <= end_date]
  dt_liq[, traded_flag := is.finite(close) & !is.na(close)]
  
  stats_liq <- dt_liq[, .(
    median_turnover = stats::median(turnover, na.rm = TRUE),
    days_traded_ratio = mean(traded_flag, na.rm = TRUE)
  ), by = .(symbol, asset_type)]
  
  stats_liq[is.na(median_turnover), median_turnover := 0]
  stats_liq[is.na(days_traded_ratio), days_traded_ratio := 0]
  
  # Filter based on config thresholds
  set_dividends <- stats_liq[
    median_turnover >= (cfg$min_turnover %||% 5e5) &
    days_traded_ratio >= (cfg$min_days_traded_ratio %||% 0.8)
  ]$symbol
  
  if (verbose) {
    af2_log("AF2_CA_PREF:", "Set A (Broad Dividend/Liquidity) count= ", length(set_dividends))
  }

  # -------------------------------
  # 3) Set B: Split Candidates (The "Gap" Set)
  # -------------------------------
  # Rule: Detect discontinuities over the FULL available history (not just recent).
  # This fixes the AURA33 issue where the split was outside the 252-day window.
  
  # We use the full 'dt' here, not a subset.
  data.table::setorder(dt, symbol, refdate)
  dt[, close_lag := data.table::shift(close, 1L), by = symbol]
  
  # Symmetric log return
  dt[, log_ret := data.table::fifelse(
    is.finite(close) & is.finite(close_lag) & close_lag > 0 & close > 0,
    log(close / close_lag),
    NA_real_
  )]
  
  # Thresholds
  gap_thr_log <- as.numeric(cfg$ca_prefilter_jump_log_thr %||% 1.0) # ~2.7x jump
  
  # Type-specific negative drops (Reverse Logic: these catch normal splits)
  thr_map <- data.table::data.table(
    asset_type = c("equity", "fii", "etf", "bdr"),
    thr = c(
      cfg$ca_prefilter_gap_equity %||% -0.20,
      cfg$ca_prefilter_gap_fii    %||% -0.12,
      cfg$ca_prefilter_gap_etf    %||% -0.15,
      cfg$ca_prefilter_gap_bdr    %||% -0.20
    )
  )
  
  # Join thresholds to full data
  # Note: This is memory intensive on full history. 
  # Optimization: Only check rows where abs(log_ret) > 0.10 first to save merge cost?
  # For now, simplistic merge is fine for < 1M rows.
  dt <- merge(dt, thr_map, by = "asset_type", all.x = TRUE)
  
  # Flag B1: Huge Jumps (Reverse Splits)
  jump_pos_syms <- dt[is.finite(log_ret) & log_ret >= gap_thr_log, unique(symbol)]
  
  # Flag B2: Deep Drops (Forward Splits) - using simple ret approx for config compatibility
  dt[, ret_1d := (close/close_lag) - 1]
  gap_neg_syms <- dt[is.finite(ret_1d) & is.finite(thr) & ret_1d <= thr, unique(symbol)]
  
  set_splits <- unique(c(jump_pos_syms, gap_neg_syms))

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Set B (Gap/Split Suspects) count= ", length(set_splits))
  }

  # -------------------------------
  # 4) Union and Return
  # -------------------------------
  cand_final <- unique(c(set_dividends, set_splits))
  
  # Clean up NAs
  cand_final <- cand_final[!is.na(cand_final) & nzchar(cand_final)]
  
  if (verbose) {
    af2_log("AF2_CA_PREF:", "Candidates Final (Union) = ", length(cand_final))
  }
  
  cand_final
}