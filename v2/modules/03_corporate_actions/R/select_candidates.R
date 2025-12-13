# v2/modules/03_corporate_actions/R/select_candidates.R

af2_ca_select_candidates <- function(universe_raw, cfg = NULL, verbose = TRUE) {

  af2_require(c("data.table"))
  cfg <- cfg %||% af2_get_config()

  dt <- data.table::as.data.table(universe_raw)

  # Allow either turnover or vol_fin (rb3 differences)
  if (!"turnover" %in% names(dt) && "vol_fin" %in% names(dt)) {
    dt[, turnover := vol_fin]
  }

  # Minimal columns needed for the prefilter
  af2_assert_cols(
    dt,
    c("symbol", "refdate", "close", "turnover", "asset_type"),
    name = "universe_raw(prefilter)"
  )

  # Normalize
  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, asset_type := tolower(trimws(as.character(asset_type)))]
  dt[, refdate := as.Date(refdate)]
  dt[, close := as.numeric(close)]
  dt[, turnover := as.numeric(turnover)]

  # -------------------------------
  # 0) Define recent window
  # -------------------------------
  end_date <- max(dt$refdate, na.rm = TRUE)

  recent_days <- as.integer(cfg$ca_prefilter_recent_days %||% 252L)
  if (!is.finite(recent_days) || recent_days < 20L) recent_days <- 252L

  # Optional tighter liquidity window (new knob; fallback to recent_days)
  liq_window_days <- as.integer(cfg$ca_prefilter_liq_window_days %||% 63L)
  if (!is.finite(liq_window_days) || liq_window_days < 20L) liq_window_days <- min(63L, recent_days)

  # Active trading filter (new knob)
  active_days <- as.integer(cfg$ca_prefilter_active_days %||% 10L)
  if (!is.finite(active_days) || active_days < 1L) active_days <- 10L

  # We approximate "last N bizdays" by date window,
  # since universe_raw already comes from B3 business dates.
  recent_start <- end_date - ceiling(recent_days * 1.6)
  liq_start    <- end_date - ceiling(liq_window_days * 1.6)
  active_start <- end_date - ceiling(active_days * 1.6)

  dt_recent <- dt[refdate >= recent_start & refdate <= end_date]
  data.table::setorder(dt_recent, symbol, refdate)
  dt_recent <- dt_recent[, tail(.SD, recent_days), by = symbol]

  dt_liqwin <- dt[refdate >= liq_start & refdate <= end_date]

  # -------------------------------
  # 1) Active symbols (must trade recently)
  # -------------------------------
  # A "trade" is any day with a finite close
  active_dt <- dt_recent[is.finite(close) & !is.na(close),
                         .(last_trade = max(refdate, na.rm = TRUE)),
                         by = .(symbol, asset_type)]

  active_dt[, is_active := last_trade >= active_start]
  active_syms <- active_dt[is_active == TRUE, unique(symbol)]

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Active symbols= ", length(active_syms))
  }

  # If active filter is too strict in some environments, fail soft
  if (!length(active_syms)) {
    active_syms <- unique(dt_recent$symbol)
  }

  # -------------------------------
  # 2) Liquidity filter (short window)
  # -------------------------------
  dt_liqwin <- dt_liqwin[symbol %in% active_syms]

  dt_liqwin[, traded_flag := is.finite(close) & !is.na(close)]

  liq <- dt_liqwin[, .(
    median_turnover = stats::median(turnover, na.rm = TRUE),
    days_traded_ratio = mean(traded_flag, na.rm = TRUE)
  ), by = .(symbol, asset_type)]

  liq[is.na(median_turnover), median_turnover := 0]
  liq[is.na(days_traded_ratio), days_traded_ratio := 0]

  liq_ok_dt <- liq[
    median_turnover >= (cfg$min_turnover %||% 5e5) &
      days_traded_ratio >= (cfg$min_days_traded_ratio %||% 0.8)
  ]

  liq_ok_syms <- liq_ok_dt$symbol

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Liquidity ok symbols= ", length(unique(liq_ok_syms)))
  }

  # -------------------------------
  # 3) Discontinuity detector (captures SPLITS + REVERSE SPLITS)
  # -------------------------------
  # IMPORTANT: do NOT restrict this to liq_ok_syms only.
  # Otherwise you can miss suspicious discontinuities for symbols that still
  # pass the screener liquidity later (window mismatch) or are just below liq_ok.
  dt_gap <- dt_recent[symbol %in% active_syms]
  data.table::setorder(dt_gap, symbol, refdate)

  dt_gap[, close_lag := data.table::shift(close, 1L), by = symbol]

  # Simple close/close return (kept for "normal split" / drop detection)
  dt_gap[, ret_1d := data.table::fifelse(
    is.finite(close) & is.finite(close_lag) & close_lag > 0,
    close / close_lag - 1,
    NA_real_
  )]

  # Symmetric log return for reverse splits / huge jumps
  dt_gap[, log_ret := data.table::fifelse(
    is.finite(close) & is.finite(close_lag) & close_lag > 0 & close > 0,
    log(close / close_lag),
    NA_real_
  )]

  dt_gap[, close_lag := NULL]

  # ---- (A) Normal split-like drops: keep your per-type negative thresholds ----
  thr_map <- data.table::data.table(
    asset_type = c("equity", "fii", "etf", "bdr"),
    thr = c(
      cfg$ca_prefilter_gap_equity %||% -0.20,
      cfg$ca_prefilter_gap_fii    %||% -0.12,
      cfg$ca_prefilter_gap_etf    %||% -0.15,
      cfg$ca_prefilter_gap_bdr    %||% -0.20
    )
  )

  dt_gap <- merge(dt_gap, thr_map, by = "asset_type", all.x = TRUE)

  gap_neg <- dt_gap[
    is.finite(ret_1d) & is.finite(thr) & ret_1d <= thr,
    .(has_gap_neg = TRUE),
    by = .(symbol)
  ]
  gap_neg_syms <- gap_neg$symbol

  # ---- (B) Reverse split-like jumps: symmetric threshold in log space ----
  # Default 1.0 -> close/lag >= e^1 ≈ 2.718x jump (or bigger).
  # This is intentionally high so we don't flag normal volatility as "needs CA fetch".
  gap_thr_log <- as.numeric(cfg$ca_prefilter_jump_log_thr %||% 1.0)
  if (!is.finite(gap_thr_log) || gap_thr_log < 0.5) gap_thr_log <- 1.0

  jump_pos <- dt_gap[
    is.finite(log_ret) & log_ret >= gap_thr_log,
    .(has_jump_pos = TRUE),
    by = .(symbol)
  ]
  jump_pos_syms <- jump_pos$symbol

  gap_syms <- unique(c(gap_neg_syms, jump_pos_syms))

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Gap-flag symbols (neg drops)= ", length(unique(gap_neg_syms)))
    af2_log("AF2_CA_PREF:", "Gap-flag symbols (pos jumps)= ", length(unique(jump_pos_syms)))
    af2_log("AF2_CA_PREF:", "Gap-flag symbols (union)= ", length(unique(gap_syms)))
  }

  # -------------------------------
  # 4) Priority ranking source
  # -------------------------------
  # Priority = liquidity rank within liq_ok set
  # (Cheap, robust, and type-aware)
  liq_ok_dt[, liq_rank_overall := data.table::frank(-median_turnover, ties.method = "first")]
  liq_ok_dt[, liq_rank_type := data.table::frank(-median_turnover, ties.method = "first"), by = asset_type]

  top_overall <- as.integer(cfg$ca_prefilter_top_n_overall %||% 200L)
  top_by_type <- as.integer(cfg$ca_prefilter_top_n_by_type %||% 50L)
  max_cand    <- as.integer(cfg$ca_prefilter_max_candidates %||% 300L)

  if (!is.finite(top_overall) || top_overall < 1L) top_overall <- 200L
  if (!is.finite(top_by_type) || top_by_type < 1L) top_by_type <- 50L
  if (!is.finite(max_cand) || max_cand < 50L) max_cand <- 300L

  c1 <- liq_ok_dt[liq_rank_overall <= top_overall, unique(symbol)]
  c2 <- liq_ok_dt[liq_rank_type <= top_by_type, unique(symbol)]

  # -------------------------------
  # 5) Union + cap (Priority: GAPS/JUMPS must survive)
  # -------------------------------
  # Priority order:
  #   1) gap_syms (suspicious discontinuities, MUST be checked)
  #   2) top overall liquidity (c1)
  #   3) top by type liquidity (c2)
  cand_all <- unique(c(gap_syms, c1, c2))

  # If still empty (edge-case data), fall back to liquidity-ok
  if (!length(cand_all)) cand_all <- unique(liq_ok_syms)

  if (length(cand_all) > max_cand) {

    must_have <- unique(gap_syms)

    # If too many "must_have", trim must_have by liquidity (best effort)
    if (length(must_have) > max_cand) {
      if (nrow(liq)) {
        pri_gap <- liq[symbol %chin% must_have, .(symbol, median_turnover)]
        data.table::setorder(pri_gap, -median_turnover)
        must_have <- head(pri_gap$symbol, max_cand)
      } else {
        must_have <- head(must_have, max_cand)
      }
    }

    slots_left <- max_cand - length(must_have)

    if (slots_left > 0 && nrow(liq_ok_dt)) {
      pri <- liq_ok_dt[, .(symbol, priority = liq_rank_overall)]
      pri <- pri[order(priority)]
      fillers <- pri[!symbol %chin% must_have, head(symbol, slots_left)]
      cand_all <- unique(c(must_have, fillers))
    } else {
      cand_all <- unique(must_have)
    }

    # Final safety (should already be <= max_cand)
    if (length(cand_all) > max_cand) cand_all <- head(cand_all, max_cand)
  }

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Candidates final= ", length(cand_all))
  }

  cand_all
}
