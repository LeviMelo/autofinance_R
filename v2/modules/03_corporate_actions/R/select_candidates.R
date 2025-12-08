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
  # 3) Gap-flag detector (recent window)
  # -------------------------------
  dt_gap <- dt_recent[symbol %in% liq_ok_syms]
  data.table::setorder(dt_gap, symbol, refdate)

  dt_gap[, close_lag := data.table::shift(close, 1L), by = symbol]
  dt_gap[, ret_1d := data.table::fifelse(
    is.finite(close) & is.finite(close_lag) & close_lag > 0,
    close / close_lag - 1,
    NA_real_
  )]
  dt_gap[, close_lag := NULL]

  # Apply per-type thresholds
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

  gap_flags <- dt_gap[
    is.finite(ret_1d) & ret_1d <= thr,
    .(has_gap = TRUE),
    by = .(symbol)
  ]

  gap_syms <- gap_flags$symbol

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Gap-flag symbols= ", length(unique(gap_syms)))
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
  # 5) Union + cap
  # -------------------------------
  cand_all <- unique(c(c1, c2, gap_syms))

  # If still empty (edge-case data), fall back to liquidity-ok
  if (!length(cand_all)) cand_all <- unique(liq_ok_syms)

  # Apply hard cap using priority order
  if (length(cand_all) > max_cand && nrow(liq_ok_dt)) {
    # Build a priority table for deterministic trimming
    pri <- liq_ok_dt[, .(symbol, priority = liq_rank_overall)]
    pri <- pri[order(priority)]
    pri_syms <- pri$symbol

    # Keep candidate order: priority first, then anything else
    ordered <- unique(c(intersect(pri_syms, cand_all), cand_all))
    cand_all <- head(ordered, max_cand)
  }

  if (verbose) {
    af2_log("AF2_CA_PREF:", "Candidates final= ", length(cand_all))
    #af2_log("AF2_CA_PREF:", "Selective actions enabled= ", isTRUE(cfg$enable_selective_actions))
    #af2_log("AF2_CA_PREF:", "Yahoo candidate symbols= ", length(cand_all))
  }

  cand_all
}
