# v2/scripts/05d_yahoo_vs_af2_crosscheck.R
# Robust cross-check AF2 adjusted prices vs Yahoo:
#  - raw close from Yahoo
#  - Yahoo's own Adjusted column (Ad)
#  - adjustOHLC reconstruction (with symbol.name)

# ----------------------------
# 00) Core + config
# ----------------------------
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

cfg <- af2_get_config(list(
  enable_selective_actions = TRUE,
  ca_cache_mode = "by_symbol",
  enable_split_plausibility_gate = FALSE,
  # NEW:
  enable_split_gap_validation = TRUE,
  split_gap_tol_log = 0.35
))
af2_log_cfg(cfg)

# ----------------------------
# 01) Universe
# ----------------------------
source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

# ----------------------------
# 02) Corporate actions + Adjuster
# ----------------------------
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")
source("v2/modules/03_corporate_actions/R/select_candidates.R")

source("v2/modules/04_adjuster/R/zzz_depends.R")
source("v2/modules/04_adjuster/R/build_adjustments.R")
source("v2/modules/04_adjuster/R/apply_adjustments.R")
source("v2/modules/04_adjuster/R/build_panel_adj.R")
source("v2/modules/04_adjuster/R/build_panel_adj_selective.R")

# ----------------------------
# 03) Packages
# ----------------------------
suppressWarnings({
  library(data.table)
  library(quantmod)
})

# ----------------------------
# 04) Suspects
# ----------------------------
sus <- c("AMER3","TIMS3","ALZR11","HAPV3","OIBR3",
         "MGLU3","NFLX34","AVGO34","WALM34","AURA33")

types_all <- c("equity","fii","etf","bdr")

end_date   <- Sys.Date() - 1
start_date <- end_date - 730

af2_log("AF2_YX05D:", "Universe window ", start_date, " to ", end_date)

univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

univ_sus <- univ[symbol %in% sus]

res_adj <- tryCatch(
  af2_build_panel_adj_selective(
    universe_raw = univ_sus,
    manual_events = NULL,
    cfg = cfg,
    from_ca = "2018-01-01",
    to_ca   = Sys.Date(),
    verbose = TRUE,
    use_cache = TRUE,
    force_refresh = FALSE,
    n_workers = 1L,
    force_symbols = sus
  ),
  error = function(err) {
    af2_log("AF2_YX05D:", "ERROR building AF2 panel: ", conditionMessage(err))
    stop(conditionMessage(err), call. = FALSE)
  }
)

panel <- as.data.table(res_adj$panel_adj)

# ----------------------------
# Helpers
# ----------------------------
as_dt_series <- function(vec_xts, symbol_clean, colname) {
  if (is.null(vec_xts) || NROW(vec_xts) == 0) return(NULL)
  dt <- data.table(
    symbol  = symbol_clean,
    refdate = as.Date(index(vec_xts))
  )
  dt[[colname]] <- as.numeric(vec_xts)
  dt
}

safe_adjustOHLC <- function(x, ysym) {
  # Most precise method: rebuild using getSplits/getDividends
  # But MUST pass symbol.name when auto.assign=FALSE
  tryCatch(
    adjustOHLC(x, use.Adjusted = FALSE, symbol.name = ysym),
    error = function(e) NULL
  )
}

# ----------------------------
# 05) Per-symbol cross-check
# ----------------------------
out_list <- list()

for (s in sus) {
  ysym <- paste0(s, ".SA")

  af2_log("AF2_YX05D:", "--------------------------------------------")
  af2_log("AF2_YX05D:", "Yahoo fetch for ", ysym)

  x <- tryCatch(
    getSymbols(ysym, src = "yahoo", from = start_date, to = end_date,
               auto.assign = FALSE),
    error = function(e) NULL
  )

  if (is.null(x) || NROW(x) == 0) {
    af2_log("AF2_YX05D:", "WARNING: Yahoo OHLC missing for ", ysym)
    next
  }

  # Direct presence checks
  sp <- tryCatch(getSplits(ysym), error = function(e) NULL)
  dv <- tryCatch(getDividends(ysym), error = function(e) NULL)

  af2_log("AF2_YX05D:", "getSplits rows for ", s, ": ", if (is.null(sp)) 0 else NROW(sp))
  af2_log("AF2_YX05D:", "getDividends rows for ", s, ": ", if (is.null(dv)) 0 else NROW(dv))

  # 1) Yahoo raw close
  dt_y_raw <- as_dt_series(Cl(x), s, "close_y_raw")

  # 2) Yahoo Adjusted column close (if present)
  dt_y_adjcol <- NULL
  adj_vec <- tryCatch(Ad(x), error = function(e) NULL)
  dt_y_adjcol <- as_dt_series(adj_vec, s, "close_y_adjcol")

  # 3) adjustOHLC reconstructed adjusted close
  x_adj <- safe_adjustOHLC(x, ysym)
  dt_y_adjcalc <- as_dt_series(if (!is.null(x_adj)) Cl(x_adj) else NULL, s, "close_y_adjcalc")

  # Start merge with raw
  dt <- dt_y_raw

  if (!is.null(dt_y_adjcol)) {
    dt <- merge(dt, dt_y_adjcol, by = c("symbol","refdate"), all = TRUE)
  }
  if (!is.null(dt_y_adjcalc)) {
    dt <- merge(dt, dt_y_adjcalc, by = c("symbol","refdate"), all = TRUE)
  }

  # AF2 data
  # Pull TRUE raw straight from the universe window (Cotahist)
  dt_b3 <- univ_sus[symbol == s, .(
    symbol, refdate,
    close_b3_raw = close
  )]

  # AF2 final from the adjusted panel
  dt_af2 <- panel[symbol == s, .(
    symbol, refdate,
    close_af2_final = close_adj_final
  )]

  dt_af2 <- merge(dt_b3, dt_af2, by = c("symbol","refdate"), all = TRUE)
  dt <- merge(dt, dt_af2, by = c("symbol","refdate"), all = TRUE)


  # Log-diff diagnostics (guarded)
  dt[, diff_af2_vs_y_adjcol := ifelse(
    is.finite(close_af2_final) & is.finite(close_y_adjcol) & close_af2_final > 0 & close_y_adjcol > 0,
    log(close_af2_final) - log(close_y_adjcol),
    NA_real_
  )]

  dt[, diff_af2_vs_y_adjcalc := ifelse(
    is.finite(close_af2_final) & is.finite(close_y_adjcalc) & close_af2_final > 0 & close_y_adjcalc > 0,
    log(close_af2_final) - log(close_y_adjcalc),
    NA_real_
  )]

  dt[, diff_b3raw_vs_yraw := ifelse(
    is.finite(close_b3_raw) & is.finite(close_y_raw) & close_b3_raw > 0 & close_y_raw > 0,
    log(close_b3_raw) - log(close_y_raw),
    NA_real_
  )]

  # Summaries
  sm1 <- dt[!is.na(diff_af2_vs_y_adjcol), .(
    n = .N,
    mean_abs_log_diff = mean(abs(diff_af2_vs_y_adjcol), na.rm = TRUE),
    max_abs_log_diff  = max(abs(diff_af2_vs_y_adjcol), na.rm = TRUE)
  )]
  sm1[, method := "AF2 vs Yahoo Adjusted column"]

  sm2 <- dt[!is.na(diff_af2_vs_y_adjcalc), .(
    n = .N,
    mean_abs_log_diff = mean(abs(diff_af2_vs_y_adjcalc), na.rm = TRUE),
    max_abs_log_diff  = max(abs(diff_af2_vs_y_adjcalc), na.rm = TRUE)
  )]
  sm2[, method := "AF2 vs adjustOHLC(rebuilt)"]

  sm3 <- dt[!is.na(diff_b3raw_vs_yraw), .(
    n = .N,
    mean_abs_log_diff = mean(abs(diff_b3raw_vs_yraw), na.rm = TRUE),
    max_abs_log_diff  = max(abs(diff_b3raw_vs_yraw), na.rm = TRUE)
  )]
  sm3[, method := "B3 raw vs Yahoo raw"]

  sm <- rbindlist(list(sm1, sm2, sm3), use.names = TRUE, fill = TRUE)
  sm[, symbol := s]

  af2_log("AF2_YX05D:", "Summary diagnostics for ", s)
  print(sm)

  # At this point, 'dt' already contains:
  # - Yahoo raw
  # - Yahoo Adjusted column
  # - Yahoo adjustOHLC rebuild
  # and we then merged AF2/B3 into it.
  dt_final <- copy(dt)

  out_list[[s]] <- dt_final
}

af2_log("AF2_YX05D:", "Done. Inspect out_list for per-symbol tables.")
