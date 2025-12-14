# v2/scripts/05c_hard_selective_split_audit.R
# Full-universe HARD diagnostic:
# - Uses the real selective CA architecture
# - Builds a full adjusted panel
# - Audits split consistency at scale
#
# Goal:
# Identify whether split misinterpretation / duplication / missing data
# is systemic beyond your 13-symbol suspect list.

# ----------------------------
# 00) Core
# ----------------------------
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

# data.table is assumed elsewhere, but load defensively
suppressWarnings(suppressMessages({
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("data.table required for this script.")
  }
}))
library(data.table)

# ----------------------------
# 00b) Audit knobs (edit here)
# ----------------------------
audit_cfg <- list(
  # Universe length
  lookback_days_universe = 730L,

  # RAW-gap thresholds for "split-like" candidates
  # We intentionally use multiple cutoffs to avoid a single presumptive band.
  raw_ratio_bands = list(
    band1 = c(0.20, 5.00),
    band2 = c(0.10, 10.0),
    band3 = c(0.05, 20.0)
  ),

  # What counts as "raw barely moved"
  # Used to flag suspicious split events that don't match COTAHIST reality.
  small_move_band = c(0.80, 1.25),

  # How many rows to print for each suspicious bucket
  top_n_print = 25L
)

# ----------------------------
# 00c) Real architecture config (selective ON)
# ----------------------------
cfg <- af2_get_config(list(
  enable_selective_actions = TRUE,
  ca_cache_mode = "by_symbol",

  # Keep prefilter limits realistic for full run
  ca_prefilter_top_n_overall   = 200L,
  ca_prefilter_top_n_by_type   = 50L,
  ca_prefilter_max_candidates  = 300L,

  # IMPORTANT:
  # For this audit we want to SEE Yahoo behavior,
  # not suppress it. So keep this OFF here.
  enable_split_plausibility_gate = FALSE
))

af2_log_cfg(cfg)

# ----------------------------
# 01) Universe (rb3)
# ----------------------------
source("v2/modules/01_b3_universe/R/zzz_depends.R")
source("v2/modules/01_b3_universe/R/rb3_init.R")
source("v2/modules/01_b3_universe/R/validate_types.R")
source("v2/modules/01_b3_universe/R/select_min_cols.R")
source("v2/modules/01_b3_universe/R/unify_liquidity.R")
source("v2/modules/01_b3_universe/R/filter_by_type_rb3.R")
source("v2/modules/01_b3_universe/R/fetch_daily.R")
source("v2/modules/01_b3_universe/R/build_universe.R")

types_all <- c("equity", "fii", "etf", "bdr")

end_date   <- Sys.Date() - 1
start_date <- end_date - as.integer(audit_cfg$lookback_days_universe)

af2_log("AF2_AUDIT05C:", "Universe window ", as.character(start_date),
        " to ", as.character(end_date))

dt_univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg,
  verbose = TRUE,
  use_cache = TRUE
)

af2_log("AF2_AUDIT05C:", "Universe rows=", nrow(dt_univ),
        " symbols=", length(unique(dt_univ$symbol)))

# ----------------------------
# 02) Corporate actions + Adjuster (selective wrapper)
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
source("v2/modules/04_adjuster/R/validate_panel_adj.R")

af2_log("AF2_AUDIT05C:", "Building adjusted panel with selective Yahoo registry...")

res_adj <- af2_build_panel_adj_selective(
  universe_raw = dt_univ,
  manual_events = NULL,
  cfg = cfg,
  from_ca = "2018-01-01",
  to_ca   = Sys.Date(),
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = 1L
)

panel_adj <- as.data.table(res_adj$panel_adj)
events    <- as.data.table(res_adj$events)

af2_validate_panel_adj(panel_adj)

af2_log("AF2_AUDIT05C:", "panel_adj rows=", nrow(panel_adj),
        " symbols=", length(unique(panel_adj$symbol)))
af2_log("AF2_AUDIT05C:", "events rows=", nrow(events),
        " symbols=", length(unique(events$symbol)))

# Defensive: ensure we have the expected columns
needed_panel <- c("symbol","refdate","asset_type","close_raw","close_adj_final")
missing_panel <- setdiff(needed_panel, names(panel_adj))
if (length(missing_panel)) {
  af2_log("AF2_AUDIT05C:", "ERROR: panel missing cols: ",
          paste(missing_panel, collapse = ", "))
  stop("panel_adj missing required columns for audit.")
}

needed_events <- c("symbol","refdate","split_value","source_mask","has_manual")
missing_events <- setdiff(needed_events, names(events))
if (length(missing_events)) {
  af2_log("AF2_AUDIT05C:", "ERROR: events missing cols: ",
          paste(missing_events, collapse = ", "))
  stop("events missing required columns for audit.")
}

# ----------------------------
# 03) Build RAW and ADJ shock series
# ----------------------------
setorder(panel_adj, symbol, refdate)

panel_adj[, raw_ratio := close_raw / shift(close_raw, 1L), by = symbol]
panel_adj[, adj_ratio := close_adj_final / shift(close_adj_final, 1L), by = symbol]

panel_adj[, raw_shock := abs(log(raw_ratio))]
panel_adj[, adj_shock := abs(log(adj_ratio))]

# Clean impossible rows
panel_adj <- panel_adj[
  is.finite(raw_ratio) & raw_ratio > 0 &
  is.finite(adj_ratio) & adj_ratio > 0
]

# ----------------------------
# 04) Event-driven split days
# ----------------------------
ev_splits <- events[is.finite(split_value) & split_value != 1]

# Join split info into panel on same date
panel_ev <- merge(
  panel_adj,
  ev_splits[, .(symbol, refdate, split_factor = split_value,
                source_mask_event = source_mask,
                has_manual_event = has_manual)],
  by = c("symbol","refdate"),
  all.x = TRUE
)

panel_ev[, is_split_day_event := !is.na(split_factor)]

af2_log("AF2_AUDIT05C:", "Event-driven split days detected: ",
        uniqueN(panel_ev[is_split_day_event == TRUE, .(symbol, refdate)]),
        " (symbol-date pairs)")

# ----------------------------
# 05) RAW-gap-driven split-like candidates
# ----------------------------
make_raw_candidates <- function(dt, band, label) {
  lo <- band[1]; hi <- band[2]
  out <- dt[
    raw_ratio < lo | raw_ratio > hi,
    .(symbol, refdate, asset_type, close_raw,
      raw_ratio, raw_shock, adj_ratio, adj_shock)
  ]
  out[, band_label := label]
  out
}

raw_cands <- rbindlist(lapply(names(audit_cfg$raw_ratio_bands), function(nm) {
  make_raw_candidates(panel_ev, audit_cfg$raw_ratio_bands[[nm]], nm)
}), use.names = TRUE, fill = TRUE)

af2_log("AF2_AUDIT05C:", "RAW-gap candidates (any band): ",
        nrow(raw_cands), " rows in total across bands.")

# ----------------------------
# 06) Coverage diagnostics (FIXED)
# ----------------------------

# Build a unique key of raw-gap symbol-date by band
raw_cands_key <- unique(raw_cands[, .(symbol, refdate, band_label)])

# Attach whether registry says that symbol-date is a split day
raw_cands_key <- merge(
  raw_cands_key,
  panel_ev[, .(symbol, refdate, is_split_day_event)],
  by = c("symbol","refdate"),
  all.x = TRUE
)

raw_cands_key[is.na(is_split_day_event), is_split_day_event := FALSE]

# Summarize coverage per band
cov_by_band <- raw_cands_key[
  , .(
    n_rows = .N,
    n_symbols = uniqueN(symbol),
    n_rows_with_event = sum(is_split_day_event),
    pct_rows_with_event = round(mean(is_split_day_event), 4)
  ),
  by = band_label
][order(band_label)]

af2_log("AF2_AUDIT05C:", "Coverage of RAW-gap candidates by registry splits:")
print(cov_by_band)

# ----------------------------
# 07) Suspicion buckets
# ----------------------------
small_lo <- audit_cfg$small_move_band[1]
small_hi <- audit_cfg$small_move_band[2]

# A) "Bad split event" suspects:
# Event says split, but raw barely moved
bad_event <- panel_ev[
  is_split_day_event == TRUE &
    raw_ratio >= small_lo & raw_ratio <= small_hi,
  .(symbol, refdate, asset_type,
    split_factor,
    raw_ratio, raw_shock,
    adj_ratio, adj_shock,
    close_raw, close_adj_final,
    source_mask_event, has_manual_event)
]

# B) "Missing split" suspects:
# Raw shows huge jump (use the middle band by default),
# but no event split recorded
band_mid <- "band1"
miss_event <- raw_cands_key[
  band_label == band_mid &
    is_split_day_event == FALSE
][
  panel_ev, on = .(symbol, refdate), nomatch = 0L
][
  , .(symbol, refdate, asset_type,
      raw_ratio, raw_shock,
      adj_ratio, adj_shock,
      close_raw, close_adj_final)
]

# C) "Harmful adjustment" suspects:
# On event split days, adjusted shock worse than raw shock
harm_event <- panel_ev[
  is_split_day_event == TRUE &
    is.finite(adj_shock) & is.finite(raw_shock) &
    adj_shock > raw_shock,
  .(symbol, refdate, asset_type,
    split_factor,
    raw_ratio, raw_shock,
    adj_ratio, adj_shock,
    close_raw, close_adj_final,
    source_mask_event, has_manual_event)
]

# ----------------------------
# 08) Summaries that are not just raw tables
# ----------------------------
af2_log("AF2_AUDIT05C:", "==================== SUMMARY ====================")

# 08a) Counts by asset type
af2_log("AF2_AUDIT05C:", "Split events by asset_type (event-driven):")
print(ev_splits[panel_adj[, .(symbol, asset_type)], on = "symbol",
               nomatch = 0L][
  , .N, by = asset_type
][order(-N)])

# 08b) How often adjustment improves on event split days
event_day_metrics <- panel_ev[
  is_split_day_event == TRUE,
  .(
    n_split_days = .N,
    mean_raw_shock = mean(raw_shock, na.rm = TRUE),
    mean_adj_shock = mean(adj_shock, na.rm = TRUE),
    pct_improved = mean(adj_shock < raw_shock, na.rm = TRUE)
  ),
  by = symbol
][order(-mean_raw_shock)]

af2_log("AF2_AUDIT05C:",
        "Event split-day shock summary (expect mean_adj_shock << mean_raw_shock).")
print(head(event_day_metrics, audit_cfg$top_n_print))

# 08c) Bucket summaries
af2_log("AF2_AUDIT05C:", "Potential BAD split events (event says split, raw small move).")
af2_log("AF2_AUDIT05C:",
        "Count rows=", nrow(bad_event),
        " symbols=", uniqueN(bad_event$symbol))
if (nrow(bad_event)) {
  print(head(bad_event[order(-adj_shock)], audit_cfg$top_n_print))
}

af2_log("AF2_AUDIT05C:", "Potential MISSING splits (raw big move, no event) using band1.")
af2_log("AF2_AUDIT05C:",
        "Count rows=", nrow(miss_event),
        " symbols=", uniqueN(miss_event$symbol))
if (nrow(miss_event)) {
  print(head(miss_event[order(-raw_shock)], audit_cfg$top_n_print))
}

af2_log("AF2_AUDIT05C:", "Potential HARMFUL split adjustments (adj_shock > raw_shock on event days).")
af2_log("AF2_AUDIT05C:",
        "Count rows=", nrow(harm_event),
        " symbols=", uniqueN(harm_event$symbol))
if (nrow(harm_event)) {
  print(head(harm_event[order(-adj_shock)], audit_cfg$top_n_print))
}

# 08d) Global rates
global_event_pairs <- uniqueN(panel_ev[is_split_day_event == TRUE, .(symbol, refdate)])
global_harm_pairs  <- uniqueN(harm_event[, .(symbol, refdate)])

af2_log("AF2_AUDIT05C:", "Global event split-day pairs=", global_event_pairs)
af2_log("AF2_AUDIT05C:", "Global harmful event pairs=", global_harm_pairs)
af2_log("AF2_AUDIT05C:",
        "Harmful rate on event split days=",
        if (global_event_pairs > 0) round(global_harm_pairs / global_event_pairs, 4) else NA_real_)

# 08e) Cross-check: how many raw-gap candidates exist vs registry splits
af2_log("AF2_AUDIT05C:", "RAW-gap band coverage recap:")
print(cov_by_band)

af2_log("AF2_AUDIT05C:", "================== END SUMMARY ==================")

# ----------------------------
# 09) Optional: feed adjusted panel into screener
# ----------------------------
source("v2/modules/05_screener/R/screener_config.R")
source("v2/modules/05_screener/R/validate_screener_input.R")
source("v2/modules/05_screener/R/liquidity_filter.R")
source("v2/modules/05_screener/R/compute_metrics.R")
source("v2/modules/05_screener/R/score_rank.R")
source("v2/modules/05_screener/R/run_screener.R")

af2_log("AF2_AUDIT05C:", "Running screener on full adjusted panel...")

res <- af2_run_screener(
  panel_ev, # includes raw/adj ratios + split flags, harmless
  config = list(
    lookback_days = 252L,
    horizons_days = c(21L, 63L, 126L, 252L)
  ),
  allow_unresolved = NULL
)

af2_log("AF2_AUDIT05C:", "Top 20 overall:")
print(utils::head(res$full, 20))

af2_log("AF2_AUDIT05C:", "OK - hard selective split audit finished.")
