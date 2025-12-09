# v2/scripts/04b_diagnose_suspects_selective.R
# Focused diagnostic run for known bad actors.
# Uses selective wrapper + force_symbols capability.

# ----------------------------
# 00) Core
# ----------------------------
source("v2/modules/00_core/R/utils.R")
source("v2/modules/00_core/R/config.R")
source("v2/modules/00_core/R/logging.R")

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
# 02) Corporate actions + selective prefilter
# ----------------------------
source("v2/modules/03_corporate_actions/R/zzz_depends.R")
source("v2/modules/03_corporate_actions/R/yahoo_symbol_map.R")
source("v2/modules/03_corporate_actions/R/fetch_splits_quantmod.R")
source("v2/modules/03_corporate_actions/R/fetch_dividends_quantmod.R")
source("v2/modules/03_corporate_actions/R/build_registry.R")
source("v2/modules/03_corporate_actions/R/select_candidates.R")

# ----------------------------
# 03) Adjuster
# ----------------------------
source("v2/modules/04_adjuster/R/zzz_depends.R")
source("v2/modules/04_adjuster/R/build_adjustments.R")
source("v2/modules/04_adjuster/R/apply_adjustments.R")
source("v2/modules/04_adjuster/R/build_panel_adj.R")
source("v2/modules/04_adjuster/R/build_panel_adj_selective.R")
source("v2/modules/04_adjuster/R/validate_panel_adj.R")

# ----------------------------
# Step 0 — Config overrides for a clean diagnostic
# ----------------------------
cfg_dbg <- af2_get_config(list(
  # First pass: disable selective so you see raw Yahoo behavior
  # You can flip this to TRUE in a second run.
  enable_selective_actions = FALSE,

  # Good for incremental cache behavior
  ca_cache_mode = "by_symbol",

  # First pass: do NOT gate splits
  enable_split_plausibility_gate = FALSE,

  split_gate_min = 0.05,
  split_gate_max = 20
))

af2_log_cfg(cfg_dbg)

# ----------------------------
# Step 1 — Your known bad actors
# ----------------------------
sus <- c("AMER3","MGLU3","OIBR3","TIMS3","HAPV3","AURA33",
         "ALZR11","HFOF11","RBVA11","TEPP11",
         "NFLX34","WALM34","AVGO34")

# ----------------------------
# 04) Build a small recent universe window
# ----------------------------
types_all <- c("equity","fii","etf","bdr")

end_date   <- Sys.Date() - 1
start_date <- end_date - 365

af2_log("AF2_DIAG04B:", "Universe window ", start_date, " to ", end_date)

univ <- af2_b3_build_universe_window(
  start_date = start_date,
  end_date   = end_date,
  include_types = types_all,
  cfg = cfg_dbg,
  verbose = TRUE,
  use_cache = TRUE
)

# Restrict universe to suspects to reduce workload noise
univ_sus <- univ[symbol %in% sus]

af2_log("AF2_DIAG04B:", "Suspect rows=", nrow(univ_sus),
        " symbols=", length(unique(univ_sus$symbol)))

# ----------------------------
# 05) Build adjusted panel via selective wrapper
# ----------------------------
res <- af2_build_panel_adj_selective(
  universe_raw = univ_sus,
  manual_events = NULL,
  cfg = cfg_dbg,
  from_ca = "2018-01-01",
  to_ca   = Sys.Date(),
  verbose = TRUE,
  use_cache = TRUE,
  force_refresh = FALSE,
  n_workers = 1L,

  # This now works after Patch 1
  force_symbols = sus
)

panel_adj <- res$panel_adj
af2_validate_panel_adj(panel_adj)

# ----------------------------
# PATCH C — Split-day shock sanity check (collision-proof)
# ----------------------------
# Goal:
# On dates where a split exists:
#   - raw close should show a large 1-day mechanical jump
#   - adjusted close should show a much smaller jump
#
# This version avoids merge name collisions with panel_adj.

events <- data.table::as.data.table(res$events)

# Keep only actual split events (price factors != 1)
spl_ev <- events[
  is.finite(split_value) & split_value != 1,
  .(
    symbol,
    refdate,
    split_value_event = split_value,
    source_mask_event = source_mask,
    has_manual_event  = has_manual
  )
]

panel_chk <- data.table::as.data.table(panel_adj)
data.table::setorder(panel_chk, symbol, refdate)

# Attach event-side split info WITHOUT clobbering any existing split_value
panel_chk <- merge(
  panel_chk,
  spl_ev,
  by = c("symbol", "refdate"),
  all.x = TRUE
)

# Choose a unified split factor column for diagnostics:
# Prefer the event table value when present; fall back to panel's split_value if it exists.
panel_chk[, split_factor := split_value_event]

if ("split_value" %in% names(panel_chk)) {
  panel_chk[is.na(split_factor) & is.finite(split_value) & split_value != 1,
            split_factor := split_value]
}

# Safety: ensure expected price columns exist
need_cols <- c("close_raw", "close_adj_final")
missing_cols <- setdiff(need_cols, names(panel_chk))
if (length(missing_cols)) {
  stop("PATCH C error: missing columns in panel_adj: ",
       paste(missing_cols, collapse = ", "))
}

# Day-to-day ratios
panel_chk[, `:=`(
  raw_ratio = close_raw / data.table::shift(close_raw),
  adj_ratio = close_adj_final / data.table::shift(close_adj_final)
), by = symbol]

# Scale-free shock size
panel_chk[, `:=`(
  raw_shock = abs(log(raw_ratio)),
  adj_shock = abs(log(adj_ratio))
)]

# Only true split days
split_days <- panel_chk[
  is.finite(split_factor) & split_factor != 1
]

if (nrow(split_days)) {

  cat("\nAF2_DIAG04B: Split-day shock summary",
      "(expect adj_shock << raw_shock)\n")

  by_sym <- split_days[, .(
    n_split_days   = .N,
    mean_raw_shock = mean(raw_shock, na.rm = TRUE),
    mean_adj_shock = mean(adj_shock, na.rm = TRUE),
    pct_improved   = mean(adj_shock < raw_shock, na.rm = TRUE)
  ), by = symbol][order(-n_split_days, -pct_improved)]

  print(by_sym)

  cat("\nAF2_DIAG04B: Worst raw split-day jumps (top 20)\n")
  print(
    split_days[
      order(-raw_shock)
    ][1:min(20L, .N),
      .(
        symbol, refdate,
        split_factor,
        close_raw, raw_ratio, raw_shock,
        close_adj_final, adj_ratio, adj_shock,
        source_mask_event, has_manual_event
      )
    ]
  )

} else {
  cat("\nAF2_DIAG04B: No split days found in this window.\n")
}

af2_log("AF2_DIAG04B:", "panel_adj rows=", nrow(panel_adj),
        " symbols=", length(unique(panel_adj$symbol)))

print(panel_adj[, .N, by = adjustment_state][order(-N)])
