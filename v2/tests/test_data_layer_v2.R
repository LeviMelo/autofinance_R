# v2/tests/test_data_layer_v2.R
# Smoke + deep diagnostics for v2 data layer (candidates -> CA fetch -> events -> adjustments -> features)

# =========================
# 1) HARD RESET (MUST BE FIRST)
# =========================
rm(list = ls(all.names = TRUE))
gc()

options(stringsAsFactors = FALSE)
Sys.setenv(TZ = "UTC")

# =========================
# 2) USER KNOBS
# =========================
# NOTE:
# - This block is intentionally written so it NEVER errors if you refactor/reset.
# - Edit the *defaults* here when you want to change behavior.

WATCH <- get0(
  "WATCH",
  ifnotfound = c("SEQL3", "IFCM3", "GOLD11", "GOGL34", "PETR4", "VALE3", "ITUB4")
)
WATCH <- unique(toupper(trimws(as.character(WATCH))))
WATCH <- WATCH[nzchar(WATCH)]

FORCE_REFRESH_CA_BATCH_CACHE <- isTRUE(get0("FORCE_REFRESH_CA_BATCH_CACHE", ifnotfound = FALSE))
RUN_PARALLEL_MINITEST        <- isTRUE(get0("RUN_PARALLEL_MINITEST", ifnotfound = TRUE))
HARD_FAIL_IF_NO_DIVIDENDS    <- isTRUE(get0("HARD_FAIL_IF_NO_DIVIDENDS", ifnotfound = TRUE))

# Optional: override candidate cap for faster tests (set NULL to keep cfg)
OVERRIDE_MAX_CANDIDATES <- get0("OVERRIDE_MAX_CANDIDATES", ifnotfound = NULL)

# If you already have universe_raw saved, set this path; otherwise script tries to build/load automatically.
UNIVERSE_RAW_RDS <- get0("UNIVERSE_RAW_RDS", ifnotfound = NULL)
if (!is.null(UNIVERSE_RAW_RDS)) UNIVERSE_RAW_RDS <- as.character(UNIVERSE_RAW_RDS)

# =========================
# 3) ASSERT PROJECT ROOT
# =========================
if (!dir.exists("v2/modules")) {
  stop("Run this from the project root (the folder that contains v2/modules).", call. = FALSE)
}

# =========================
# 4) HELPERS
# =========================
.msg <- function(...) cat(paste0(..., collapse = ""), "\n")

source_all_v2_modules <- function(mod_root = "v2/modules") {
  files <- list.files(mod_root, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
  if (!length(files)) stop("No R files found under v2/modules.", call. = FALSE)

  # Deterministic order; numeric module dirs should already sort correctly
  files <- sort(normalizePath(files, winslash = "/", mustWork = TRUE))

  # Prefer 00_core first if present
  core_first <- grepl("/00_", files) | grepl("/00core", files, ignore.case = TRUE)
  files <- c(files[core_first], files[!core_first])

  .msg("Sourcing ", length(files), " files under ", mod_root, " ...")
  for (f in files) {
    tryCatch(
      source(f, local = FALSE),
      error = function(e) {
        stop("Failed sourcing: ", f, "\n", conditionMessage(e), call. = FALSE)
      }
    )
  }
  invisible(files)
}

assert_has <- function(fn) {
  if (!exists(fn, mode = "function")) stop("Missing required function: ", fn, call. = FALSE)
}

as_dt <- function(x) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table not installed.", call. = FALSE)
  data.table::as.data.table(x)
}

# Safely delete only batch corp_actions caches (not by_symbol)
delete_batch_ca_caches <- function(cache_dir = "v2/data/cache/corp_actions") {
  if (!dir.exists(cache_dir)) return(invisible(FALSE))
  files <- list.files(cache_dir, pattern = "^corp_actions_.*\\.rds$", full.names = TRUE)
  if (!length(files)) return(invisible(FALSE))
  ok <- file.remove(files)
  .msg("Deleted ", sum(ok), "/", length(files), " batch corp_actions cache files.")
  invisible(TRUE)
}

# Try to obtain universe_raw in a robust way
get_or_build_universe_raw <- function() {
  
  # 1. Try explicit user path
  if (!is.null(UNIVERSE_RAW_RDS) && file.exists(UNIVERSE_RAW_RDS)) {
    .msg("Loading universe_raw from: ", UNIVERSE_RAW_RDS)
    return(readRDS(UNIVERSE_RAW_RDS))
  }

  # 2. Try default cache location
  default_cache <- "v2/data/cache/universe_raw_latest.rds"
  if (file.exists(default_cache)) {
    .msg("Loading universe_raw from: ", default_cache)
    return(readRDS(default_cache))
  }

  # 3. Try to build it (PATCHED: Added af2_b3_build_universe)
  candidates <- c(
    "af2_b3_build_universe",       # <--- The correct v2 function
    "af2_build_universe",          # Legacy alias
    "af2_cotahist_build_universe"  # Legacy alias
  )

  for (fn in candidates) {
    if (exists(fn, mode = "function")) {
      .msg("Building universe_raw via ", fn, "() ...")
      
      # Default to last 2 years + current year to be safe and fast
      y_end <- as.integer(format(Sys.Date(), "%Y"))
      y_start <- y_end - 1L
      
      # Call builder
      res <- get(fn)(years = y_start:y_end)
      
      # Auto-save to cache for next time
      .msg("Saving universe_raw to ", default_cache, " for future runs...")
      dir.create(dirname(default_cache), recursive = TRUE, showWarnings = FALSE)
      saveRDS(res, default_cache)
      
      return(res)
    }
  }

  stop(
    "Could not load/build universe_raw.\n",
    "Function 'af2_b3_build_universe' was not found after sourcing modules.\n",
    "Check if v2/modules/01_b3_universe/R/build_universe.R exists.",
    call. = FALSE
  )
}

# Fallback features (only used if your screener entrypoint is unavailable)
compute_fallback_features <- function(panel_adj_dt, lookback = 253L) {
  dt <- as_dt(panel_adj_dt)
  data.table::setorder(dt, symbol, refdate)

  out <- dt[, {
    x <- close_adj_final
    x <- x[is.finite(x) & x > 0]
    if (length(x) < 30) {
      .(end_refdate = max(refdate), n_obs = .N,
        ret_21d = NA_real_, vol_21d = NA_real_, max_dd = NA_real_,
        median_turnover = stats::median(turnover, na.rm = TRUE),
        asset_type = asset_type[.N])
    } else {
      # last lookback rows by date
      idx <- tail(seq_len(.N), lookback)
      p <- close_adj_final[idx]
      d <- refdate[idx]

      # returns
      r <- diff(p) / data.table::shift(p, 1L, type = "lag")[idx][-1]
      # safer: log returns
      lr <- diff(log(p))

      # 21d total return
      ret_21d <- if (length(p) >= 22) (p[length(p)] / p[length(p) - 21] - 1) else NA_real_

      # 21d vol annualized (sqrt(252))
      vol_21d <- {
        lr21 <- tail(lr, 21)
        if (length(lr21) >= 10) stats::sd(lr21, na.rm = TRUE) * sqrt(252) else NA_real_
      }

      # max drawdown inside window
      cm <- cummax(p)
      dd <- p / cm - 1
      max_dd <- min(dd, na.rm = TRUE)

      .(end_refdate = max(d), n_obs = length(p),
        ret_21d = ret_21d, vol_21d = vol_21d, max_dd = max_dd,
        median_turnover = stats::median(turnover, na.rm = TRUE),
        asset_type = asset_type[.N])
    }
  }, by = symbol]

  out[]
}

# =========================
# 5) SOURCE MODULES + LOAD CFG
# =========================
source_all_v2_modules("v2/modules")

assert_has("af2_get_config")
assert_has("af2_ca_select_candidates")
assert_has("af2_build_panel_adj_selective")

cfg <- af2_get_config()

# Optional cap override for faster runs
if (!is.null(OVERRIDE_MAX_CANDIDATES)) {
  cfg$ca_prefilter_max_candidates <- as.integer(OVERRIDE_MAX_CANDIDATES)
  .msg("OVERRIDE: cfg$ca_prefilter_max_candidates = ", cfg$ca_prefilter_max_candidates)
}

# Make sure new knobs exist (won't fail if absent; just informative)
.msg("CFG snapshot (relevant):")
flat <- unlist(cfg, recursive = TRUE)
print(flat[grep("ca_|corp|split|div|prefilter|gap|jump|max_candidates|cache_mode|fetch_mode",
                names(flat), ignore.case = TRUE)])

# =========================
# 6) GET universe_raw
# =========================
universe_raw <- get_or_build_universe_raw()
universe_raw <- as_dt(universe_raw)

.msg("universe_raw: rows=", nrow(universe_raw), " syms=", length(unique(universe_raw$symbol)),
     " date_range=", min(universe_raw$refdate), " -> ", max(universe_raw$refdate))

# =========================
# 7) CANDIDATE SELECTION DIAGNOSTICS
# =========================
cand <- af2_ca_select_candidates(universe_raw, cfg = cfg, verbose = TRUE)
cand <- unique(toupper(cand))

.msg("Candidate set: n=", length(cand))
cand_hit <- setNames(WATCH %in% cand, WATCH)
print(cand_hit)

# Require SEQL3/IFCM3 in candidates (your must-have regression test)
if (!("SEQL3" %in% cand && "IFCM3" %in% cand)) {
  stop("Candidate regression: SEQL3/IFCM3 not in candidates. Fix select_candidates.R.", call. = FALSE)
}

# =========================
# 8) OPTIONAL: FORCE REFRESH CA BATCH CACHE
# =========================
if (isTRUE(FORCE_REFRESH_CA_BATCH_CACHE)) {
  delete_batch_ca_caches("v2/data/cache/corp_actions")
}

# =========================
# 9) OPTIONAL: PARALLEL MINITEST (registry only)
# =========================
if (isTRUE(RUN_PARALLEL_MINITEST) && exists("af2_ca_build_registry", mode = "function")) {

  .msg("\n--- Parallel mini-test (af2_ca_build_registry, n_workers=2) ---")
  mini_syms <- unique(c("PETR4", "VALE3", "ITUB4", "TAEE11", "HGLG11", "BBAS3"))
  mini_syms <- mini_syms[mini_syms %in% unique(toupper(universe_raw$symbol))]

  if (length(mini_syms) >= 2) {
    mini <- tryCatch(
      af2_ca_build_registry(
        symbols = mini_syms,
        cfg = cfg,
        from = "2022-01-01",
        to = Sys.Date(),
        verbose = TRUE,
        use_cache = FALSE,
        force_refresh = TRUE,
        n_workers = 2L,
        cache_mode = "batch"
      ),
      error = function(e) e
    )

    if (inherits(mini, "error")) {
      stop("Parallel registry mini-test FAILED:\n", conditionMessage(mini), call. = FALSE)
    } else {
      mini <- as_dt(mini)
      .msg("Parallel registry mini-test OK. action_type counts:")
      print(mini[, .N, by = action_type][order(-N)])
    }
  } else {
    .msg("Skipping parallel mini-test: not enough mini_syms present in universe_raw.")
  }
}

# =========================
# 10) BUILD PANEL (SELECTIVE CA + ADJUSTER)
# =========================
.msg("\n--- Building panel_adj_result (selective) ---")
panel_adj_result <- af2_build_panel_adj_selective(universe_raw = universe_raw)

# Contract checks
needed <- c("panel_adj", "events", "corp_actions_apply", "corp_actions_quarantine", "split_audit")
miss <- setdiff(needed, names(panel_adj_result))
if (length(miss)) {
  stop("panel_adj_result missing fields: ", paste(miss, collapse = ", "), call. = FALSE)
}

panel_adj_dt <- as_dt(panel_adj_result$panel_adj)
events <- as_dt(panel_adj_result$events)
ca_apply <- as_dt(panel_adj_result$corp_actions_apply)
ca_quar <- as_dt(panel_adj_result$corp_actions_quarantine)
audit <- as_dt(panel_adj_result$split_audit)

.msg("panel_adj: rows=", nrow(panel_adj_dt), " syms=", length(unique(panel_adj_dt$symbol)))
.msg("events:    rows=", nrow(events), " syms=", length(unique(events$symbol)))
.msg("ca_apply:  rows=", nrow(ca_apply), " types=", paste(unique(ca_apply$action_type), collapse = ", "))
.msg("ca_quar:   rows=", nrow(ca_quar))

# =========================
# 11) DIVIDEND ASSERTIONS
# =========================
ca_counts <- ca_apply[, .N, by = action_type][order(-N)]
.msg("\nCorporate actions APPLY counts:")
print(ca_counts)

n_div_apply <- if ("dividend" %in% ca_apply$action_type) nrow(ca_apply[action_type == "dividend"]) else 0L
n_div_ev <- nrow(events[div_cash > 0])

.msg("Dividend rows in ca_apply: ", n_div_apply)
.msg("Dividend-positive event days (events$div_cash>0): ", n_div_ev)

if (HARD_FAIL_IF_NO_DIVIDENDS && (n_div_apply == 0L || n_div_ev == 0L)) {
  stop(
    "Dividend ghost still present: no dividends made it into ca_apply/events.\n",
    "Fix fetcher wiring (chart events) OR confirm Yahoo returns dividends in your environment.\n",
    "Tip: inspect ca_apply[action_type=='dividend'] and the registry cache file created this run.",
    call. = FALSE
  )
}

# =========================
# 12) SPLIT VALIDATION SUMMARY (KEPT/REJECTED/UNVERIFIED)
# =========================
if ("status" %in% names(audit)) {
  .msg("\nSplit audit status counts:")
  print(audit[, .N, by = status][order(-N)])
}

# Check watch symbols CA rows
.msg("\nCA rows for WATCH:")
print(ca_apply[symbol %chin% WATCH][order(symbol, action_type, refdate)][1:200])

# =========================
# 13) RESIDUAL JUMP SAFETY NET CHECK
# =========================
if ("residual_jump_audit" %in% names(panel_adj_result)) {
  rj <- as_dt(panel_adj_result$residual_jump_audit)
  .msg("\nResidual jump audit: top offenders:")
  print(rj[order(-residual_max_abs_logret)][1:30])
} else {
  .msg("\nNOTE: residual_jump_audit not present in panel_adj_result; skipping this check.")
}

# =========================
# 14) PER-SYMBOL DIAGNOSTICS
# =========================
if (exists("af2_diag_symbol", mode = "function")) {
  .msg("\n--- af2_diag_symbol(WATCH) ---")
  for (s in WATCH) {
    if (s %in% unique(panel_adj_dt$symbol)) {
      af2_diag_symbol(
        symbol = s,
        panel_adj = panel_adj_dt,
        events = events,
        corp_actions_apply = ca_apply,
        split_audit = audit,
        show_plot = TRUE
      )
    }
  }
} else {
  .msg("\nNOTE: af2_diag_symbol() not found; skipping per-symbol diag.")
}

# =========================
# 15) REBUILD RES / FEATURES (Screener if available; fallback otherwise)
# =========================
res <- NULL
if (exists("af2_run_screener", mode = "function")) {
  .msg("\n--- Running af2_run_screener(panel_adj_dt) ---")
  res <- af2_run_screener(panel_adj_dt)
} else if (exists("af2_run_screener_v2", mode = "function")) {
  .msg("\n--- Running af2_run_screener_v2(panel_adj_dt) ---")
  res <- af2_run_screener_v2(panel_adj_dt)
}

if (!is.null(res) && is.list(res) && "features" %in% names(res)) {
  feats <- as_dt(res$features)
  .msg("\nScreener features present. WATCH rows:")
  print(feats[symbol %chin% WATCH, .(symbol, end_refdate, n_obs, ret_21d, vol_21d, max_dd, median_turnover, asset_type)][order(symbol)])

  .msg("\nTop 30 by abs(ret_21d):")
  top <- feats[is.finite(ret_21d)][order(-abs(ret_21d))][1:30]
  print(top[, .(symbol, ret_21d, vol_21d, max_dd, median_turnover, asset_type)])

} else {
  .msg("\nNOTE: Screener entrypoint not found or did not return $features. Using fallback feature computation.")
  feats <- compute_fallback_features(panel_adj_dt, lookback = 253L)

  .msg("\nFallback features (WATCH):")
  print(feats[symbol %chin% WATCH][order(symbol)])

  .msg("\nFallback top 30 by abs(ret_21d):")
  top <- feats[is.finite(ret_21d)][order(-abs(ret_21d))][1:30]
  print(top[, .(symbol, ret_21d, vol_21d, max_dd, median_turnover, asset_type)])
}

# =========================
# 16) FINAL SUCCESS MESSAGE
# =========================
.msg("\n✅ v2 data-layer test completed.")
.msg("If HARD_FAIL_IF_NO_DIVIDENDS=TRUE, then dividends are confirmed present end-to-end.")