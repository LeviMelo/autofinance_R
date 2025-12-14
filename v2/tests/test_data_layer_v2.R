# v2/tests/test_data_layer_v2.R
# Integration regression test for v2 data layer AFTER patches:
# - module load stability (zzz_depends first)
# - selective CA coverage (Set A + Set B)
# - split validator sanity (audit + jump reduction)
# - FII dividend coverage measured against *actual Set A logic*
# - screener/ranking runs (or fallback features)

rm(list = ls(all.names = TRUE))
gc()
options(stringsAsFactors = FALSE)
Sys.setenv(TZ = "UTC")

# ----------------------------
# User knobs
# ----------------------------
FORCE_REFRESH_CA_BATCH_CACHE <- isTRUE(get0("FORCE_REFRESH_CA_BATCH_CACHE", ifnotfound = FALSE))
RUN_PARALLEL_MINITEST        <- isTRUE(get0("RUN_PARALLEL_MINITEST", ifnotfound = TRUE))
FORCE_SYMBOLS                <- get0("FORCE_SYMBOLS", ifnotfound = c("AURA33"))
FORCE_SYMBOLS                <- unique(toupper(trimws(as.character(FORCE_SYMBOLS))))
FORCE_SYMBOLS                <- FORCE_SYMBOLS[nzchar(FORCE_SYMBOLS)]

# FII coverage expectation (measured on Set A eligible FIIs)
MIN_FII_COVERAGE_PCT <- as.numeric(get0("MIN_FII_COVERAGE_PCT", ifnotfound = 60)) # 60% is realistic
MIN_FII_COVERAGE_N   <- as.integer(get0("MIN_FII_COVERAGE_N", ifnotfound = 50))  # at least 50 FIIs w/ divs

# Split-day improvement expectations
MIN_SPLIT_IMPROVE_SHARE <- as.numeric(get0("MIN_SPLIT_IMPROVE_SHARE", ifnotfound = 0.90))

# ----------------------------
# Helpers
# ----------------------------
.msg <- function(...) cat(paste0(..., collapse=""), "\n")
stopf <- function(...) stop(sprintf(...), call. = FALSE)

source_dir_safe <- function(path) {
  if (!dir.exists(path)) return(invisible(FALSE))
  files <- list.files(path, pattern="\\.R$", full.names=TRUE, recursive=FALSE)
  files <- sort(files)
  zzz  <- files[grepl("zzz_depends\\.R$", files)]
  rest <- files[!grepl("zzz_depends\\.R$", files)]
  for (f in c(zzz, rest)) {
    tryCatch(source(f, local = FALSE), error = function(e) {
      stopf("Failed sourcing %s:\n%s", f, conditionMessage(e))
    })
  }
  invisible(TRUE)
}

source_v2_modules <- function(root=".") {
  mod_root <- file.path(root, "v2", "modules")
  if (!dir.exists(mod_root)) stopf("Run from project root (missing %s).", mod_root)

  mods <- sort(list.dirs(mod_root, full.names = TRUE, recursive = FALSE))
  core <- mods[grepl("^00_", basename(mods))]
  oth  <- mods[!grepl("^00_", basename(mods))]

  .msg("[1/6] Sourcing modules...")
  for (m in c(core, oth)) {
    rdir <- file.path(m, "R")
    if (dir.exists(rdir)) {
      .msg("  - ", basename(m))
      source_dir_safe(rdir)
    }
  }
  invisible(TRUE)
}

delete_batch_ca_caches <- function(cache_dir = "v2/data/cache/corp_actions") {
  if (!dir.exists(cache_dir)) return(invisible(FALSE))
  files <- list.files(cache_dir, pattern = "^corp_actions_.*\\.rds$", full.names = TRUE)
  if (!length(files)) return(invisible(FALSE))
  ok <- file.remove(files)
  .msg("Deleted ", sum(ok), "/", length(files), " batch corp_actions cache files.")
  invisible(TRUE)
}

need_fn <- function(x) if (!exists(x, mode="function")) stopf("Missing function: %s", x)

# attempt to run screener if present
run_screener_if_exists <- function(panel_adj_dt) {
  cand <- c("af2_run_screener", "af2_run_screener_v2")
  fn <- cand[cand %in% ls(.GlobalEnv)][1]
  if (is.na(fn) || is.null(fn)) return(NULL)

  f <- get(fn, mode="function")
  fml <- names(formals(f))
  .msg("[6/6] Running screener via ", fn, "() ...")

  # pass allow_unresolved if supported
  if ("allow_unresolved" %in% fml) {
    return(f(panel_adj_dt, allow_unresolved = TRUE))
  }
  return(f(panel_adj_dt))
}

# ----------------------------
# 1) Source modules + config
# ----------------------------
source_v2_modules(getwd())
need_fn("af2_get_config")
need_fn("af2_build_panel_adj_selective")
need_fn("af2_ca_select_candidates")

cfg <- af2_get_config()

# ----------------------------
# 2) Load/build universe_raw
# ----------------------------
.msg("[2/6] Loading universe_raw...")
cache_path <- "v2/data/cache/universe_raw_latest.rds"
if (file.exists(cache_path)) {
  universe_raw <- readRDS(cache_path)
  .msg("Loaded: ", cache_path)
} else {
  # minimal build fallback
  if (!exists("af2_b3_build_universe", mode="function")) {
    stopf("No universe cache and af2_b3_build_universe() not found.")
  }
  y_end <- as.integer(format(Sys.Date(), "%Y"))
  universe_raw <- af2_b3_build_universe(years = (y_end-1L):y_end)
  dir.create(dirname(cache_path), recursive=TRUE, showWarnings=FALSE)
  saveRDS(universe_raw, cache_path)
  .msg("Built + cached: ", cache_path)
}

library(data.table)
u <- as.data.table(universe_raw)

# Normalize (tests should not rely on upstream perfect normalization)
u[, symbol := toupper(trimws(as.character(symbol)))]
u[, asset_type := tolower(trimws(as.character(asset_type)))]
u[, refdate := as.Date(refdate)]

# minimal contract check
req_u <- c("symbol","asset_type","refdate","open","high","low","close","turnover","qty")
miss_u <- setdiff(req_u, names(u))
if (length(miss_u)) stopf("universe_raw missing cols: %s", paste(miss_u, collapse=", "))

.msg("universe_raw rows=", nrow(u),
     " syms=", length(unique(u$symbol)),
     " range=", min(u$refdate, na.rm=TRUE), " -> ", max(u$refdate, na.rm=TRUE))

# ----------------------------
# 3) Candidate selection sanity
# ----------------------------
.msg("[3/6] Candidate selection...")
cand <- af2_ca_select_candidates(u, cfg = cfg, verbose = TRUE)
cand <- unique(toupper(trimws(as.character(cand))))
cand <- cand[nzchar(cand)]

.msg("Candidates n=", length(cand))
if (length(FORCE_SYMBOLS)) .msg("Force symbols: ", paste(FORCE_SYMBOLS, collapse=", "))

# ----------------------------
# 4) Optional cache refresh + parallel mini-test
# ----------------------------
if (isTRUE(FORCE_REFRESH_CA_BATCH_CACHE)) delete_batch_ca_caches("v2/data/cache/corp_actions")

if (isTRUE(RUN_PARALLEL_MINITEST) && exists("af2_ca_build_registry", mode="function")) {
  .msg("\n--- Parallel registry mini-test (n_workers=2) ---")
  mini_syms <- unique(c("PETR4","VALE3","ITUB4","TAEE11","HGLG11","BBAS3"))
  mini_syms <- mini_syms[mini_syms %chin% unique(u$symbol)]
  if (length(mini_syms) >= 2) {
    mini <- af2_ca_build_registry(
      symbols = mini_syms,
      cfg = cfg,
      from = "2022-01-01",
      to = Sys.Date(),
      verbose = TRUE,
      use_cache = FALSE,
      force_refresh = TRUE,
      n_workers = 2L,
      cache_mode = "batch"
    )
    mini <- as.data.table(mini)
    .msg("Mini-test action_type counts:")
    print(mini[, .N, by=action_type][order(-N)])
  } else {
    .msg("Skipping mini-test: insufficient mini_syms in universe.")
  }
}

# ----------------------------
# 5) Build pipeline (selective)
# ----------------------------
.msg("\n[4/6] Building panel_adj_result...")
res <- af2_build_panel_adj_selective(
  universe_raw   = u,
  force_symbols  = FORCE_SYMBOLS,
  verbose        = TRUE
)

# required outputs
need_fields <- c("panel_adj","events","corp_actions_apply","corp_actions_quarantine","split_audit","residual_jump_audit")
miss_f <- setdiff(need_fields, names(res))
if (length(miss_f)) stopf("panel_adj_result missing fields: %s", paste(miss_f, collapse=", "))

panel  <- as.data.table(res$panel_adj)
events <- as.data.table(res$events)
ca_apply <- as.data.table(res$corp_actions_apply)
audit  <- as.data.table(res$split_audit)
rj     <- as.data.table(res$residual_jump_audit)

# ----------------------------
# 5a) Core invariants
# ----------------------------
.msg("[5/6] Core invariants...")

# no duplicate market keys
stopifnot(panel[, anyDuplicated(paste(symbol, refdate))] == 0L)

# required cols exist
req_panel <- c("symbol","refdate","asset_type","close_raw","close_adj_final","adjustment_state")
miss_p <- setdiff(req_panel, names(panel))
if (length(miss_p)) stopf("panel_adj missing cols: %s", paste(miss_p, collapse=", "))

# events sane
stopifnot(events[, all(is.finite(split_value) & split_value > 0)])
stopifnot(events[, all(is.finite(div_cash) & div_cash >= 0)])

# ca_apply must not leak row_id
if ("row_id" %in% names(ca_apply)) stopf("row_id leaked into corp_actions_apply")

# audit sanity: kept must have eff_refdate + chosen_value
if (nrow(audit) && "status" %in% names(audit)) {
  print(audit[, .N, by=status][order(-N)])
  if (nrow(audit[status=="kept"])) {
    stopifnot(audit[status=="kept", all(!is.na(eff_refdate) & is.finite(chosen_value) & chosen_value > 0)])
  }
}

# ----------------------------
# 5b) Split-day improvement test
# ----------------------------
splits <- unique(ca_apply[action_type=="split", .(symbol, refdate)])
if (nrow(splits)) {
  setkey(panel, symbol, refdate)
  panel[, lr_raw := log(close_raw / shift(close_raw)), by=symbol]
  panel[, lr_adj := log(close_adj_final / shift(close_adj_final)), by=symbol]

  at_split <- panel[splits, on=.(symbol, refdate), nomatch=0L]
  at_split <- at_split[is.finite(lr_raw) & is.finite(lr_adj)]
  if (nrow(at_split)) {
    at_split[, `:=`(abs_raw = abs(lr_raw), abs_adj = abs(lr_adj))]
    share_improved <- mean(at_split$abs_adj < at_split$abs_raw, na.rm=TRUE)
    .msg("Split-day evaluated: ", nrow(at_split),
         " | share improved: ", round(share_improved, 4),
         " | median improvement: ", round(median(at_split$abs_raw - at_split$abs_adj, na.rm=TRUE), 4))

    if (!is.finite(share_improved) || share_improved < MIN_SPLIT_IMPROVE_SHARE) {
      stopf("Split improvement share too low (%.3f < %.3f).", share_improved, MIN_SPLIT_IMPROVE_SHARE)
    }

    .msg("Worst 10 abs_adj after adjustment:")
    print(at_split[order(-abs_adj)][1:10, .(symbol, refdate, abs_raw, abs_adj)])
  }
}

# ----------------------------
# 5c) AURA33 jump reduction (if present)
# ----------------------------
if ("AURA33" %chin% panel$symbol) {
  x <- panel[symbol=="AURA33"][order(refdate)]
  raw_jump <- max(abs(diff(log(x$close_raw))), na.rm=TRUE)
  adj_jump <- max(abs(diff(log(x$close_adj_final))), na.rm=TRUE)
  .msg("AURA33 max abs logret raw=", round(raw_jump, 6), " adj=", round(adj_jump, 6))

  # not ultra-strict; we just want "meaningful reduction"
  if (is.finite(raw_jump) && is.finite(adj_jump)) {
    if (!(adj_jump < raw_jump * 0.5)) stopf("AURA33 did not show meaningful jump reduction.")
  }
}

# ----------------------------
# 5d) FII dividend coverage measured on Set A-eligible FIIs
# ----------------------------
liq_window_days <- as.integer(cfg$ca_prefilter_liq_window_days %||% 63L)
end_date <- max(u$refdate, na.rm=TRUE)
liq_start <- end_date - ceiling(liq_window_days * 1.6)

u_liq <- u[refdate >= liq_start & refdate <= end_date]
u_liq[, traded_flag := is.finite(close) & !is.na(close)]

stats_liq <- u_liq[, .(
  median_turnover = median(turnover, na.rm=TRUE),
  days_traded_ratio = mean(traded_flag, na.rm=TRUE)
), by=.(symbol, asset_type)]

stats_liq[is.na(median_turnover), median_turnover := 0]
stats_liq[is.na(days_traded_ratio), days_traded_ratio := 0]

setA <- stats_liq[
  median_turnover >= (cfg$min_turnover %||% 5e5) &
  days_traded_ratio >= (cfg$min_days_traded_ratio %||% 0.8)
]

setA_fii <- setA[asset_type=="fii", unique(symbol)]
panel_min <- min(panel$refdate, na.rm=TRUE)
panel_max <- max(panel$refdate, na.rm=TRUE)
ev_win <- events[refdate >= panel_min & refdate <= panel_max]

fii_with_divs <- intersect(setA_fii, unique(ev_win[div_cash > 0, symbol]))
cov_pct <- if (length(setA_fii)) 100 * length(fii_with_divs) / length(setA_fii) else NA_real_

.msg("\nFII dividend coverage (Set A eligible):")
.msg("  Set A FIIs: ", length(setA_fii))
.msg("  With div events: ", length(fii_with_divs))
.msg("  Coverage %: ", if (is.finite(cov_pct)) round(cov_pct,1) else "NA")

if (length(setA_fii) >= 10) {
  if (!is.finite(cov_pct) || cov_pct < MIN_FII_COVERAGE_PCT || length(fii_with_divs) < MIN_FII_COVERAGE_N) {
    .msg("Missing SetA FIIs (first 50):")
    print(head(setdiff(setA_fii, fii_with_divs), 50))
    stopf("FII coverage below expectation (pct<%.1f or n<%d).", MIN_FII_COVERAGE_PCT, MIN_FII_COVERAGE_N)
  }
}

# ----------------------------
# 6) Run screener/ranking
# ----------------------------
scr <- run_screener_if_exists(panel)

if (!is.null(scr) && is.list(scr) && "features" %in% names(scr)) {
  feats <- as.data.table(scr$features)
  .msg("Screener features rows=", nrow(feats), " syms=", length(unique(feats$symbol)))
  .msg("Top 30 by abs(ret_21d) if present:")
  if ("ret_21d" %in% names(feats)) {
    print(feats[is.finite(ret_21d)][order(-abs(ret_21d))][1:30,
               .(symbol, ret_21d, vol_21d, max_dd, median_turnover, asset_type)])
  } else {
    .msg("features has no ret_21d column; printing head():")
    print(head(feats, 30))
  }
} else {
  .msg("No screener entrypoint found or no $features returned. (This is OK.)")
}

.msg("\n✅ v2 data-layer integration test PASSED.")
