# playground/ipea_discovery_suite.R
# Standalone IPEA discovery + profiling suite (ipeadatar)
# Run from project root: source("playground/ipea_discovery_suite.R")
#
# Artifacts: playground/_runs/<timestamp>/
# Cache:     playground/_cache/ipea/   (PERSISTENT across runs)
#
# Purpose:
#   - Discover what series exist for macro/factor work (not "API ok")
#   - Profile candidate series (coverage, gaps, staleness, territory, transform suitability)
#   - Produce inspectable artifacts (CSV + plots + wide panels)

options(stringsAsFactors = FALSE)

# -------------------------------
# 0) Helpers (standalone)
# -------------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

stop_if_missing <- function(pkgs) {
  miss <- pkgs[!vapply(pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) stop("Missing packages: ", paste(miss, collapse = ", "), call. = FALSE)
}

ts_now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
log_msg <- function(...) cat(sprintf("[%s] %s\n", ts_now(), paste(..., collapse = " ")))

safe_dir <- function(p) { dir.create(p, recursive = TRUE, showWarnings = FALSE); p }

# minimal safe write
safe_fwrite <- function(x, path) {
  safe_dir(dirname(path))
  data.table::fwrite(data.table::as.data.table(x), path)
  path
}

# -------------------------------
# 1) Setup paths (no setwd)
# -------------------------------
stop_if_missing(c("data.table", "ipeadatar", "jsonlite"))

PROJECT_DIR <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
PLAYGROUND_DIR <- file.path(PROJECT_DIR, "playground")
if (!dir.exists(PLAYGROUND_DIR)) stop("Run from project root containing /playground.", call. = FALSE)

RUN_ID  <- format(Sys.time(), "%Y%m%d_%H%M%S")
RUN_DIR <- safe_dir(file.path(PLAYGROUND_DIR, "_runs", RUN_ID))

# Persistent cache (outside timestamp runs)
CACHE_DIR <- safe_dir(file.path(PLAYGROUND_DIR, "_cache", "ipea"))
CACHE_CATALOG <- file.path(CACHE_DIR, "catalog_full_br.rds")

# Per-run artifact dirs
ART_META_DIR <- safe_dir(file.path(RUN_DIR, "meta"))
ART_DATA_DIR <- safe_dir(file.path(RUN_DIR, "data_tail"))
ART_PLOT_DIR <- safe_dir(file.path(RUN_DIR, "plots"))
ART_SEARCH_DIR <- safe_dir(file.path(RUN_DIR, "search"))
ART_PANELS_DIR <- safe_dir(file.path(RUN_DIR, "panels"))

log_msg("RUN_DIR:", RUN_DIR)
log_msg("CACHE_DIR:", CACHE_DIR)

# Save session info for reproducibility
capture.output(sessionInfo(), file = file.path(RUN_DIR, "sessionInfo.txt"))

DT <- data.table::as.data.table

# -------------------------------
# 2) Normalization & detection helpers
# -------------------------------
norm_ascii <- function(x) {
  x <- as.character(x)
  x <- tolower(x)
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

detect_first <- function(nms, candidates) {
  out <- intersect(nms, candidates)
  if (length(out)) out[1] else NA_character_
}

# local OR-search across selected fields (accent-insensitive)
local_search_or <- function(catalog_dt, terms, fields, top_n = 500L) {
  dt <- data.table::copy(DT(catalog_dt))
  fields <- intersect(fields, names(dt))
  if (!length(fields)) return(dt[0])
  
  blob <- do.call(paste, c(dt[, ..fields], sep = " | "))
  blob <- norm_ascii(blob)
  
  terms_n <- norm_ascii(terms)
  hit <- rep(FALSE, length(blob))
  for (t in terms_n) if (nzchar(t)) hit <- hit | grepl(t, blob, fixed = FALSE)
  
  out <- dt[hit]
  out[1:min(.N, top_n)]
}

# -------------------------------
# 3) Persistent caching
# -------------------------------
cache_path <- function(kind = c("meta", "data"), code) {
  kind <- match.arg(kind)
  file.path(CACHE_DIR, sprintf("%s__%s.rds", kind, code))
}

cache_get <- function(kind, code) {
  p <- cache_path(kind, code)
  if (file.exists(p)) readRDS(p) else NULL
}

cache_put <- function(kind, code, obj) {
  p <- cache_path(kind, code)
  saveRDS(obj, p)
  invisible(p)
}

smart_fetch <- function(kind, code, fetch_fn) {
  # Returns: list(data=..., source="CACHE HIT"|"API FETCH"|"API ERROR")
  obj <- cache_get(kind, code)
  if (!is.null(obj)) return(list(data = obj, source = "CACHE HIT"))
  
  obj <- tryCatch(fetch_fn(), error = function(e) e)
  if (inherits(obj, "error")) {
    return(list(data = NULL, source = paste0("API ERROR: ", conditionMessage(obj))))
  }
  cache_put(kind, code, obj)
  list(data = obj, source = "API FETCH")
}

# -------------------------------
# 4) Modeling transforms (fixes your nonsense vol outputs)
# -------------------------------
# We need transforms that make sense across macro series types.
# - "level_positive": indices, FX levels, price/amount levels (>0): use diff(log(value))
# - "rate_percent": interest/inflation rates in %: use diff(value) (pp changes)
# - "level_signed": fiscal balances etc (can be negative): use diff(value)
infer_series_type <- function(name, unity) {
  n <- norm_ascii(name)
  u <- norm_ascii(unity)
  
  # strongest signals
  if (grepl("percent|%|taxa|juros|selic|cdi", paste(n,u), fixed = FALSE)) return("rate_percent")
  if (grepl("ipca|inpc|inflacao|inflation", n)) return("rate_percent") # many inflation series are rates
  if (grepl("indice|index", paste(n,u))) return("level_positive")
  if (grepl("cambio|dolar|usd|brl|ptax|fx", paste(n,u))) return("level_positive")
  if (grepl("saldo|deficit|superavit|resultado", n)) return("level_signed")
  if (grepl("divida|estoque", n)) return("level_positive")
  if (grepl("r\\$|reais|real", u)) return("level_signed") # can be signed, keep diff
  
  # fallback
  "unknown"
}

apply_transform <- function(value, series_type) {
  v <- suppressWarnings(as.numeric(value))
  if (all(is.na(v))) return(rep(NA_real_, length(v)))
  
  if (series_type == "rate_percent") {
    # pp changes
    return(c(NA_real_, diff(v)))
  }
  if (series_type == "level_signed") {
    return(c(NA_real_, diff(v)))
  }
  if (series_type == "level_positive") {
    # log returns; guard against <= 0
    v2 <- v
    v2[v2 <= 0] <- NA_real_
    return(c(NA_real_, diff(log(v2))))
  }
  # unknown: choose diff, safest
  c(NA_real_, diff(v))
}

# infer annualization scale from frequency label
infer_scale <- function(freq_str) {
  f <- norm_ascii(freq_str)
  if (grepl("dia|daily", f)) return(252)
  if (grepl("sem|week",  f)) return(52)
  if (grepl("mes|month", f)) return(12)
  if (grepl("tri|quart", f)) return(4)
  if (grepl("ano|year|anual", f)) return(1)
  12
}

expected_gap_days <- function(freq_str) {
  f <- norm_ascii(freq_str)
  if (grepl("dia|daily", f)) return(1)
  if (grepl("sem|week",  f)) return(7)
  if (grepl("mes|month", f)) return(31)
  if (grepl("tri|quart", f)) return(92)
  if (grepl("ano|year|anual", f)) return(366)
  31
}

# robust z using MAD
robust_outlier_rate <- function(x, z_thresh = 10) {
  x <- x[is.finite(x)]
  if (length(x) < 30) return(NA_real_)
  med <- median(x)
  mad <- median(abs(x - med))
  if (!is.finite(mad) || mad == 0) return(0)
  z <- abs((x - med) / mad)
  mean(z > z_thresh)
}

# -------------------------------
# 5) Territory detection / national-only filtering
# -------------------------------
detect_territory_cols <- function(dt) {
  nms <- names(dt)
  # prefer obvious names
  candidates <- nms[grepl("territ|unidade|uf|regiao|região|pais|país|country", nms, ignore.case = TRUE)]
  unique(candidates)
}

filter_to_national <- function(dt) {
  # Returns list(dt_national, territory_mode, territory_col)
  # If no territory cols, treat as single territory.
  terr_cols <- detect_territory_cols(dt)
  if (!length(terr_cols)) return(list(dt = dt, mode = "NO_TERR_COL", col = NA_character_))
  
  # Search for a column that actually contains Brazil-like labels
  brazil_tokens <- c("brasil", "brazil", "nacional", "br")
  for (col in terr_cols) {
    v <- dt[[col]]
    if (is.null(v)) next
    if (!(is.character(v) || is.factor(v))) next
    vn <- norm_ascii(v)
    if (any(grepl(paste(brazil_tokens, collapse="|"), vn))) {
      keep <- grepl(paste(brazil_tokens, collapse="|"), vn)
      out <- dt[keep]
      if (nrow(out) > 0) return(list(dt = out, mode = "FILTERED_TO_NATIONAL", col = col))
    }
  }
  # Has territory cols but no Brazil row found
  list(dt = dt, mode = "MULTI_TERR_NO_BRAZIL_FOUND", col = terr_cols[1])
}

# -------------------------------
# 6) One-series profiler (writes artifacts)
# -------------------------------
meta_get1 <- function(meta_dt, cols) {
  nm <- names(meta_dt)
  col <- intersect(nm, cols)
  if (!length(col)) return(NA_character_)
  v <- meta_dt[[col[1]]]
  if (is.null(v) || !length(v)) return(NA_character_)
  as.character(v[1])
}

profile_one_code <- function(code, topic, cat_row, language = "br", tail_n = 800L, make_plot = TRUE) {
  # fetch meta + data (cached)
  meta_res <- smart_fetch("meta", code, function() ipeadatar::metadata(code, language = language, quiet = TRUE))
  data_res <- smart_fetch("data", code, function() ipeadatar::ipeadata(code, language = language, quiet = TRUE))
  
  meta_dt <- DT(meta_res$data)
  raw_dt  <- DT(data_res$data)
  
  # detect date/value columns
  dcol <- detect_first(names(raw_dt), c("date","data","DATE","DATA"))
  vcol <- detect_first(names(raw_dt), c("value","valor","VALUE","VALOR"))
  
  if (is.na(dcol) || is.na(vcol)) {
    return(DT(list(
      code = code, topic = topic,
      ok = FALSE,
      error = "Could not detect date/value columns in ipeadata() result",
      meta_source = meta_res$source,
      data_source = data_res$source
    )))
  }
  
  dt <- raw_dt[, .(
    date = as.Date(get(dcol)),
    value_raw = suppressWarnings(as.numeric(as.character(get(vcol))))
  )]
  
  dt <- dt[!is.na(date)]
  data.table::setorder(dt, date)
  
  # apply national filter attempt
  terr_res <- filter_to_national(raw_dt)
  terr_mode <- terr_res$mode
  terr_col  <- terr_res$col
  
  # rebuild dt from filtered view if we actually filtered
  if (terr_mode == "FILTERED_TO_NATIONAL") {
    raw_f <- DT(terr_res$dt)
    dcol2 <- detect_first(names(raw_f), c("date","data","DATE","DATA"))
    vcol2 <- detect_first(names(raw_f), c("value","valor","VALUE","VALOR"))
    if (!is.na(dcol2) && !is.na(vcol2)) {
      dt <- raw_f[, .(
        date = as.Date(get(dcol2)),
        value_raw = suppressWarnings(as.numeric(as.character(get(vcol2))))
      )]
      dt <- dt[!is.na(date)]
      data.table::setorder(dt, date)
    }
  }
  
  # extract meta headline fields
  meta_name       <- meta_get1(meta_dt, c("name","nome"))
  meta_freq       <- meta_get1(meta_dt, c("freq","frequency","frequencia"))
  meta_unity      <- meta_get1(meta_dt, c("unity","unit","unidade"))
  meta_multiplier <- meta_get1(meta_dt, c("multiplier factor","multiplier_factor","multiplier","fator_multiplicador"))
  meta_lastupdate <- meta_get1(meta_dt, c("last update","last_update","lastupdate"))
  meta_status     <- meta_get1(meta_dt, c("status"))
  
  # multiplier (optional scaling)
  mult_num <- suppressWarnings(as.numeric(gsub(",", ".", meta_multiplier)))
  if (!is.finite(mult_num)) mult_num <- 1
  
  dt[, value := value_raw * mult_num]
  
  # stats: coverage, gaps, duplicates, missing
  n_obs <- nrow(dt)
  start_date <- if (n_obs) as.character(dt[1, date]) else NA_character_
  end_date   <- if (n_obs) as.character(dt[.N, date]) else NA_character_
  
  n_na <- sum(is.na(dt$value))
  na_rate <- if (n_obs) n_na / n_obs else NA_real_
  
  n_dupes <- dt[, sum(duplicated(date))]
  
  gap_days <- if (n_obs > 1) as.numeric(diff(dt$date)) else numeric()
  gap_p95 <- if (length(gap_days)) as.numeric(stats::quantile(gap_days, 0.95, na.rm = TRUE)) else NA_real_
  gap_max <- if (length(gap_days)) max(gap_days, na.rm = TRUE) else NA_real_
  gap_med <- if (length(gap_days)) median(gap_days, na.rm = TRUE) else NA_real_
  
  days_stale <- if (n_obs) as.numeric(Sys.Date() - max(dt$date)) else NA_real_
  
  # transform choice
  series_type <- infer_series_type(meta_name, meta_unity)
  scale <- infer_scale(meta_freq)
  exp_gap <- expected_gap_days(meta_freq)
  
  x <- apply_transform(dt$value, series_type)
  x <- x[is.finite(x)]
  drift_ann <- if (length(x) >= 10) mean(x) * scale else NA_real_
  vol_ann   <- if (length(x) >= 10) stats::sd(x) * sqrt(scale) else NA_real_
  acf1      <- if (length(x) >= 20) stats::cor(head(x,-1), tail(x,-1), use="complete.obs") else NA_real_
  out_rate  <- robust_outlier_rate(x, z_thresh = 10)
  
  # "gap severity": how bad compared to expected cadence
  gap_severity <- if (is.finite(gap_max) && is.finite(exp_gap) && exp_gap > 0) gap_max / exp_gap else NA_real_
  gaps_big <- if (length(gap_days)) sum(gap_days > (3 * exp_gap), na.rm = TRUE) else NA_integer_
  
  # Suitability score (heuristic, not sacred)
  # Penalize stale, huge gaps, missing, multi-territory without Brazil filtering.
  score <- 100
  if (!is.finite(days_stale)) score <- score - 30 else if (days_stale > 120) score <- score - 30 else if (days_stale > 30) score <- score - 10
  if (!is.finite(gap_severity)) score <- score - 15 else if (gap_severity > 10) score <- score - 35 else if (gap_severity > 3) score <- score - 15
  if (is.finite(na_rate) && na_rate > 0.05) score <- score - 20
  if (terr_mode == "MULTI_TERR_NO_BRAZIL_FOUND") score <- score - 30
  if (n_dupes > 0) score <- score - 10
  if (is.finite(out_rate) && out_rate > 0.02) score <- score - 10
  score <- max(0, min(100, score))
  
  # write artifacts (meta csv + tail csv)
  safe_fwrite(meta_dt, file.path(ART_META_DIR, sprintf("ipea_meta__%s.csv", code)))
  safe_fwrite(tail(dt, tail_n), file.path(ART_DATA_DIR, sprintf("ipea_tail__%s.csv", code)))
  
  # plot (quick visual sanity)
  if (make_plot && n_obs > 5) {
    png(file.path(ART_PLOT_DIR, sprintf("ipea_plot__%s.png", code)), width = 1200, height = 700)
    on.exit(dev.off(), add = TRUE)
    plot(dt$date, dt$value, type = "l",
         main = paste0(code, " | ", meta_name, " | ", meta_freq, " | ", series_type),
         xlab = "date", ylab = paste0("value (x", mult_num, ")"))
    grid()
    mtext(paste0("territory_mode=", terr_mode, " | meta=", meta_res$source, " | data=", data_res$source),
          side = 3, line = 0.2, cex = 0.8)
  }
  
  DT(list(
    code = code,
    topic = topic,
    ok = TRUE,
    score = score,
    
    meta_name = meta_name,
    meta_freq = meta_freq,
    meta_unity = meta_unity,
    meta_multiplier = meta_multiplier,
    meta_lastupdate = meta_lastupdate,
    meta_status = meta_status,
    
    series_type = series_type,
    transform_scale = scale,
    
    n_obs = n_obs,
    start_date = start_date,
    end_date = end_date,
    days_stale = days_stale,
    
    n_dupes = n_dupes,
    na_rate = na_rate,
    gap_med_days = gap_med,
    gap_p95_days = gap_p95,
    gap_max_days = gap_max,
    gap_severity = gap_severity,
    gaps_over_3x_expected = gaps_big,
    
    drift_ann = drift_ann,
    vol_ann = vol_ann,
    acf1 = acf1,
    outlier_rate = out_rate,
    
    territory_mode = terr_mode,
    territory_col = terr_col,
    
    meta_source = meta_res$source,
    data_source = data_res$source
  ))
}

# -------------------------------
# 7) Main discovery runner
# -------------------------------
run_ipea_discovery <- function(
    language = "br",
    max_hits_per_topic = 800L,
    sample_per_topic = 8L,
    exclude_annual = TRUE,
    prefer_active_only = TRUE,
    make_plots = TRUE
) {
  log_msg("=== IPEA DISCOVERY SUITE (v2, not-toy) ===")
  
  # --- catalog load (persistent cached) ---
  catalog <- NULL
  catalog_src <- NULL
  if (file.exists(CACHE_CATALOG)) {
    catalog <- readRDS(CACHE_CATALOG)
    catalog_src <- "CACHE HIT"
  } else {
    catalog <- ipeadatar::available_series(language = language)
    saveRDS(catalog, CACHE_CATALOG)
    catalog_src <- "API FETCH"
  }
  catalog <- DT(catalog)
  log_msg("Catalog:", catalog_src, "| n=", nrow(catalog), "| cols=", ncol(catalog))
  
  # subjects snapshot (not too big; no need to cache)
  subjects <- DT(ipeadatar::available_subjects(language = language))
  safe_fwrite(subjects, file.path(RUN_DIR, "ipea_subjects.csv"))
  safe_fwrite(catalog,  file.path(RUN_DIR, "ipea_catalog_full.csv"))
  
  # normalize common column names
  if ("codigo" %in% names(catalog)) data.table::setnames(catalog, "codigo", "code")
  if ("nome"   %in% names(catalog)) data.table::setnames(catalog, "nome",   "name")
  if ("tema"   %in% names(catalog)) data.table::setnames(catalog, "tema",   "theme")
  if ("fonte"  %in% names(catalog)) data.table::setnames(catalog, "fonte",  "source")
  if (!("lastupdate" %in% names(catalog))) {
    lu <- detect_first(names(catalog), c("last update","last_update","lastupdate"))
    if (!is.na(lu)) data.table::setnames(catalog, lu, "lastupdate")
  }
  
  # optional filters (don’t over-assume theme; just prefer active if asked)
  if (prefer_active_only && ("status" %in% names(catalog))) {
    catalog[, status_norm := norm_ascii(status)]
    catalog <- catalog[!grepl("inativ|inactive|nao ativo|não ativo", status_norm)]
    log_msg("After active-only filter:", nrow(catalog))
  }
  
  # topic packs (expand as you wish)
  topics <- list(
    inflation = c("ipca","inpc","igp","deflator","inflacao","inflação"),
    rates     = c("selic","cdi","juros","swap","cupom","taxa"),
    fx        = c("cambio","câmbio","dolar","dólar","ptax","usd","brl"),
    activity  = c("pib","ibc","producao","produção","industria","indústria","varejo","servicos","serviços"),
    labor     = c("pnad","desemprego","ocupacao","ocupação","rendimento","salario","salário"),
    credit    = c("credito","crédito","spread","inadimplencia","inadimplência","bancos"),
    fiscal    = c("divida","dívida","resultado","primario","primário","gasto","receita","nfsp")
  )
  
  fields_remote <- c("code","name","theme","source","freq","status","lastupdate")
  fields_local  <- intersect(names(catalog), c("code","name","theme","source","freq","status","lastupdate"))
  
  remote_all <- list()
  local_all  <- list()
  
  for (tp in names(topics)) {
    terms <- topics[[tp]]
    
    # Remote supported search
    r <- tryCatch(
      ipeadatar::search_series(terms = terms, fields = fields_remote, language = language),
      error = function(e) NULL
    )
    r_dt <- DT(r)
    if (nrow(r_dt)) r_dt[, topic := tp]
    safe_fwrite(r_dt, file.path(ART_SEARCH_DIR, sprintf("ipea_search_remote__%s.csv", tp)))
    
    # Local OR-search across full (filtered) catalog
    l_dt <- local_search_or(catalog, terms = terms, fields = fields_local, top_n = max_hits_per_topic)
    if (nrow(l_dt)) l_dt[, topic := tp]
    safe_fwrite(l_dt, file.path(ART_SEARCH_DIR, sprintf("ipea_search_local__%s.csv", tp)))
    
    log_msg("Search:", tp, "| remote_n=", nrow(r_dt), "| local_n=", nrow(l_dt))
    remote_all[[tp]] <- r_dt
    local_all[[tp]]  <- l_dt
  }
  
  remote_dt <- data.table::rbindlist(remote_all, fill = TRUE)
  local_dt  <- data.table::rbindlist(local_all,  fill = TRUE)
  
  safe_fwrite(remote_dt, file.path(RUN_DIR, "ipea_search_remote__ALL.csv"))
  safe_fwrite(local_dt,  file.path(RUN_DIR, "ipea_search_local__ALL.csv"))
  
  # Build candidates = union codes from both sources, then join back to catalog for metadata columns
  if (!("code" %in% names(catalog))) stop("Catalog missing 'code' column after normalization.", call. = FALSE)
  
  cand_codes <- unique(data.table::rbindlist(list(
    remote_dt[, .(topic, code)],
    local_dt[,  .(topic, code)]
  ), fill = TRUE))[!is.na(code) & nzchar(code)]
  
  # drop NA topic (shouldn’t happen, but prevents contamination)
  cand_codes <- cand_codes[!is.na(topic) & nzchar(topic)]
  
  # Correct join direction: keep ONLY searched codes, bring catalog columns
  # (catalog is x, cand_codes is i; nomatch=0 drops anything not in catalog)
  candidates <- catalog[cand_codes, on = "code", nomatch = 0, allow.cartesian = TRUE]
  
  # optional: if a code appears in multiple topics, keep it (discovery), but avoid exact duplicates
  candidates <- unique(candidates, by = c("topic", "code"))
  
  # Exclude annual if requested
  if (exclude_annual && ("freq" %in% names(candidates))) {
    candidates[, freq_norm := norm_ascii(freq)]
    candidates <- candidates[!grepl("ano|year|anual", freq_norm)]
    log_msg("After excluding annual:", nrow(candidates))
  }
  
  # Ranking heuristic for sampling: daily > monthly > quarterly, then freshest lastupdate if available
  if ("freq" %in% names(candidates)) {
    candidates[, rank := data.table::fcase(
      grepl("dia|daily",   norm_ascii(freq)), 1L,
      grepl("mes|month",   norm_ascii(freq)), 2L,
      grepl("tri|quart",   norm_ascii(freq)), 3L,
      grepl("sem|week",    norm_ascii(freq)), 4L,
      default = 99L
    )]
  } else {
    candidates[, rank := 99L]
  }
                    # lastupdate ordering (avoid factor numeric nonsense)
                    if ("lastupdate" %in% names(candidates)) {
                      lu <- candidates$lastupdate
                      if (inherits(lu, "Date")) {
                        candidates[, lastupdate_dt := lu]
                      } else {
                        # try parse
                        candidates[, lastupdate_dt := as.Date(as.character(lastupdate))]
                      }
                    } else candidates[, lastupdate_dt := as.Date(NA)]
                    
                    if ("name" %in% names(candidates)) {
                      candidates[, name_show := as.character(name)]
                    } else {
                      candidates[, name_show := NA_character_]
                    }
                    
                    data.table::setorder(candidates, topic, rank, -lastupdate_dt, name_show)
                    safe_fwrite(candidates, file.path(RUN_DIR, "ipea_candidates_catalog_join.csv"))
                    
                    # Select Top N per topic (ranked order already set earlier)
                    targets_topic <- candidates[, head(.SD, sample_per_topic), by = topic]
                    
                    # Mapping table: a code can belong to multiple topics
                    targets_map <- unique(targets_topic[, .(code, topic)])
                    
                    # Profile each code only once (keep first row per code as “primary”)
                    targets <- unique(targets_topic, by = "code")
                    
                    safe_fwrite(targets_topic, file.path(RUN_DIR, "ipea_targets_by_topic.csv"))
                    safe_fwrite(targets,       file.path(RUN_DIR, "ipea_targets_unique_codes.csv"))
                    log_msg("Targets selected (topic rows):", nrow(targets_topic))
                    log_msg("Targets selected (unique codes):", nrow(targets))
                    
                    # Profile each target
                    prof_list <- list()
                    for (i in seq_len(nrow(targets))) {
                      cd <- targets$code[i]
                      tp <- targets$topic[i]
                      nm <- targets$name_show[i] %||% NA_character_
                      log_msg("Profile:", sprintf("%-22s", cd), "| topic=", tp, "| name=", substr(nm, 1, 60))
                      prof_list[[cd]] <- tryCatch(
                        profile_one_code(cd, tp, targets[i], language = language, make_plot = make_plots),
                        error = function(e) DT(list(code = cd, topic = tp, ok = FALSE, error = conditionMessage(e)))
                      )
                    }
                    profiles <- data.table::rbindlist(prof_list, fill = TRUE)
                    
                    # Order profiles by score desc
                    if ("score" %in% names(profiles)) data.table::setorder(profiles, -score, topic, code)
                    
                    prof_file <- safe_fwrite(profiles, file.path(RUN_DIR, "ipea_candidates_profile.csv"))
                    log_msg("Wrote profile:", prof_file)
                    
                    # Build wide panels by frequency rank (national-only may still include multi-territory failures; use ok==TRUE)
                    ok_profiles <- profiles[ok == TRUE]
                    if (nrow(ok_profiles) > 0) {
                      # We stored tails only; for wide panels we need full cached series again.
                      build_wide <- function(codes, panel_name) {
                        if (!length(codes)) return(NULL)
                        all <- list()
                        for (cd in codes) {
                          dat <- cache_get("data", cd)
                          if (is.null(dat)) next
                          raw <- DT(dat)
                          dcol <- detect_first(names(raw), c("date","data","DATE","DATA"))
                          vcol <- detect_first(names(raw), c("value","valor","VALUE","VALOR"))
                          if (is.na(dcol) || is.na(vcol)) next
                          
                          # national-only filter
                          terr_res <- filter_to_national(raw)
                          raw2 <- if (terr_res$mode == "FILTERED_TO_NATIONAL") DT(terr_res$dt) else raw
                          
                          dcol2 <- detect_first(names(raw2), c("date","data","DATE","DATA"))
                          vcol2 <- detect_first(names(raw2), c("value","valor","VALUE","VALOR"))
                          if (is.na(dcol2) || is.na(vcol2)) next
                          
                          dt <- raw2[, .(date = as.Date(get(dcol2)),
                                         value = suppressWarnings(as.numeric(as.character(get(vcol2)))))]
                          
                          dt <- dt[!is.na(date)]
                          dt[, code := cd]
                          all[[cd]] <- dt
                        }
                        long <- data.table::rbindlist(all, fill = TRUE)
                        if (!nrow(long)) return(NULL)
                        # check duplicates explicitly
                        dup <- long[, .N, by = .(code, date)][N > 1]
                        if (nrow(dup) > 0) {
                          log_msg("WARN: duplicates in panel (code-date):", nrow(dup))
                        }
                        
                        wide <- data.table::dcast(
                          long,
                          date ~ code,
                          value.var = "value",
                          fun.aggregate = mean,
                          na.rm = TRUE
                        )
                        
                        safe_fwrite(wide, file.path(ART_PANELS_DIR, panel_name))
                        TRUE
                      }
                      
                      # classify by meta_freq text
                      freq_map <- ok_profiles[, .(code, meta_freq)]
                      freq_map[, freq_norm := norm_ascii(meta_freq)]
                      
                      daily_codes    <- freq_map[grepl("dia|daily", freq_norm)]$code
                      monthly_codes  <- freq_map[grepl("mes|month", freq_norm)]$code
                      quarterly_codes<- freq_map[grepl("tri|quart", freq_norm)]$code
                      
                      build_wide(daily_codes,     "ipea_panel_daily_wide.csv")
                      build_wide(monthly_codes,   "ipea_panel_monthly_wide.csv")
                      build_wide(quarterly_codes, "ipea_panel_quarterly_wide.csv")
                    }
                    
                    # Write JSON summary
                    summary <- list(
                      run = list(run_id = RUN_ID, run_dir = RUN_DIR, started_at = ts_now()),
                      cache = list(cache_dir = CACHE_DIR),
                      outputs = list(
                        candidates = file.path(RUN_DIR, "ipea_candidates_catalog_join.csv"),
                        targets = file.path(RUN_DIR, "ipea_targets.csv"),
                        profile = file.path(RUN_DIR, "ipea_candidates_profile.csv"),
                        search_remote_all = file.path(RUN_DIR, "ipea_search_remote__ALL.csv"),
                        search_local_all  = file.path(RUN_DIR, "ipea_search_local__ALL.csv"),
                        panels_dir = ART_PANELS_DIR,
                        plots_dir  = ART_PLOT_DIR
                      )
                    )
                    jsonlite::write_json(summary, file.path(RUN_DIR, "summary_ipea_discovery.json"),
                                         auto_unbox = TRUE, pretty = TRUE)
                    
                    log_msg("DONE.")
                    invisible(list(catalog = catalog, candidates = candidates, targets = targets, profiles = profiles))
                    }

# -------------------------------
# 8) Execute
# -------------------------------
results <- run_ipea_discovery(
  language = "br",
  max_hits_per_topic = 800L,
  sample_per_topic = 8L,
  exclude_annual = TRUE,
  prefer_active_only = TRUE,
  make_plots = TRUE
)

# -------------------------------------------------------
# Export to GlobalEnv for direct inspection
# -------------------------------------------------------
assign("IPEA_RUN_ID",  RUN_ID,  envir = .GlobalEnv)
assign("IPEA_RUN_DIR", RUN_DIR, envir = .GlobalEnv)

assign("IPEA_catalog",    results$catalog,    envir = .GlobalEnv)
assign("IPEA_candidates", results$candidates, envir = .GlobalEnv)
assign("IPEA_targets",    results$targets,    envir = .GlobalEnv)
assign("IPEA_profiles",   results$profiles,   envir = .GlobalEnv)

assign("IPEA_ok",  results$profiles[ok == TRUE],  envir = .GlobalEnv)
assign("IPEA_bad", results$profiles[ok == FALSE], envir = .GlobalEnv)

log_msg("Exported objects: IPEA_catalog, IPEA_candidates, IPEA_targets, IPEA_profiles, IPEA_ok, IPEA_bad")


# Quick console report (non-toy)
if (!is.null(results) && nrow(results$profiles)) {
  cat("\n==================== IPEA SUITABILITY TOP 30 ====================\n")
  print(results$profiles[1:min(.N, 30),
                         .(score, topic, code, meta_freq, series_type, days_stale, gap_max_days, gap_severity, na_rate, territory_mode,
                           drift_ann = round(drift_ann, 6), vol_ann = round(vol_ann, 6), acf1 = round(acf1, 3), outlier_rate = round(outlier_rate, 4),
                           meta_name)][order(-score, topic, code)
                           ])
  cat("\nOpen:", file.path("playground/_runs", RUN_ID, "ipea_candidates_profile.csv"), "\n")
}
