# ============================================================
# playground/ipea_catalog_deep_dive.R
#
# IPEA Catalog Deep Dive (IPEADATAR)
# Goal: make the IPEA universe "bright":
#   - load catalog from cache if possible (searches under playground/)
#   - summarize catalog structure (freq/name tokens/etc.)
#   - probe time-series availability/shape via stratified sampling
#   - reuse per-code cache from previous runs when found
#
# Outputs: playground/_runs/<RUN_ID>/...
# ============================================================

options(stringsAsFactors = FALSE)

# -------------------------------
# 0) Helpers
# -------------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

stop_if_missing <- function(pkgs) {
  miss <- pkgs[!vapply(pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) stop("Missing packages: ", paste(miss, collapse = ", "), call. = FALSE)
}

stop_if_missing(c("data.table", "jsonlite"))

ts_now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
log_msg <- function(...) cat(sprintf("[%s] %s\n", ts_now(), paste(..., collapse = " ")))

PROJECT_DIR <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
PLAYGROUND_DIR <- file.path(PROJECT_DIR, "playground")
if (!dir.exists(PLAYGROUND_DIR)) stop("Expected /playground under project root. getwd() must be project root.", call. = FALSE)

RUN_ID  <- format(Sys.time(), "%Y%m%d_%H%M%S")
RUN_DIR <- file.path(PLAYGROUND_DIR, "_runs", RUN_ID)
CACHE_DIR <- file.path(RUN_DIR, "cache")
CODE_CACHE_DIR <- file.path(CACHE_DIR, "ipea_series")

dir.create(RUN_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(CODE_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

#Build a cache index ONCE per run (massive speedup)
GLOBAL_CODE_CACHE_DIR <- file.path(PLAYGROUND_DIR, "cache", "ipea_series")
dir.create(GLOBAL_CODE_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

build_code_cache_index <- function(max_runs = 40L) {
  idx <- new.env(parent = emptyenv())
  
  # search order: this run -> global -> newest prior runs
  dirs <- c(CODE_CACHE_DIR, GLOBAL_CODE_CACHE_DIR)
  
  runs_root <- file.path(PLAYGROUND_DIR, "_runs")
  if (dir.exists(runs_root)) {
    run_dirs <- list.dirs(runs_root, recursive = FALSE, full.names = TRUE)
    if (length(run_dirs)) {
      info <- file.info(run_dirs)
      run_dirs <- run_dirs[order(info$mtime, decreasing = TRUE)]
      run_dirs <- head(run_dirs, max_runs)
      dirs <- c(dirs, file.path(run_dirs, "cache", "ipea_series"))
    }
  }
  
  dirs <- dirs[dir.exists(dirs)]
  
  for (d in dirs) {
    fs <- list.files(d, pattern = "\\.rds$", full.names = TRUE)
    if (!length(fs)) next
    for (f in fs) {
      code <- sub("\\.rds$", "", basename(f))
      if (!exists(code, envir = idx, inherits = FALSE)) {
        assign(code, f, envir = idx)
      }
    }
  }
  
  idx
}

CODE_CACHE_INDEX <- build_code_cache_index(max_runs = 40L)

find_code_cache <- function(code) {
  code <- as.character(code)
  if (exists(code, envir = CODE_CACHE_INDEX, inherits = FALSE)) {
    return(get(code, envir = CODE_CACHE_INDEX, inherits = FALSE))
  }
  NULL
}


log_msg("RUN_DIR:", RUN_DIR)

dt_write <- function(dt, path) {
  data.table::fwrite(dt, path)
  log_msg("Wrote:", path, "rows=", nrow(dt), "cols=", ncol(dt))
  invisible(path)
}

# pick a column name from a set of candidates (case-insensitive)
pick_col <- function(nms, candidates) {
  nms_l <- tolower(nms)
  cand_l <- tolower(candidates)
  hit <- match(cand_l, nms_l)
  hit <- hit[!is.na(hit)]
  if (length(hit) == 0) return(NULL)
  nms[hit[1]]
}

# normalize text for tokenization/search (accent-stripping)
norm_txt <- function(x) {
  x <- tolower(as.character(x))
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  x
}

# -------------------------------
# 1) Deterministic catalog loader (NO lucky shots)
# -------------------------------
# Canonical cache location:
#   playground/cache/ipea_catalog_full.csv
#
# Behavior:
#   1) If IPEA_CATALOG_PATH is set -> load exactly that file.
#   2) Else if canonical cache exists (and is valid) -> use it.
#   3) Else try to copy the newest prior run's ipea_catalog_full.csv into canonical and use it.
#   4) Else fetch via ipeadatar::available_series() and write canonical.

IPEA_CATALOG_REFRESH <- get0("IPEA_CATALOG_REFRESH", ifnotfound = FALSE)
IPEA_CATALOG_PATH    <- get0("IPEA_CATALOG_PATH",    ifnotfound = NULL)

CANONICAL_CACHE_DIR  <- file.path(PLAYGROUND_DIR, "cache")
CANONICAL_CATALOG    <- file.path(CANONICAL_CACHE_DIR, "ipea_catalog_full.csv")
dir.create(CANONICAL_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

validate_ipea_catalog <- function(dt, require_full = TRUE) {
  dt <- data.table::as.data.table(dt)
  
  # Required columns for "catalog"
  if (!all(c("code", "name") %in% names(dt))) {
    stop("Catalog validation failed: missing required columns {code, name}. Got: ",
         paste(names(dt), collapse = ", "), call. = FALSE)
  }
  
  # Basic hygiene
  dt <- dt[!is.na(code) & nzchar(code)]
  dt[, code := as.character(code)]
  dt[, name := as.character(name)]
  
  # "Full catalog" sanity (your target is ~2804)
  if (require_full && nrow(dt) < 2000) {
    stop("Catalog validation failed: expected FULL catalog (>=2000 rows). Loaded ",
         nrow(dt), " rows. This is probably a filtered artifact (e.g., candidates).",
         call. = FALSE)
  }
  
  # Uniqueness sanity
  u <- dt[, data.table::uniqueN(code)]
  if (require_full && u < 2000) {
    stop("Catalog validation failed: too few unique codes (", u, ").", call. = FALSE)
  }
  
  dt
}

read_catalog_file <- function(path, require_full = TRUE) {
  if (!file.exists(path)) stop("Catalog file not found: ", path, call. = FALSE)
  ext <- tolower(tools::file_ext(path))
  dt <- if (ext == "rds") readRDS(path) else data.table::fread(path, showProgress = FALSE)
  dt <- validate_ipea_catalog(dt, require_full = require_full)
  dt
}

find_prior_run_full_catalog <- function() {
  # Only look for the exact file name "ipea_catalog_full.csv" under playground/_runs
  runs_root <- file.path(PLAYGROUND_DIR, "_runs")
  if (!dir.exists(runs_root)) return(NULL)
  
  cand <- list.files(runs_root,
                     pattern = "ipea_catalog_full\\.csv$",
                     recursive = TRUE,
                     full.names = TRUE,
                     ignore.case = TRUE)
  if (!length(cand)) return(NULL)
  
  info <- file.info(cand)
  o <- order(info$mtime, decreasing = TRUE)
  cand <- cand[o]
  
  # Pick the newest that actually validates as full catalog
  for (p in cand) {
    ok <- tryCatch({
      invisible(read_catalog_file(p, require_full = TRUE))
      TRUE
    }, error = function(e) FALSE)
    if (ok) return(p)
  }
  
  NULL
}

load_ipea_catalog <- function() {
  # 1) explicit path override (no guessing)
  if (!is.null(IPEA_CATALOG_PATH) && nzchar(IPEA_CATALOG_PATH)) {
    log_msg("Using explicit catalog path:", IPEA_CATALOG_PATH)
    dt <- read_catalog_file(IPEA_CATALOG_PATH, require_full = TRUE)
    return(list(dt = dt, source = IPEA_CATALOG_PATH, from_cache = TRUE))
  }
  
  # 2) canonical cache
  if (!IPEA_CATALOG_REFRESH && file.exists(CANONICAL_CATALOG)) {
    log_msg("Using canonical cached catalog:", CANONICAL_CATALOG)
    dt <- read_catalog_file(CANONICAL_CATALOG, require_full = TRUE)
    return(list(dt = dt, source = CANONICAL_CATALOG, from_cache = TRUE))
  }
  
  # 3) bootstrap canonical cache from prior runs
  if (!IPEA_CATALOG_REFRESH) {
    prior <- find_prior_run_full_catalog()
    if (!is.null(prior)) {
      log_msg("Bootstrapping canonical catalog from prior run:", prior)
      file.copy(prior, CANONICAL_CATALOG, overwrite = TRUE)
      dt <- read_catalog_file(CANONICAL_CATALOG, require_full = TRUE)
      return(list(dt = dt, source = CANONICAL_CATALOG, from_cache = TRUE))
    }
  }
  
  # 4) fetch fresh (deterministic)
  stop_if_missing("ipeadatar")
  log_msg("Fetching FULL catalog via ipeadatar::available_series()")
  dt <- ipeadatar::available_series(language = c("en","br"))
  dt <- validate_ipea_catalog(dt, require_full = TRUE)
  
  data.table::fwrite(dt, CANONICAL_CATALOG)
  log_msg("Wrote canonical cached catalog:", CANONICAL_CATALOG)
  
  list(dt = dt, source = "ipeadatar::available_series()", from_cache = FALSE)
}

# ---- Load catalog (deterministic)
cat_res <- load_ipea_catalog()
IPEA_catalog <- cat_res$dt
log_msg("Catalog loaded:", "n=", nrow(IPEA_catalog), "cols=", ncol(IPEA_catalog), "source=", cat_res$source)


# -------------------------------
# 2) Standardize catalog (STRICT: full ipeadatar available_series format)
# -------------------------------
req_cols <- c("code","name","theme","source","freq","lastupdate","status")
miss <- setdiff(req_cols, names(IPEA_catalog))
if (length(miss)) {
  stop("Full catalog is missing required columns: ",
       paste(miss, collapse = ", "),
       "\nGot: ", paste(names(IPEA_catalog), collapse = ", "),
       call. = FALSE)
}

cat_dt <- data.table::as.data.table(IPEA_catalog)

# deterministic column names (no guessing)
col_code <- "code"
col_name <- "name"
col_freq <- "freq"
col_act  <- "status"

cat_view <- cat_dt[, .(
  code      = as.character(code),
  name      = as.character(name),
  theme     = as.character(theme),
  source    = as.character(source),
  meta_freq = as.character(freq),
  lastupdate = as.character(lastupdate),
  active    = as.character(status)
)]

dt_write(cat_view, file.path(RUN_DIR, "catalog_standard_view.csv"))

# -------------------------------
# 3) Catalog-wide summaries
# -------------------------------
# Frequency distribution
if (!is.null(col_freq)) {
  freq_tab <- cat_view[, .N, by = .(meta_freq)][order(-N)]
  dt_write(freq_tab, file.path(RUN_DIR, "catalog_freq_counts.csv"))
}

# Duplicate names (useful to see repeated labels)
dup_names <- cat_view[!is.na(name) & nzchar(name), .N, by = .(name)][N > 1][order(-N)]
dt_write(dup_names, file.path(RUN_DIR, "catalog_duplicate_names.csv"))

# Token stats (rough)
stopwords <- c("de","da","do","das","dos","a","o","e","em","para","por","com",
               "no","na","nos","nas","um","uma","uns","umas","taxa","indice","índice")
nm_norm <- norm_txt(cat_view$name)
tokens <- unlist(strsplit(gsub("[^a-z0-9]+", " ", nm_norm), "\\s+"))
tokens <- tokens[nzchar(tokens)]
tokens <- tokens[!(tokens %in% stopwords)]
tok_tab <- data.table::as.data.table(table(tokens))
data.table::setnames(tok_tab, c("token","N"))
tok_tab <- tok_tab[order(-N)]
dt_write(tok_tab[1:min(.N, 500)], file.path(RUN_DIR, "catalog_top_tokens_500.csv"))

# -------------------------------
# 4) Series fetcher with cache reuse
# -------------------------------
# We implement our own per-code caching.
# It will reuse any prior cached file matching ipea_series/<CODE>.rds under playground/_runs/*/cache or playground/cache.

find_code_cache <- function(code) {
  code <- as.character(code)
  fname <- paste0(code, ".rds")
  
  # 1) this run
  p1 <- file.path(CODE_CACHE_DIR, fname)
  if (file.exists(p1)) return(p1)
  
  # 2) global playground cache if you have one
  p2 <- file.path(PLAYGROUND_DIR, "cache", "ipea_series", fname)
  if (file.exists(p2)) return(p2)
  
  # 3) search recent runs (fast-ish; only checks cache folders)
  runs_root <- file.path(PLAYGROUND_DIR, "_runs")
  if (!dir.exists(runs_root)) return(NULL)
  
  run_dirs <- list.dirs(runs_root, recursive = FALSE, full.names = TRUE)
  if (!length(run_dirs)) return(NULL)
  
  # sort by mtime desc
  info <- file.info(run_dirs)
  run_dirs <- run_dirs[order(info$mtime, decreasing = TRUE)]
  
  # search top K runs only to avoid crawling forever
  K <- min(length(run_dirs), 40L)
  for (i in seq_len(K)) {
    p <- file.path(run_dirs[i], "cache", "ipea_series", fname)
    if (file.exists(p)) return(p)
  }
  
  NULL
}

# Robust ipeadatar fetch (tries ipeadata first, then others)
ipea_pick_exported_fn <- function(candidates) {
  ns <- "ipeadatar"
  exp <- tryCatch(getNamespaceExports(ns), error = function(e) character())
  for (nm in candidates) if (nm %in% exp) return(getExportedValue(ns, nm))
  NULL
}

ipea_fetch_raw <- function(code) {
  stop_if_missing("ipeadatar")
  f <- ipea_pick_exported_fn(c("ipeadata", "get_series", "series", "fetch_series", "ipea_series"))
  if (is.null(f)) {
    ns <- asNamespace("ipeadatar")
    if (exists("ipeadata", envir = ns, inherits = FALSE)) f <- get("ipeadata", envir = ns)
  }
  if (is.null(f) || !is.function(f)) stop("Could not locate ipeadatar fetch function.", call. = FALSE)
  f(code)
}

normalize_series_dt <- function(x) {
  dt <- data.table::as.data.table(x)
  nms <- names(dt)
  
  # find date column
  date_col <- pick_col(nms, c("date","data","dt","refdate"))
  if (!is.null(date_col)) {
    dt[, date := as.Date(get(date_col))]
  } else {
    # try ANO/MES
    ano_col <- pick_col(nms, c("ano","ANO"))
    mes_col <- pick_col(nms, c("mes","MES"))
    if (!is.null(ano_col) && !is.null(mes_col)) {
      dt[, date := as.Date(sprintf("%04d-%02d-01",
                                   as.integer(get(ano_col)),
                                   as.integer(get(mes_col))))]
    } else {
      stop("Could not infer date column from series payload.", call. = FALSE)
    }
  }
  
  # find value column
  val_col <- pick_col(nms, c("value","valor","val","y"))
  if (is.null(val_col)) {
    candidates <- setdiff(nms, c(date_col, "date", "data", "dt", "refdate"))
    if (!length(candidates)) stop("No candidates for value column.", call. = FALSE)
    val_col <- candidates[1]
  }
  dt[, value := suppressWarnings(as.numeric(as.character(get(val_col))))]
  
  # detect territorial columns (these are expected in IPEA API outputs)
  uname_col <- pick_col(nms, c("uname","unidade","territorial_unit","territory_name"))
  tcode_col <- pick_col(nms, c("tcode","cod_territorio","territory_code","codigo_territorio"))
  
  n_tcode <- if (!is.null(tcode_col)) data.table::uniqueN(dt[[tcode_col]]) else NA_integer_
  n_uname <- if (!is.null(uname_col)) data.table::uniqueN(dt[[uname_col]]) else NA_integer_
  
  # Keep extras list for visibility, but the key dimensionality is n_tcode / n_uname
  extras <- setdiff(names(dt), c("date","value", date_col, val_col))
  
  list(
    core = dt[, .(date, value)],
    extras = extras,
    n_tcode = n_tcode,
    n_uname = n_uname,
    full_names = names(dt)
  )
}

read_series_cached <- function(code) {
  code <- as.character(code)
  cached <- find_code_cache(code)
  if (!is.null(cached)) {
    obj <- readRDS(cached)
    return(list(
      code = code,
      dt = obj$dt,
      extras = obj$extras,
      n_tcode = obj$n_tcode %||% NA_integer_,
      n_uname = obj$n_uname %||% NA_integer_,
      from_cache = TRUE,
      cache_path = cached
    ))
  }
  
  x <- ipea_fetch_raw(code)
  norm <- normalize_series_dt(x)
  
  obj <- list(
    code = code,
    dt = norm$core,
    extras = norm$extras,
    n_tcode = norm$n_tcode,
    n_uname = norm$n_uname,
    fetched_at = ts_now()
  )
  
  dest <- file.path(GLOBAL_CODE_CACHE_DIR, paste0(code, ".rds"))
  saveRDS(obj, dest)
  assign(code, dest, envir = CODE_CACHE_INDEX)
  
  return(list(
    code = code,
    dt = obj$dt,
    extras = obj$extras,
    n_tcode = obj$n_tcode,
    n_uname = obj$n_uname,
    from_cache = FALSE,
    cache_path = dest
  ))
}

profile_series <- function(code, dt, extras, n_tcode = NA_integer_, n_uname = NA_integer_) {
  dt <- data.table::as.data.table(dt)
  dt <- dt[!is.na(date)]
  data.table::setorder(dt, date)
  
  n_rows <- nrow(dt)
  n_dates <- if (n_rows) data.table::uniqueN(dt$date) else 0L
  
  min_date <- if (n_rows) as.character(dt$date[1]) else NA_character_
  max_date <- if (n_rows) as.character(dt$date[n_rows]) else NA_character_
  
  # duplicates by date in the (date,value) collapsed table
  dup_n <- if (n_rows) (n_rows - n_dates) else 0L
  
  # gaps computed on UNIQUE dates (not raw rows)
  max_gap <- NA_real_
  med_gap <- NA_real_
  if (n_dates >= 2) {
    udates <- sort(unique(dt$date))
    d <- as.numeric(diff(udates))
    max_gap <- max(d, na.rm = TRUE)
    med_gap <- stats::median(d, na.rm = TRUE)
  }
  
  na_rate <- if (n_rows) mean(is.na(dt$value)) else NA_real_
  
  finite_vals <- dt$value[is.finite(dt$value)]
  min_val <- if (length(finite_vals)) min(finite_vals) else NA_real_
  max_val <- if (length(finite_vals)) max(finite_vals) else NA_real_
  p_le0  <- if (length(finite_vals)) mean(finite_vals <= 0) else NA_real_
  
  rows_per_date <- if (n_dates > 0) (n_rows / n_dates) else NA_real_
  
  data.table::data.table(
    code = as.character(code),
    n_rows = as.integer(n_rows),
    n_dates = as.integer(n_dates),
    rows_per_date = as.numeric(rows_per_date),
    n_tcode = as.integer(n_tcode),
    n_uname = as.integer(n_uname),
    min_date = min_date,
    max_date = max_date,
    med_gap_days = as.numeric(med_gap),
    max_gap_days = as.numeric(max_gap),
    dup_date_rows = as.integer(dup_n),
    na_rate = as.numeric(na_rate),
    min_value = as.numeric(min_val),
    max_value = as.numeric(max_val),
    p_le0 = as.numeric(p_le0),
    extra_cols = paste(extras, collapse = "|"),
    error = NA_character_
  )
}

# -------------------------------
# 5) Stratified probe across catalog
# -------------------------------
# Config: you can override by setting these variables before sourcing the script.
PROBE_N_PER_GROUP <- get0("IPEA_PROBE_N_PER_GROUP", ifnotfound = 40L)
PROBE_SEED <- get0("IPEA_PROBE_SEED", ifnotfound = 123L)

set.seed(PROBE_SEED)

# define grouping for probe
if (!is.null(col_freq)) {
  grp <- cat_view[, .(code, meta_freq)]
} else {
  grp <- cat_view[, .(code)]
  grp[, meta_freq := "UNKNOWN"]
}

grp <- grp[!is.na(code) & nzchar(code)]
freq_levels <- unique(grp$meta_freq)

probe_codes <- grp[, .SD[sample.int(.N, size = min(.N, PROBE_N_PER_GROUP))], by = meta_freq]$code
probe_codes <- unique(probe_codes)
log_msg("Probe sample size:", length(probe_codes), "across groups:", length(unique(grp$meta_freq)))

# Fetch+profile probe sample
profiles_list <- vector("list", length(probe_codes))
for (i in seq_along(probe_codes)) {
  code <- probe_codes[i]
  if (i %% 25 == 0) log_msg("Probe progress:", i, "/", length(probe_codes))
  res <- tryCatch({
    s <- read_series_cached(code)
    profile_series(code, s$dt, s$extras, n_tcode = s$n_tcode, n_uname = s$n_uname)
  }, error = function(e) {
    data.table::data.table(
      code = as.character(code),
      n_rows = NA_integer_,
      n_dates = NA_integer_,
      rows_per_date = NA_real_,
      n_tcode = NA_integer_,
      n_uname = NA_integer_,
      min_date = NA_character_,
      max_date = NA_character_,
      med_gap_days = NA_real_,
      max_gap_days = NA_real_,
      dup_date_rows = NA_integer_,
      na_rate = NA_real_,
      min_value = NA_real_,
      max_value = NA_real_,
      p_le0 = NA_real_,
      extra_cols = NA_character_,
      error = conditionMessage(e)
    )})
  profiles_list[[i]] <- res
}
# ---- probe_profile is ONLY the probed codes
probe_profile <- data.table::rbindlist(profiles_list, fill = TRUE)

# attach metadata (LEFT join onto probe rows)
probe_profile <- cat_view[probe_profile, on = .(code)]
data.table::setcolorder(probe_profile, c("code","meta_freq","name", setdiff(names(probe_profile), c("code","meta_freq","name"))))

dt_write(probe_profile, file.path(RUN_DIR, "probe_profile_sample.csv"))

# summarize probe results (availability distribution)
probe_summary <- probe_profile[, .(
  n_series = .N,
  n_ok = sum(!is.na(n_dates) & n_dates > 0),
  median_n_dates = as.integer(stats::median(n_dates, na.rm = TRUE)),
  median_rows_per_date = as.numeric(stats::median(rows_per_date, na.rm = TRUE)),
  median_max_gap = as.numeric(stats::median(max_gap_days, na.rm = TRUE)),
  frac_panel_like = mean(rows_per_date > 1.01, na.rm = TRUE),
  frac_dup_dates = mean(dup_date_rows > 0, na.rm = TRUE),
  frac_nonpos = mean(p_le0 > 0, na.rm = TRUE)
), by = meta_freq][order(-n_series)]

dt_write(probe_summary, file.path(RUN_DIR, "probe_summary_by_freq.csv"))

# -------------------------------
# 6) Write a run summary.json
# -------------------------------
summary_list <- list(
  run = list(
    run_id = RUN_ID,
    run_dir = RUN_DIR,
    started_at = ts_now(),
    catalog_source = cat_res$source,
    catalog_n = nrow(IPEA_catalog),
    catalog_cols = names(IPEA_catalog),
    probe_n_per_group = PROBE_N_PER_GROUP,
    probe_total = length(probe_codes),
    seed = PROBE_SEED
  ),
  outputs = list(
    catalog_head = "ipea_catalog_head300.csv",
    catalog_standard = "catalog_standard_view.csv",
    freq_counts = if (!is.null(col_freq)) "catalog_freq_counts.csv" else NA,
    top_tokens = "catalog_top_tokens_500.csv",
    probe_profile = "probe_profile_sample.csv",
    probe_summary = "probe_summary_by_freq.csv"
  )
)

json_path <- file.path(RUN_DIR, "summary.json")
jsonlite::write_json(summary_list, json_path, auto_unbox = TRUE, pretty = TRUE)
log_msg("Wrote:", json_path)

log_msg("DONE.")

# -------------------------------
# 7) Console report + deep diagnostics (human-readable)
# -------------------------------
PRINT_N <- get0("IPEA_PRINT_N", ifnotfound = 25L)

cat_sep <- function(title = NULL) {
  cat("\n")
  cat(paste(rep("=", 78), collapse = ""), "\n")
  if (!is.null(title)) cat(title, "\n")
  cat(paste(rep("=", 78), collapse = ""), "\n")
}

safe_read_csv <- function(path) {
  if (!file.exists(path)) return(NULL)
  data.table::fread(path, showProgress = FALSE)
}

# Build a single report that is printed AND saved
report_lines <- character()
push <- function(...) {
  txt <- paste0(..., collapse = "")
  report_lines <<- c(report_lines, txt)
  cat(txt)
}

# Helper: print a small table in a stable way
print_dt <- function(dt, n = PRINT_N) {
  if (is.null(dt)) { push("<<NULL>>\n"); return(invisible(NULL)) }
  if (!data.table::is.data.table(dt)) dt <- data.table::as.data.table(dt)
  n <- min(n, nrow(dt))
  if (n == 0) { push("<<EMPTY>>\n"); return(invisible(NULL)) }
  out <- capture.output(print(dt[1:n]))
  push(paste0(out, collapse = "\n"), "\n")
  invisible(NULL)
}

# ---- Paths & outputs
cat_sep("IPEA CATALOG DEEP DIVE — CONSOLE REPORT")
push("RUN_ID: ", RUN_ID, "\n")
push("RUN_DIR: ", RUN_DIR, "\n")
push("Catalog source: ", cat_res$source, "\n")
push("Catalog rows/cols: ", nrow(IPEA_catalog), " / ", ncol(IPEA_catalog), "\n\n")

# List key output files in this run
cat_sep("Output files in RUN_DIR")
run_files <- list.files(RUN_DIR, full.names = FALSE)
print_dt(data.table::data.table(file = sort(run_files)))

# ---- Catalog column detection
cat_sep("Catalog column detection")
push("Detected code column: ", col_code %||% "<NOT FOUND>", "\n")
push("Detected name column: ", col_name %||% "<NOT FOUND>", "\n")
push("Detected freq column: ", col_freq %||% "<NOT FOUND>", "\n")
push("Detected active/status column: ", col_act %||% "<NOT FOUND>", "\n\n")
push("Raw catalog columns:\n")
push(paste0(" - ", names(IPEA_catalog), collapse = "\n"), "\n")

# ---- Standard view quick sanity
cat_sep("Standard catalog view (head)")
print_dt(cat_view)

# ---- Frequency distribution (if present)
cat_sep("Frequency distribution (catalog)")
freq_counts_path <- file.path(RUN_DIR, "catalog_freq_counts.csv")
freq_tab2 <- safe_read_csv(freq_counts_path)
if (!is.null(freq_tab2)) {
  print_dt(freq_tab2, n = 50)
} else {
  push("No frequency column was detected -> catalog_freq_counts.csv not created.\n")
}

# ---- Top tokens
cat_sep("Top name tokens (catalog)")
tok_path <- file.path(RUN_DIR, "catalog_top_tokens_500.csv")
tok2 <- safe_read_csv(tok_path)
print_dt(tok2, n = 50)

# ---- Duplicate names
cat_sep("Duplicate names (catalog) — top")
dup_names_path <- file.path(RUN_DIR, "catalog_duplicate_names.csv")
dup2 <- safe_read_csv(dup_names_path)
if (!is.null(dup2)) {
  print_dt(dup2, n = 50)
} else {
  push("No duplicate name table found.\n")
}

# ---- Probe diagnostics
cat_sep("Probe summary by frequency")
probe_sum_path <- file.path(RUN_DIR, "probe_summary_by_freq.csv")
probe_sum2 <- safe_read_csv(probe_sum_path)
print_dt(probe_sum2, n = 100)

cat_sep("Probe profile sample (head)")
print_dt(probe_profile)

# ---- Deep diagnostics on the PROBE sample (actionable flags)
cat_sep("Probe deep diagnostics — what looks problematic and why")

pp <- data.table::copy(probe_profile)
# Some probe rows may have errors; keep them visible
if (!"error" %in% names(pp)) pp[, error := NA_character_]
n_err <- pp[!is.na(error) & nzchar(error), .N]
push("Probe rows with fetch/parse error: ", n_err, " / ", nrow(pp), "\n")

if (n_err > 0) {
  push("\nTop errors (count):\n")
  err_tab <- pp[!is.na(error) & nzchar(error), .N, by = error][order(-N)]
  print_dt(err_tab, n = 15)
}

# Worst availability (fewest observations, excluding errors)
push("\nWorst availability (lowest n_dates, excluding errors):\n")
worst_n <- pp[is.na(error) | !nzchar(error), ][!is.na(n_dates)][order(n_dates)][1:min(.N, PRINT_N)]

print_dt(worst_n[, .(
  code, meta_freq,
  n_dates, n_rows, rows_per_date,
  min_date, max_date,
  med_gap_days, max_gap_days,
  dup_date_rows, na_rate,
  n_tcode, n_uname,
  extra_cols
)])


# Biggest gaps
push("\nLargest native gaps (max_gap_days):\n")
worst_gap <- pp[is.na(error) | !nzchar(error), ][!is.na(max_gap_days)][order(-max_gap_days)][1:min(.N, PRINT_N)]
print_dt(worst_gap[, .(
  code, meta_freq,
  n_dates, n_rows, rows_per_date,
  min_date, max_date,
  med_gap_days, max_gap_days,
  dup_date_rows, na_rate,
  n_tcode, n_uname,
  extra_cols
)])

# Duplicate dates within the raw series (this is a real red flag)
push("\nSeries with duplicate dates inside the raw series (dup_date_rows > 0):\n")
dup_in_series <- pp[dup_date_rows > 0][order(-dup_date_rows)][1:min(.N, PRINT_N)]
if (nrow(dup_in_series)) {
  print_dt(dup_in_series[, .(
    code, meta_freq,
    dup_date_rows,
    n_dates, n_rows, rows_per_date,
    min_date, max_date,
    n_tcode, n_uname,
    extra_cols, name
  )])
} else {
  push("None found in the probe sample.\n")
}

# Extra columns present in the fetched payload (possible multidimensional payloads)
push("\nSeries whose payload had extra columns (extra_cols not empty):\n")
has_extras <- pp[!is.na(extra_cols) & nzchar(extra_cols)][order(-n_rows)][1:min(.N, PRINT_N)]
if (nrow(has_extras)) {
  print_dt(has_extras[, .(
    code, meta_freq,
    n_dates, n_rows, rows_per_date,
    min_date, max_date,
    n_tcode, n_uname,
    extra_cols, name
  )])
} else {
  push("None found in the probe sample.\n")
}

# Non-positive share (p_le0): helps decide if log transforms are even valid
push("\nSeries with many non-positive values (p_le0 >= 0.05):\n")
nonpos <- pp[!is.na(p_le0) & p_le0 >= 0.05][order(-p_le0)][1:min(.N, PRINT_N)]
if (nrow(nonpos)) {
  print_dt(nonpos[, .(
    code, meta_freq,
    p_le0, min_value, max_value,
    n_dates, n_rows, rows_per_date,
    name
  )])
} else {
  push("None found in the probe sample above threshold.\n")
}

# NA-heavy series
push("\nNA-heavy series (na_rate >= 0.20):\n")
naheavy <- pp[!is.na(na_rate) & na_rate >= 0.20][order(-na_rate)][1:min(.N, PRINT_N)]
if (nrow(naheavy)) {
  print_dt(naheavy[, .(
    code, meta_freq,
    na_rate,
    n_dates, n_rows, rows_per_date,
    min_date, max_date,
    name
  )])
} else {
  push("None found in the probe sample above threshold.\n")
}

# ---- Suggest immediate “catalog exploration” queries (NOT GenAI; just tokens)
cat_sep("Catalog exploration hints (deterministic)")
push("If you want to explore topics more thoroughly, the token table is your friend.\n")
push("Example approach: pick high-frequency tokens (excluding stopwords) and search catalog names.\n")
push("Top 20 tokens shown above in catalog_top_tokens_500.csv.\n")

# ---- Save report to file (so you can paste back later)
report_path <- file.path(RUN_DIR, "report.txt")
writeLines(report_lines, report_path, useBytes = TRUE)
log_msg("Wrote:", report_path)

# Optionally: also save an RDS snapshot of main objects for later inspection
saveRDS(list(
  IPEA_catalog = IPEA_catalog,
  cat_view = cat_view,
  probe_profile = probe_profile,
  probe_summary = probe_sum2 %||% NULL
), file.path(RUN_DIR, "deep_dive_objects.rds"))
log_msg("Wrote:", file.path(RUN_DIR, "deep_dive_objects.rds"))
