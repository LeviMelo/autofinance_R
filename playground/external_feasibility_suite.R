# playground/external_feasibility_suite.R
# External feasibility suite (BCB/SGS, IPEA, CVM, NEFIN)
# - No setwd()
# - All artifacts go to playground/_runs/<timestamp>/
# - Logs URLs + schemas + samples for feasibility assessment

options(stringsAsFactors = FALSE)

# -------------------------------
# 0) Small helpers
# -------------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

stop_if_missing <- function(pkgs) {
  miss <- pkgs[!vapply(pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) stop("Missing packages: ", paste(miss, collapse = ", "), call. = FALSE)
}

stop_if_missing(c("data.table", "jsonlite", "curl"))

ts_now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
log_msg <- function(...) {
  msg <- paste(..., collapse = " ")
  cat(sprintf("[%s] %s\n", ts_now(), msg))
}

# Paths: user guarantees getwd() == project root, and we keep ALL artifacts in playground/_runs/
PROJECT_DIR <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
PLAYGROUND_DIR <- file.path(PROJECT_DIR, "playground")

if (!dir.exists(PLAYGROUND_DIR)) {
  stop("Expected folder not found: ", PLAYGROUND_DIR,
       "\nYour getwd() must be the project root (autofinance_R) containing /playground.", call. = FALSE)
}

RUN_ID <- format(Sys.time(), "%Y%m%d_%H%M%S")
RUN_DIR <- file.path(PLAYGROUND_DIR, "_runs", RUN_ID)
CACHE_DIR <- file.path(RUN_DIR, "cache")

dir.create(RUN_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

log_msg("RUN_DIR:", RUN_DIR)

# Robust downloader (always local file)
download_cached <- function(url, dest, overwrite = FALSE) {
  if (file.exists(dest) && !overwrite) return(dest)
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  
  t0 <- proc.time()[3]
  h <- curl::new_handle()
  curl::handle_setheaders(h, "User-Agent" = "autofinance_R feasibility suite", "Accept" = "*/*")
  
  tmp <- paste0(dest, ".tmp")
  if (file.exists(tmp)) unlink(tmp)
  
  ok <- TRUE
  err <- NULL
  tryCatch({
    curl::curl_download(url, destfile = tmp, handle = h, quiet = TRUE)
    file.rename(tmp, dest)
  }, error = function(e) {
    ok <<- FALSE
    err <<- conditionMessage(e)
    if (file.exists(tmp)) unlink(tmp)
  })
  
  elapsed <- round(proc.time()[3] - t0, 2)
  if (!ok) stop(sprintf("Download failed: %s | URL: %s", err, url), call. = FALSE)
  
  mb <- round(file.info(dest)$size / 1024^2, 2)
  log_msg("Saved:", dest, "(", mb, "MB )", "elapsed_s=", elapsed)
  dest
}

# Read URL body as text (for HTML parsing)
read_url_text <- function(url) {
  h <- curl::new_handle()
  curl::handle_setheaders(h, "User-Agent" = "autofinance_R feasibility suite", "Accept" = "text/html,*/*")
  res <- curl::curl_fetch_memory(url, handle = h)
  rawToChar(res$content)
}

# JSON GET helper with headers
read_url_json <- function(url) {
  h <- curl::new_handle()
  curl::handle_setheaders(h, "User-Agent" = "autofinance_R feasibility suite", "Accept" = "application/json")
  res <- curl::curl_fetch_memory(url, handle = h)
  jsonlite::fromJSON(rawToChar(res$content))
}

# -------------------------------
# 1) BCB / SGS
# -------------------------------
bcb_sgs_url_ultimos <- function(id, n = 10L) {
  # IMPORTANT: BCB SGS /ultimos has a hard limit (<= 20). Larger returns HTTP 400.
  if (n > 20) stop("BCB /ultimos limit exceeded: n must be <= 20 (got n=", n, ")", call. = FALSE)
  sprintf("https://api.bcb.gov.br/dados/serie/bcdata.sgs.%d/dados/ultimos/%d?formato=json", as.integer(id), as.integer(n))
}

bcb_sgs_url_range <- function(id, start_date, end_date) {
  # start_date/end_date are Date
  di <- format(start_date, "%d/%m/%Y")
  df <- format(end_date, "%d/%m/%Y")
  sprintf("https://api.bcb.gov.br/dados/serie/bcdata.sgs.%d/dados?formato=json&dataInicial=%s&dataFinal=%s",
          as.integer(id), di, df)
}

bcb_parse_dt <- function(x) {
  # API returns list with fields: data (dd/mm/YYYY) and valor (string)
  dt <- data.table::as.data.table(x)
  if (!all(c("data", "valor") %in% names(dt))) {
    stop("Unexpected BCB JSON shape; expected fields {data, valor}. Got: ", paste(names(dt), collapse = ", "), call. = FALSE)
  }
  dt[, date := as.Date(data, format = "%d/%m/%Y")]
  dt[, value := suppressWarnings(as.numeric(gsub(",", ".", valor, fixed = FALSE)))]
  dt[, c("data", "valor") := NULL]
  data.table::setcolorder(dt, c("date", "value"))
  dt[order(date)]
}

bcb_fetch_ultimos <- function(id, n = 10L) {
  url <- bcb_sgs_url_ultimos(id, n)
  x <- read_url_json(url)
  list(url = url, data = bcb_parse_dt(x))
}

bcb_fetch_range <- function(id, start_date, end_date) {
  url <- bcb_sgs_url_range(id, start_date, end_date)
  x <- read_url_json(url)
  list(url = url, data = bcb_parse_dt(x))
}

test_bcb <- function() {
  log_msg("=== Test: BCB/SGS ===")
  
  ids <- list(
    selic_daily  = 11L,
    cdi_daily    = 12L,
    ipca_month   = 433L,
    usdbrl_ptax  = 10813L
  )
  
  out <- list(ok = TRUE, series = list())
  # Keep /ultimos small and valid
  n_last <- 10L
  # Range: ~10y (same scale you already validated)
  end_date <- Sys.Date()
  start_date <- end_date - 3650
  
  for (nm in names(ids)) {
    id <- ids[[nm]]
    t0 <- proc.time()[3]
    res <- tryCatch({
      # 1) small "ultimos" (endpoint shape sanity)
      u <- bcb_fetch_ultimos(id, n_last)
      # 2) bigger range (enough history for later macro alignment)
      r <- bcb_fetch_range(id, start_date, end_date)
      
      dt <- r$data
      last_obs <- if (nrow(dt)) as.character(dt[nrow(dt), date]) else NA_character_
      
      # save samples
      sample_file <- file.path(RUN_DIR, sprintf("bcb_%s_id%d_sample.csv", nm, id))
      data.table::fwrite(dt, sample_file)
      
      elapsed <- round(proc.time()[3] - t0, 2)
      log_msg("BCB OK:", nm, "(id=", id, ")",
              "n=", nrow(dt), "last_observed=", last_obs, "elapsed_s=", elapsed)
      
      list(
        ok = TRUE,
        id = id,
        ultimos_url = u$url,
        range_url = r$url,
        n = nrow(dt),
        last_observed = last_obs,
        sample_csv = sample_file,
        colnames = names(dt)
      )
    }, error = function(e) {
      out$ok <<- FALSE
      msg <- conditionMessage(e)
      log_msg("ERROR: BCB fetch", nm, "(id=", id, "):", msg)
      list(ok = FALSE, id = id, error = msg)
    })
    
    out$series[[nm]] <- res
  }
  
  out
}

# -------------------------------
# 2) IPEA (ipeadatar)
# -------------------------------
# We avoid relying on one specific exported function name:
# - discover exports
# - pick first matching candidate for list series and fetch series

ipea_pick_exported_fn <- function(candidates) {
  ns <- "ipeadatar"
  exp <- tryCatch(getNamespaceExports(ns), error = function(e) character())
  for (nm in candidates) {
    if (nm %in% exp) return(getExportedValue(ns, nm))
  }
  NULL
}

ipea_normalize <- function(x) {
  x <- tolower(as.character(x))
  # strip accents using iconv (base)
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  x
}

ipea_list_series <- function() {
  stop_if_missing(c("ipeadatar"))
  f <- ipea_pick_exported_fn(c(
    "available_series",
    "list_series",
    "ipea_available_series",
    "series_available",
    "dictionary",
    "ipeadata_dictionary"
  ))
  if (is.null(f)) {
    # last resort: try known object in namespace (not exported)
    ns <- asNamespace("ipeadatar")
    if (exists("available_series", envir = ns, inherits = FALSE)) f <- get("available_series", envir = ns)
  }
  if (is.null(f) || !is.function(f)) stop("Could not locate a 'list available series' function in ipeadatar.", call. = FALSE)
  f()
}

ipea_fetch_series <- function(code) {
  stop_if_missing(c("ipeadatar"))
  f <- ipea_pick_exported_fn(c(
    "ipeadata",
    "get_series",
    "series",
    "fetch_series",
    "ipea_series"
  ))
  if (is.null(f)) {
    ns <- asNamespace("ipeadatar")
    if (exists("ipeadata", envir = ns, inherits = FALSE)) f <- get("ipeadata", envir = ns)
  }
  if (is.null(f) || !is.function(f)) stop("Could not locate a 'fetch series' function in ipeadatar.", call. = FALSE)
  
  x <- f(code)
  dt <- data.table::as.data.table(x)
  
  # Normalize column names commonly seen: date/value OR data/valor OR year/month combos
  nms <- names(dt)
  if ("date" %in% nms) {
    dt[, date := as.Date(date)]
  } else if ("data" %in% nms) {
    # could be Date or string
    dt[, date := as.Date(data)]
  } else if (all(c("ANO", "MES") %in% toupper(nms))) {
    # fallback: build monthly date
    a <- dt[[nms[toupper(nms) == "ANO"][1]]]
    m <- dt[[nms[toupper(nms) == "MES"][1]]]
    dt[, date := as.Date(sprintf("%04d-%02d-01", as.integer(a), as.integer(m)))]
  }
  
  val_col <- NULL
  if ("value" %in% nms) val_col <- "value"
  if (is.null(val_col) && "valor" %in% nms) val_col <- "valor"
  if (is.null(val_col)) {
    # try first numeric-ish column not date
    candidates <- setdiff(nms, c("date", "data"))
    val_col <- candidates[1]
  }
  
  dt[, value := suppressWarnings(as.numeric(as.character(get(val_col))))]
  dt <- dt[, .(date, value)]
  dt[order(date)]
}

ipea_local_search <- function(available_dt, query, top_n = 15L) {
  q <- ipea_normalize(query)
  dt <- data.table::copy(data.table::as.data.table(available_dt))
  # Search across all columns (as text)
  for (j in names(dt)) dt[[j]] <- ipea_normalize(dt[[j]])
  hit <- dt[
    apply(dt, 1, function(row) any(grepl(q, row, fixed = FALSE))),
  ]
  hit[1:min(.N, top_n)]
}

test_ipea <- function() {
  log_msg("=== Test: IPEA (ipeadatar) ===")
  
  out <- list(ok = TRUE)
  
  res <- tryCatch({
    t0 <- proc.time()[3]
    available <- ipea_list_series()
    dt_av <- data.table::as.data.table(available)
    elapsed <- round(proc.time()[3] - t0, 2)
    
    log_msg("IPEA OK: available_series n=", nrow(dt_av), "cols=", ncol(dt_av), "elapsed_s=", elapsed)
    
    # Save a sample for inspection
    sample_av <- file.path(RUN_DIR, "ipea_available_series_sample.csv")
    data.table::fwrite(dt_av[1:min(nrow(dt_av), 200)], sample_av)
    log_msg("Wrote:", sample_av)
    
    # Local searches (accent-safe)
    queries <- c("IPCA", "selic", "juros", "cambio", "câmbio", "dolar", "dólar")
    hits <- list()
    for (q in queries) {
      h <- ipea_local_search(dt_av, q, top_n = 20L)
      hits[[q]] <- list(n = nrow(h), head = h)
      log_msg("IPEA local_search:", q, "hits=", nrow(h), "elapsed_s=0.00")
    }
    
    # Resolve a likely IPCA code:
    # Prefer a code that literally contains "ipca" after normalization.
    best_ipca <- NULL
    # Try common column names where codes live
    code_cols <- intersect(names(dt_av), c("code", "codigo", "CODIGO", "serie", "SERIE", "id", "ID"))
    if (length(code_cols) == 0) code_cols <- names(dt_av)
    
    # Find any row with "ipca" somewhere, then pick first non-empty token from any column
    dt_norm <- data.table::copy(dt_av)
    for (j in names(dt_norm)) dt_norm[[j]] <- ipea_normalize(dt_norm[[j]])
    ipca_rows <- dt_norm[
      apply(dt_norm, 1, function(row) any(grepl("ipca", row, fixed = FALSE))),
    ]
    if (nrow(ipca_rows) > 0) {
      r1 <- ipca_rows[1]
      # pick first value that looks like an alnum code
      vals <- unlist(r1, use.names = FALSE)
      vals <- vals[nzchar(vals)]
      best_ipca <- vals[1] %||% NULL
    }
    
    fetched <- NULL
    if (!is.null(best_ipca)) {
      t1 <- proc.time()[3]
      dt_ipca <- ipea_fetch_series(best_ipca)
      elapsed2 <- round(proc.time()[3] - t1, 2)
      last_obs <- if (nrow(dt_ipca)) as.character(dt_ipca[nrow(dt_ipca), date]) else NA_character_
      log_msg("IPEA OK: ipca_code=", best_ipca, "n=", nrow(dt_ipca), "last_observed=", last_obs, "elapsed_s=", elapsed2)
      
      ipca_file <- file.path(RUN_DIR, "ipea_ipca_series.csv")
      data.table::fwrite(dt_ipca, ipca_file)
      
      fetched <- list(code = best_ipca, n = nrow(dt_ipca), last_observed = last_obs, sample_csv = ipca_file)
    } else {
      log_msg("WARN: IPEA fetch_example skipped (no IPCA code resolved).")
    }
    
    list(
      ok = TRUE,
      available_n = nrow(dt_av),
      available_cols = names(dt_av),
      available_sample_csv = sample_av,
      searches = lapply(hits, function(x) list(n = x$n)), # keep JSON smaller
      ipca_fetch = fetched
    )
  }, error = function(e) {
    out$ok <<- FALSE
    msg <- conditionMessage(e)
    log_msg("ERROR: IPEA:", msg)
    list(ok = FALSE, error = msg)
  })
  
  res
}

# -------------------------------
# 3) CVM (CAD + ITR/DFP zips)
# -------------------------------
cvm_url_cad <- function() "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
cvm_url_itr <- function(year) sprintf("https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_%d.zip", as.integer(year))
cvm_url_dfp <- function(year) sprintf("https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_%d.zip", as.integer(year))

unzip_to <- function(zipfile, exdir) {
  dir.create(exdir, recursive = TRUE, showWarnings = FALSE)
  utils::unzip(zipfile, exdir = exdir)
  invisible(TRUE)
}

cvm_zip_inventory <- function(zipfile, year, kind) {
  exdir <- file.path(CACHE_DIR, sprintf("%s_%d", kind, year))
  unzip_to(zipfile, exdir)
  files <- list.files(exdir, full.names = TRUE)
  csvs <- files[grepl("\\.csv$", files, ignore.case = TRUE)]
  list(exdir = exdir, files = files, csvs = csvs)
}

cvm_schema_sample <- function(csv_path, n = 2000L) {
  dt <- data.table::fread(csv_path, nrows = n, showProgress = FALSE)
  list(
    cols = names(dt),
    n_sample = nrow(dt)
  )
}

test_cvm <- function(years = 2023:as.integer(format(Sys.Date(), "%Y"))) {
  log_msg("=== Test: CVM (CAD + ITR/DFP) ===")
  out <- list(ok = TRUE, cad = NULL, itr = list(), dfp = list())
  
  # CAD
  out$cad <- tryCatch({
    url <- cvm_url_cad()
    dest <- file.path(CACHE_DIR, "cad_cia_aberta.csv")
    download_cached(url, dest)
    dt <- data.table::fread(dest, nrows = 3000, showProgress = FALSE)
    log_msg("CVM OK: CAD saved:", dest, "MB=", round(file.info(dest)$size / 1024^2, 2))
    list(ok = TRUE, url = url, file = dest, cols = names(dt), n_sample = nrow(dt))
  }, error = function(e) {
    out$ok <<- FALSE
    msg <- conditionMessage(e)
    log_msg("ERROR: CVM CAD:", msg)
    list(ok = FALSE, error = msg)
  })
  
  # ITR/DFP per year
  for (y in years) {
    # ITR
    out$itr[[as.character(y)]] <- tryCatch({
      url <- cvm_url_itr(y)
      dest <- file.path(CACHE_DIR, sprintf("ITR_%d.zip", y))
      log_msg("Downloading:", url)
      download_cached(url, dest)
      inv <- cvm_zip_inventory(dest, y, "ITR")
      mb <- round(file.info(dest)$size / 1024^2, 2)
      log_msg("CVM OK: ITR", y, "zipMB=", mb, "files=", length(inv$files), "csvs=", length(inv$csvs))
      
      # Schema summary for first few csvs
      schema <- lapply(inv$csvs[1:min(5, length(inv$csvs))], function(p) {
        s <- cvm_schema_sample(p, n = 2000L)
        list(file = basename(p), cols = s$cols, n_sample = s$n_sample)
      })
      
      list(ok = TRUE, url = url, zip = dest, zip_mb = mb, n_csv = length(inv$csvs), schema = schema)
    }, error = function(e) {
      out$ok <<- FALSE
      msg <- conditionMessage(e)
      log_msg("ERROR: CVM ITR", y, ":", msg)
      list(ok = FALSE, error = msg)
    })
    
    # DFP
    out$dfp[[as.character(y)]] <- tryCatch({
      url <- cvm_url_dfp(y)
      dest <- file.path(CACHE_DIR, sprintf("DFP_%d.zip", y))
      log_msg("Downloading:", url)
      download_cached(url, dest)
      inv <- cvm_zip_inventory(dest, y, "DFP")
      mb <- round(file.info(dest)$size / 1024^2, 2)
      log_msg("CVM OK: DFP", y, "zipMB=", mb, "files=", length(inv$files), "csvs=", length(inv$csvs))
      if (mb < 1.0) log_msg("WARN: DFP", y, "zip is very small (", mb, "MB). This can be expected for the current year.")
      
      schema <- lapply(inv$csvs[1:min(5, length(inv$csvs))], function(p) {
        s <- cvm_schema_sample(p, n = 2000L)
        list(file = basename(p), cols = s$cols, n_sample = s$n_sample)
      })
      
      list(ok = TRUE, url = url, zip = dest, zip_mb = mb, n_csv = length(inv$csvs), schema = schema)
    }, error = function(e) {
      out$ok <<- FALSE
      msg <- conditionMessage(e)
      log_msg("ERROR: CVM DFP", y, ":", msg)
      list(ok = FALSE, error = msg)
    })
  }
  
  out
}

# -------------------------------
# 4) NEFIN risk factors (robust)
# -------------------------------
nefin_extract_download_href <- function(html) {
  # Find the segment around the anchor text and extract href="..."
  i <- regexpr("NEFIN Risk Factors", html, ignore.case = TRUE)
  if (i[1] < 0) return(NULL)
  
  seg <- substr(html, max(1, i[1] - 400), min(nchar(html), i[1] + 400))
  m <- regexec("href=[\"']([^\"']+)[\"']", seg, ignore.case = TRUE)
  rr <- regmatches(seg, m)
  if (length(rr) == 0 || length(rr[[1]]) < 2) return(NULL)
  rr[[1]][2]
}

nefin_absolutize <- function(href) {
  if (is.null(href) || !nzchar(href)) return(NULL)
  if (grepl("^https?://", href, ignore.case = TRUE)) return(href)
  # site is https://nefin.com.br
  if (!startsWith(href, "/")) href <- paste0("/", href)
  paste0("https://nefin.com.br", href)
}

test_nefin <- function() {
  log_msg("=== Test: NEFIN risk factors ===")
  out <- list(ok = TRUE)
  
  res <- tryCatch({
    page_url <- "https://nefin.com.br/data/risk_factors.html"
    html <- read_url_text(page_url)
    href <- nefin_extract_download_href(html)
    dl_url <- nefin_absolutize(href)
    
    if (is.null(dl_url)) stop("Could not extract NEFIN download link from risk_factors page.", call. = FALSE)
    
    # download locally
    ext <- tolower(tools::file_ext(dl_url))
    if (!nzchar(ext)) ext <- "dat"
    dest <- file.path(CACHE_DIR, paste0("nefin_risk_factors.", ext))
    download_cached(dl_url, dest, overwrite = TRUE)
    
    # read locally based on extension
    dt <- NULL
    if (ext %in% c("csv", "txt")) {
      dt <- data.table::fread(dest, showProgress = FALSE)
    } else if (ext %in% c("xls", "xlsx")) {
      stop_if_missing(c("readxl"))
      dt <- data.table::as.data.table(readxl::read_excel(dest))
    } else {
      # try fread anyway
      dt <- data.table::fread(dest, showProgress = FALSE)
    }
    
    # Guess a date column for max_date
    max_date <- NA_character_
    date_candidates <- names(dt)[grepl("date|data|dt", names(dt), ignore.case = TRUE)]
    if (length(date_candidates) > 0) {
      dc <- date_candidates[1]
      dd <- suppressWarnings(as.Date(dt[[dc]]))
      if (any(!is.na(dd))) max_date <- as.character(max(dd, na.rm = TRUE))
    }
    
    sample_file <- file.path(RUN_DIR, "nefin_risk_factors_sample.csv")
    data.table::fwrite(dt, sample_file)
    
    log_msg("NEFIN OK: n=", nrow(dt), "mb=", round(file.info(dest)$size / 1024^2, 2), "max_date=", max_date)
    
    list(
      ok = TRUE,
      page_url = page_url,
      download_url = dl_url,
      file = dest,
      n = nrow(dt),
      colnames = names(dt),
      max_date = max_date,
      sample_csv = sample_file
    )
  }, error = function(e) {
    out$ok <<- FALSE
    msg <- conditionMessage(e)
    log_msg("ERROR: NEFIN:", msg)
    list(ok = FALSE, error = msg)
  })
  
  res
}

# -------------------------------
# 5) Summary writers (stable KV flatten)
# -------------------------------
af_flatten_kv <- function(x, root = "summary") {
  rows <- list()
  i <- 0L
  
  push <- function(path, value) {
    i <<- i + 1L
    key <- as.character(path)
    
    value_chr <- if (is.null(value)) {
      NA_character_
    } else if (is.atomic(value) && length(value) == 1) {
      as.character(value)
    } else {
      # IMPORTANT: toJSON returns class "json" -> force plain character
      as.character(jsonlite::toJSON(value, auto_unbox = TRUE, null = "null"))
    }
    
    rows[[i]] <<- data.table::data.table(
      key = key,
      value = as.character(value_chr),
      type = as.character(paste(class(value), collapse = "|"))
    )
  }
  
  walk <- function(obj, path) {
    if (is.list(obj) && !is.data.frame(obj)) {
      nms <- names(obj)
      if (is.null(nms)) nms <- rep("", length(obj))
      for (k in seq_along(obj)) {
        nm <- nms[k]
        child_path <- if (nzchar(nm)) paste0(path, "/", nm) else paste0(path, "/", k)
        walk(obj[[k]], child_path)
      }
    } else {
      push(path, obj)
    }
  }
  
  walk(x, root)
  data.table::rbindlist(rows, fill = TRUE)
}

# -------------------------------
# 6) Run suite
# -------------------------------
summary_list <- list(
  run = list(
    run_id = RUN_ID,
    run_dir = RUN_DIR,
    started_at = ts_now()
  ),
  tests = list()
)

summary_list$tests$bcb   <- test_bcb()
summary_list$tests$ipea  <- test_ipea()
summary_list$tests$cvm   <- test_cvm(years = 2023:as.integer(format(Sys.Date(), "%Y")))
summary_list$tests$nefin <- test_nefin()

summary_list$run$ended_at <- ts_now()

# Write summary.json
json_file <- file.path(RUN_DIR, "summary.json")
jsonlite::write_json(summary_list, json_file, auto_unbox = TRUE, pretty = TRUE)
log_msg("Wrote:", json_file)

# Write summary_kv.csv
kv <- af_flatten_kv(summary_list, "summary")
kv_file <- file.path(RUN_DIR, "summary_kv.csv")
data.table::fwrite(kv, kv_file)
log_msg("Wrote:", kv_file)

log_msg("DONE.")
