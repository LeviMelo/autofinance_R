# ============================================================
# playground/external_data_feasibility.R
# External data feasibility + diagnostics (BCB/SGS, IPEA, CVM, NEFIN)
# Writes:
#   _runs/<ts>/summary.json
#   _runs/<ts>/summary_kv.csv
#   _runs/<ts>/... sample files
# ============================================================

suppressPackageStartupMessages({
  if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table")
  if (!requireNamespace("httr", quietly = TRUE))       install.packages("httr")
  if (!requireNamespace("jsonlite", quietly = TRUE))   install.packages("jsonlite")
  if (!requireNamespace("readr", quietly = TRUE))      install.packages("readr")
  if (!requireNamespace("ipeadatar", quietly = TRUE))  install.packages("ipeadatar")
})

library(data.table)

# -------------------------------
# 0) Config
# -------------------------------
PLAYGROUND_DIR <- normalizePath(
  "C:/Users/Galaxy/LEVI/Projetos R/autofinance_R/playground",
  winslash = "/", mustWork = TRUE
)

TS <- format(Sys.time(), "%Y%m%d_%H%M%S")
RUN_DIR   <- file.path(PLAYGROUND_DIR, "_runs", TS)
CACHE_DIR <- file.path(RUN_DIR, "cache")
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

log_msg <- function(..., sep = " ") {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  msg <- paste(..., sep = sep)
  cat(sprintf("[%s] %s\n", ts, msg))
}

log_msg("RUN_DIR:", RUN_DIR)

# -------------------------------
# 1) Low-level HTTP helpers
# -------------------------------
ua_string <- "autofinance_R playground feasibility (httr)"

http_get_text <- function(url, timeout_s = 60) {
  resp <- httr::GET(
    url,
    httr::user_agent(ua_string),
    httr::timeout(timeout_s)
  )
  status <- httr::status_code(resp)
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  list(status = status, text = txt, url = url)
}

download_cached <- function(url, destfile, timeout_s = 120) {
  if (file.exists(destfile) && file.info(destfile)$size > 0) {
    return(list(ok = TRUE, cached = TRUE, file = destfile, mb = file.info(destfile)$size / 1024^2))
  }
  resp <- httr::GET(
    url,
    httr::user_agent(ua_string),
    httr::timeout(timeout_s),
    httr::write_disk(destfile, overwrite = TRUE)
  )
  status <- httr::status_code(resp)
  ok <- status >= 200 && status < 300
  if (!ok) {
    if (file.exists(destfile)) unlink(destfile)
    return(list(ok = FALSE, status = status, url = url))
  }
  list(ok = TRUE, cached = FALSE, file = destfile, mb = file.info(destfile)$size / 1024^2)
}

# -------------------------------
# 2) Robust list flattener (never crashes)
# -------------------------------
af_flatten_kv <- function(x, root = "summary") {
  paths  <- character()
  values <- character()
  
  add_row <- function(p, v) {
    paths  <<- c(paths,  as.character(p))
    values <<- c(values, as.character(v))
  }
  
  to_value <- function(v) {
    if (is.null(v)) return("null")
    
    if (inherits(v, c("Date", "POSIXct", "POSIXt"))) {
      return(as.character(v))
    }
    
    # scalar atomic -> string
    if (is.atomic(v) && length(v) == 1) {
      return(as.character(v))
    }
    
    # everything else -> JSON string, but STRIP CLASS/ATTRIBUTES
    s <- jsonlite::toJSON(v, auto_unbox = TRUE, null = "null")
    s <- unclass(s)     # removes class "json"
    s <- paste0(s)      # guarantees plain character
    s
  }
  
  rec <- function(obj, path) {
    if (is.list(obj)) {
      nms <- names(obj)
      if (is.null(nms)) nms <- as.character(seq_along(obj))
      
      if (length(obj) == 0) {
        add_row(path, "{}")
        return(invisible())
      }
      
      for (nm in nms) {
        rec(obj[[nm]], paste0(path, "/", nm))
      }
      return(invisible())
    }
    
    add_row(path, to_value(obj))
    invisible()
  }
  
  rec(x, root)
  data.table::data.table(path = paths, value = values)
}

# -------------------------------
# 3) BCB/SGS
#    Key fixes:
#      - Use /ultimos/N to get latest available observation date (avoids weekend/holiday 404)
#      - Always fetch within <= 10 years (per BCB restrictions)
# -------------------------------
bcb_fmt_date <- function(d) format(as.Date(d), "%d/%m/%Y")

bcb_url_range <- function(series_id, start_date, end_date) {
  sprintf(
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados?formato=json&dataInicial=%s&dataFinal=%s",
    series_id, bcb_fmt_date(start_date), bcb_fmt_date(end_date)
  )
}

bcb_url_ultimos <- function(series_id, n = 20) {
  sprintf(
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados/ultimos/%s?formato=json",
    series_id, as.integer(n)
  )
}

bcb_parse_json <- function(txt) {
  x <- jsonlite::fromJSON(txt)
  if (!is.data.frame(x) || nrow(x) == 0) return(data.table())
  dt <- as.data.table(x)
  # Typical keys: data (dd/mm/YYYY), valor (string number)
  if ("data" %in% names(dt)) {
    dt[, data := as.Date(data, format = "%d/%m/%Y")]
  }
  if ("valor" %in% names(dt)) {
    dt[, valor := suppressWarnings(as.numeric(gsub(",", ".", valor)))]
  }
  dt[]
}

safe_years_ago <- function(d, years = 10) {
  d <- as.Date(d)
  y <- as.integer(format(d, "%Y")) - years
  m <- as.integer(format(d, "%m"))
  day <- as.integer(format(d, "%d"))
  for (k in 0:5) {
    dd <- day - k
    if (dd >= 1) {
      cand <- as.Date(sprintf("%04d-%02d-%02d", y, m, dd))
      if (!is.na(cand)) return(cand)
    }
  }
  d - years * 365
}

bcb_fetch_ultimos <- function(series_id, n = 20) {
  url <- bcb_url_ultimos(series_id, n)
  res <- http_get_text(url, timeout_s = 60)
  if (res$status != 200) {
    stop(sprintf("HTTP status %s | URL: %s", res$status, res$url))
  }
  bcb_parse_json(res$text)
}

bcb_fetch_range <- function(series_id, start_date, end_date) {
  url <- bcb_url_range(series_id, start_date, end_date)
  res <- http_get_text(url, timeout_s = 60)
  if (res$status != 200) {
    stop(sprintf("HTTP status %s | URL: %s", res$status, res$url))
  }
  bcb_parse_json(res$text)
}

test_bcb <- function() {
  log_msg("=== Test: BCB/SGS ===")
  
  series <- list(
    selic_daily   = 11,
    cdi_daily     = 12,
    ipca_month    = 433,
    usdbrl_ptax   = 10813
  )
  
  out <- list(ok = TRUE, provider = "BCB/SGS", series = list())
  
  for (nm in names(series)) {
    id <- series[[nm]]
    t0 <- Sys.time()
    tryCatch({
      # 1) get latest observation date via /ultimos/20 (avoids weekend/holiday issues)
      last <- bcb_fetch_ultimos(id, n = 20)
      if (nrow(last) == 0) stop("No data returned from /ultimos.")
      last_date <- max(last$data, na.rm = TRUE)
      
      # 2) fetch <=10y window ending at last_date (BCB restriction)
      start_date <- safe_years_ago(last_date, years = 10)
      
      dt <- bcb_fetch_range(id, start_date, last_date)
      elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
      
      out$series[[nm]] <- list(
        ok = TRUE,
        id = id,
        n = nrow(dt),
        min = if (nrow(dt)) as.character(min(dt$data, na.rm = TRUE)) else NA_character_,
        max = if (nrow(dt)) as.character(max(dt$data, na.rm = TRUE)) else NA_character_,
        last_observed = as.character(last_date),
        elapsed_s = round(elapsed, 3)
      )
      log_msg("BCB OK:", nm, "(id=", id, ") n=", nrow(dt),
              "last_observed=", as.character(last_date),
              "elapsed_s=", round(elapsed, 2))
      
    }, error = function(e) {
      out$ok <- FALSE
      out$series[[nm]] <- list(ok = FALSE, id = id, error = conditionMessage(e))
      log_msg("ERROR: BCB fetch", nm, "(", id, "):", conditionMessage(e))
    })
  }
  
  out
}

# -------------------------------
# 4) IPEA (ipeadatar)
#    Key fix: use search_series(terms=..., fields=..., language=...)
# -------------------------------
test_ipea <- function() {
  log_msg("=== Test: IPEA (ipeadatar) ===")
  
  out <- list(ok = TRUE, provider = "IPEA", checks = list())
  
  tryCatch({
    t0 <- Sys.time()
    avail <- ipeadatar::available_series()
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    
    dt_av <- as.data.table(avail)
    out$checks$available_series <- list(
      ok = TRUE,
      n = nrow(dt_av),
      cols = names(dt_av),
      elapsed_s = round(elapsed, 3)
    )
    log_msg("IPEA OK: available_series n=", nrow(dt_av), "cols=", ncol(dt_av), "elapsed_s=", round(elapsed, 2))
    
    # Save a sample for human inspection
    sample_file <- file.path(RUN_DIR, "ipea_available_series_sample.csv")
    fwrite(dt_av[1:min(.N, 2000)], sample_file)
    log_msg("Wrote:", sample_file)
    
    # Search (proper API)
    terms <- c("ipca", "selic", "cambio", "câmbio", "dolar", "dólar", "cdi")
    out$checks$search <- list()
    
    for (term in terms) {
      t1 <- Sys.time()
      res <- tryCatch(
        ipeadatar::search_series(terms = term, fields = c("name", "code", "theme"), language = "br"),
        error = function(e) e
      )
      elapsed1 <- as.numeric(difftime(Sys.time(), t1, units = "secs"))
      
      if (inherits(res, "error")) {
        out$ok <- FALSE
        out$checks$search[[term]] <- list(ok = FALSE, error = conditionMessage(res))
        log_msg("ERROR: IPEA search term=", term, ":", conditionMessage(res))
      } else {
        dt_res <- as.data.table(res)
        out$checks$search[[term]] <- list(ok = TRUE, hits = nrow(dt_res), elapsed_s = round(elapsed1, 3))
        log_msg("IPEA search:", term, "hits=", nrow(dt_res), "elapsed_s=", round(elapsed1, 2))
      }
    }
    
    # Try a direct fetch if we can identify a code from search, else skip gracefully
    # We'll pick the first hit from term "ipca" if present.
    ipca_hits <- tryCatch(ipeadatar::search_series(terms = "ipca", fields = c("name", "code"), language = "br"),
                          error = function(e) NULL)
    if (!is.null(ipca_hits) && nrow(ipca_hits) > 0 && "code" %in% names(ipca_hits)) {
      code <- ipca_hits$code[1]
      t2 <- Sys.time()
      dat <- ipeadatar::ipeadata(code)
      dt_dat <- as.data.table(dat)
      elapsed2 <- as.numeric(difftime(Sys.time(), t2, units = "secs"))
      
      out$checks$fetch_example <- list(
        ok = TRUE, code = code,
        n = nrow(dt_dat),
        cols = names(dt_dat),
        elapsed_s = round(elapsed2, 3)
      )
      log_msg("IPEA fetch OK: code=", code, "n=", nrow(dt_dat), "elapsed_s=", round(elapsed2, 2))
    } else {
      out$checks$fetch_example <- list(ok = FALSE, note = "No IPCA code resolved via search_series().")
      log_msg("WARN: IPEA fetch_example skipped (no IPCA code resolved).")
    }
    
  }, error = function(e) {
    out$ok <- FALSE
    out$error <- conditionMessage(e)
    log_msg("ERROR: IPEA:", conditionMessage(e))
  })
  
  out
}

# -------------------------------
# 5) CVM (CAD + ITR/DFP)
# -------------------------------
cvm_url <- function(kind = c("CAD", "ITR", "DFP"), year = NULL) {
  kind <- match.arg(kind)
  base <- "https://dados.cvm.gov.br/dados/CIA_ABERTA"
  
  if (kind == "CAD") {
    return(sprintf("%s/CAD/DADOS/cad_cia_aberta.csv", base))
  }
  stopifnot(!is.null(year))
  sprintf("%s/DOC/%s/DADOS/%s_cia_aberta_%s.zip",
          base, kind, tolower(kind), as.integer(year))
}

test_cvm <- function(years = 2023:as.integer(format(Sys.Date(), "%Y"))) {
  log_msg("=== Test: CVM (CAD + ITR/DFP) ===")
  out <- list(ok = TRUE, provider = "CVM", cad = NULL, years = list())
  
  # CAD
  tryCatch({
    url <- cvm_url("CAD")
    dest <- file.path(CACHE_DIR, "cad_cia_aberta.csv")
    dl <- download_cached(url, dest)
    if (!dl$ok) stop(sprintf("CAD download failed status=%s url=%s", dl$status, dl$url))
    out$cad <- list(ok = TRUE, file = dest, mb = round(dl$mb, 2))
    log_msg("CVM OK: CAD saved:", dest, "MB=", round(dl$mb, 2))
  }, error = function(e) {
    out$ok <- FALSE
    out$cad <- list(ok = FALSE, error = conditionMessage(e))
    log_msg("ERROR: CVM CAD:", conditionMessage(e))
  })
  
  for (yr in years) {
    out$years[[as.character(yr)]] <- list()
    
    for (kind in c("ITR", "DFP")) {
      nm <- paste0(kind, "_", yr)
      url <- cvm_url(kind, yr)
      dest <- file.path(CACHE_DIR, sprintf("%s_%s.zip", kind, yr))
      
      tryCatch({
        log_msg("Downloading:", url)
        dl <- download_cached(url, dest, timeout_s = 180)
        if (!dl$ok) stop(sprintf("%s download failed status=%s url=%s", nm, dl$status, dl$url))
        
        # List zip contents
        z <- unzip(dest, list = TRUE)
        nfiles <- nrow(z)
        ncsv <- sum(grepl("\\.csv$", z$Name, ignore.case = TRUE))
        
        # Sample first 3 CSVs (head 3000 rows) without extracting everything
        # unzip() to temp dir and read a couple
        exdir <- file.path(CACHE_DIR, sprintf("unzip_%s_%s", kind, yr))
        dir.create(exdir, showWarnings = FALSE)
        unzip(dest, exdir = exdir)
        
        csvs <- list.files(exdir, pattern = "\\.csv$", full.names = TRUE)
        samples <- list()
        for (i in seq_len(min(3, length(csvs)))) {
          f <- csvs[i]
          dt <- fread(f, nrows = 3000, showProgress = FALSE)
          samples[[basename(f)]] <- list(n = nrow(dt), cols = ncol(dt))
        }
        
        out$years[[as.character(yr)]][[kind]] <- list(
          ok = TRUE,
          zip = dest,
          zipMB = round(dl$mb, 2),
          files = nfiles,
          csvs = ncsv,
          samples = samples
        )
        
        log_msg("CVM OK:", kind, yr, "zipMB=", round(dl$mb, 2), "files=", nfiles, "csvs=", ncsv)
        
        # Special note: DFP for the current year is often tiny because annual filings
        # typically arrive the following year (this is expected, not necessarily a bug).
        if (kind == "DFP" && dl$mb < 1) {
          log_msg("WARN:", kind, yr, "zip is very small (", round(dl$mb, 2), "MB). This can be expected for the current year.")
        }
        
      }, error = function(e) {
        out$ok <- FALSE
        out$years[[as.character(yr)]][[kind]] <- list(ok = FALSE, error = conditionMessage(e), url = url)
        log_msg("ERROR: CVM", kind, yr, ":", conditionMessage(e))
      })
    }
  }
  
  out
}

# -------------------------------
# 6) NEFIN Risk Factors
# -------------------------------
test_nefin <- function() {
  log_msg("=== Test: NEFIN risk factors ===")
  out <- list(ok = TRUE, provider = "NEFIN", risk_factors = NULL)
  
  # As linked on NEFIN Risk Factors page
  url <- "https://nefin.com.br/resources/risk_factors/nefin_factors.csv"
  dest <- file.path(CACHE_DIR, "nefin_factors.csv")
  
  tryCatch({
    dl <- download_cached(url, dest, timeout_s = 120)
    if (!dl$ok) stop(sprintf("NEFIN download failed status=%s url=%s", dl$status, dl$url))
    
    dt <- fread(dest, showProgress = FALSE)
    
    # Guess a date column for quick sanity checks
    date_col <- intersect(names(dt), c("date", "Date", "DATA", "data"))[1]
    if (!is.na(date_col)) {
      dt[, (date_col) := as.Date(get(date_col))]
    }
    
    out$risk_factors <- list(
      ok = TRUE,
      file = dest,
      mb = round(dl$mb, 2),
      n = nrow(dt),
      cols = names(dt),
      date_col = ifelse(is.na(date_col), NA_character_, date_col),
      max_date = ifelse(is.na(date_col), NA_character_, as.character(max(dt[[date_col]], na.rm = TRUE)))
    )
    
    log_msg("NEFIN OK: n=", nrow(dt), "mb=", round(dl$mb, 2),
            "max_date=", out$risk_factors$max_date)
    
  }, error = function(e) {
    out$ok <- FALSE
    out$risk_factors <- list(ok = FALSE, error = conditionMessage(e), url = url)
    log_msg("ERROR: NEFIN:", conditionMessage(e))
  })
  
  out
}

# -------------------------------
# 7) Main runner
# -------------------------------
summary_list <- list(
  run = list(
    run_dir = RUN_DIR,
    cache_dir = CACHE_DIR,
    when = Sys.time(),
    session = capture.output(sessionInfo())
  ),
  tests = list()
)

summary_list$tests$bcb  <- test_bcb()
summary_list$tests$ipea <- test_ipea()
summary_list$tests$cvm  <- test_cvm(years = 2023:2025)
summary_list$tests$nefin <- test_nefin()

# Write summary.json
json_file <- file.path(RUN_DIR, "summary.json")
jsonlite::write_json(summary_list, json_file, auto_unbox = TRUE, pretty = TRUE)
log_msg("Wrote:", json_file)

# Write summary_kv.csv (never crashes)
kv <- af_flatten_kv(summary_list, "summary")
kv_file <- file.path(RUN_DIR, "summary_kv.csv")
fwrite(kv, kv_file)
log_msg("Wrote:", kv_file)

log_msg("DONE.")
