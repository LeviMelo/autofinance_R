# v2/modules/03_corporate_actions/R/build_registry.R
source("v2/modules/03_corporate_actions/R/fetch_events_yahoo_chart.R")

af2_ca_cache_file <- function(cfg, tag = "splits_dividends") {
  cache_dir <- file.path(cfg$cache_dir, "corp_actions")
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  file.path(cache_dir, paste0("corp_actions_", tag, ".rds"))
}

# Per-symbol cache directory
af2_ca_cache_dir_by_symbol <- function(cfg) {
  d <- file.path(cfg$cache_dir, "corp_actions", "by_symbol")
  if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  d
}

af2_ca_cache_file_symbol <- function(cfg, symbol) {
  symbol <- toupper(trimws(as.character(symbol)))
  file.path(af2_ca_cache_dir_by_symbol(cfg), paste0(symbol, ".rds"))
}

af2_ca_build_registry <- function(symbols,
                                  asset_types = NULL,
                                  cfg = NULL,
                                  from = "2018-01-01",
                                  to = Sys.Date(),
                                  verbose = TRUE,
                                  use_cache = TRUE,
                                  force_refresh = FALSE,
                                  n_workers = 1L,
                                  cache_mode = c("batch", "by_symbol")) {

  cfg <- cfg %||% af2_get_config()
  symbols <- as.character(symbols)
  symbols <- toupper(trimws(symbols))
  symbols <- symbols[!is.na(symbols) & nzchar(symbols)]
  symbols <- sort(unique(symbols))

  cache_mode <- match.arg(cache_mode)

  if (cache_mode == "by_symbol") {
    if (!exists("af2_ca_build_registry_by_symbol")) {
      stop("by_symbol cache_mode requested but af2_ca_build_registry_by_symbol is not defined.",
           call. = FALSE)
    }
    return(
      af2_ca_build_registry_by_symbol(
        symbols = symbols,
        asset_types = asset_types,
        cfg = cfg,
        from = from,
        to = to,
        verbose = verbose,
        use_cache = use_cache,
        force_refresh = force_refresh
      )
    )
  }

  if (!length(symbols)) stop("af2_ca_build_registry: empty symbols.", call. = FALSE)

  af2_ca_require("digest")

  from <- as.Date(from)
  to   <- as.Date(to)

  # Bucket 'to' by week to avoid daily refetch storms in dev
  to_tag   <- as.Date(cut(to, breaks = "week"))
  from_tag <- from

  # symbols is already sorted+unique above
  sym_hash <- digest::digest(symbols, algo = "xxhash64")

  tag <- paste0(
    format(from_tag, "%Y%m%d"), "_",
    format(to_tag, "%Y%m%d"), "_",
    length(symbols), "_",
    sym_hash
  )

  cache_file <- af2_ca_cache_file(cfg, tag)

  if (isTRUE(use_cache) && file.exists(cache_file) && !isTRUE(force_refresh)) {
    if (verbose) af2_log("AF2_CA:", "Using cache: ", cache_file)
    return(readRDS(cache_file))
  }

  yahoo_syms <- af2_yahoo_symbol_vec(symbols, asset_types)
  map_dt <- data.table::data.table(
    symbol = symbols,
    yahoo_symbol = yahoo_syms
  )
  map_dt <- map_dt[!is.na(yahoo_symbol)]

  if (!nrow(map_dt)) {
    stop("af2_ca_build_registry: no Yahoo symbols could be mapped.", call. = FALSE)
  }

  if (verbose) {
    af2_log("AF2_CA:", "Fetching corporate actions for ", nrow(map_dt), " symbols (Yahoo).")
  }

  worker_fun <- function(i) {
    sym <- map_dt$symbol[i]
    ysym <- map_dt$yahoo_symbol[i]

    mode <- tolower(cfg$ca_fetch_mode %||% "chart")

    out <- NULL
    if (mode == "chart") {
      out <- af2_ca_fetch_events_yahoo_chart_one(ysym, from = from, to = to, verbose = FALSE)
    } else {
      # fallback legacy (2 calls)
      dt_s <- af2_ca_fetch_splits_one(ysym, from = from, to = to, verbose = FALSE)
      dt_d <- af2_ca_fetch_dividends_one(ysym, from = from, to = to, verbose = FALSE, split.adjust = FALSE)
      out <- data.table::rbindlist(list(dt_s, dt_d), use.names = TRUE, fill = TRUE)
      if (!nrow(out)) out <- NULL
    }

    if (is.null(out) || !nrow(out)) return(NULL)

    out[, symbol := sym]
    out[, yahoo_symbol := ysym]
    out
  }

  res_list <- list()

  n_workers <- as.integer(n_workers)
  if (is.na(n_workers) || n_workers < 1L) n_workers <- 1L

  # Windows-safe optional parallel
  if (n_workers > 1L) {
    af2_ca_require("parallel")

    cl <- parallel::makeCluster(n_workers)
    on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)

    parallel::clusterExport(
      cl,
      varlist = c(
        "map_dt",
        "from", "to",
        "af2_ca_fetch_splits_one",
        "af2_ca_fetch_dividends_one",
        "af2_ca_fetch_events_yahoo_chart_one", # <--- ADD THIS
        "af2_ca_with_retry",
        "af2_ca_is_rate_limit_error",
        "af2_ca_require", # <--- ADD THIS (needed inside the new function)
        "af2_log"
      ),
      envir = environment()
    )
    parallel::clusterEvalQ(cl, {
      library(data.table)
      library(quantmod)
      library(xts)
      library(zoo)
      # Add these for the new chart fetcher:
      if (requireNamespace("jsonlite", quietly=TRUE)) library(jsonlite) 
      if (requireNamespace("curl", quietly=TRUE)) library(curl)
    })

    idx <- seq_len(nrow(map_dt))
    res_list <- parallel::parLapply(cl, idx, worker_fun)

  } else {
    for (i in seq_len(nrow(map_dt))) {
      res_list[[i]] <- worker_fun(i)
      if (verbose && i %% 50 == 0) {
        af2_log("AF2_CA:", "Progress: ", i, "/", nrow(map_dt))
      }
    }
  }

  dt_all <- data.table::rbindlist(res_list, use.names = TRUE, fill = TRUE)

  if (!nrow(dt_all)) {
    # It is valid to have zero actions, but keep schema stable
    dt_all <- data.table::data.table(
      symbol = character(),
      yahoo_symbol = character(),
      refdate = as.Date(character()),
      action_type = character(),
      value = numeric(),
      source = character()
    )
  }

  # Normalize + order
  dt_all[, refdate := as.Date(refdate)]
  dt_all <- dt_all[order(symbol, action_type, refdate)]

  # Basic contract sanity
  req <- c("symbol", "yahoo_symbol", "refdate", "action_type", "value", "source")
  miss <- setdiff(req, names(dt_all))
  if (length(miss)) stop("Corporate actions registry missing cols: ", paste(miss, collapse = ", "))

  if (isTRUE(use_cache)) {
    saveRDS(dt_all, cache_file)
    if (verbose) af2_log("AF2_CA:", "Wrote cache: ", cache_file)
  }

  dt_all
}

af2_ca_build_registry_by_symbol <- function(symbols,
                                            asset_types = NULL,
                                            cfg = NULL,
                                            from = "2018-01-01",
                                            to = Sys.Date(),
                                            verbose = TRUE,
                                            use_cache = TRUE,
                                            force_refresh = FALSE) {

  cfg <- cfg %||% af2_get_config()

  symbols <- as.character(symbols)
  symbols <- toupper(trimws(symbols))
  symbols <- symbols[!is.na(symbols) & nzchar(symbols)]
  symbols <- sort(unique(symbols))

  if (!length(symbols)) {
    stop("af2_ca_build_registry_by_symbol: empty symbols.", call. = FALSE)
  }

  from <- as.Date(from)
  to   <- as.Date(to)

  yahoo_syms <- af2_yahoo_symbol_vec(symbols, asset_types)
  map_dt <- data.table::data.table(
    symbol = symbols,
    yahoo_symbol = yahoo_syms
  )
  map_dt <- map_dt[!is.na(yahoo_symbol)]

  if (!nrow(map_dt)) {
    stop("af2_ca_build_registry_by_symbol: no Yahoo symbols could be mapped.", call. = FALSE)
  }

  if (verbose) {
    af2_log("AF2_CA:", "by_symbol mode: candidates=", nrow(map_dt))
  }

  res_list <- vector("list", nrow(map_dt))

  for (i in seq_len(nrow(map_dt))) {

    sym  <- map_dt$symbol[i]
    ysym <- map_dt$yahoo_symbol[i]

    cache_file <- af2_ca_cache_file_symbol(cfg, sym)

    if (isTRUE(use_cache) && file.exists(cache_file) && !isTRUE(force_refresh)) {
      dt_cached <- readRDS(cache_file)
      dt_cached <- data.table::as.data.table(dt_cached)
      if (nrow(dt_cached)) {
        dt_cached[, refdate := as.Date(refdate)]
        dt_cached <- dt_cached[refdate >= from & refdate <= to]
      }
      res_list[[i]] <- dt_cached
      next
    }

    dt_s <- af2_ca_fetch_splits_one(ysym, from = from, to = to, verbose = FALSE)
    dt_d <- af2_ca_fetch_dividends_one(ysym, from = from, to = to, verbose = FALSE)

    out <- data.table::rbindlist(list(dt_s, dt_d), use.names = TRUE, fill = TRUE)

    if (!nrow(out)) {
      out <- data.table::data.table(
        symbol = character(),
        yahoo_symbol = character(),
        refdate = as.Date(character()),
        action_type = character(),
        value = numeric(),
        source = character()
      )[0]
    } else {
      out[, symbol := sym]
      out[, yahoo_symbol := ysym]
    }

    if (isTRUE(use_cache)) {
      # Save even empty to avoid refetch storms
      saveRDS(out, cache_file)
    }

    res_list[[i]] <- out

    if (verbose && i %% 50 == 0) {
      af2_log("AF2_CA:", "by_symbol progress: ", i, "/", nrow(map_dt))
    }
  }

  dt_all <- data.table::rbindlist(res_list, use.names = TRUE, fill = TRUE)

  if (!nrow(dt_all)) {
    dt_all <- data.table::data.table(
      symbol = character(),
      yahoo_symbol = character(),
      refdate = as.Date(character()),
      action_type = character(),
      value = numeric(),
      source = character()
    )
  }

  dt_all[, refdate := as.Date(refdate)]
  dt_all <- dt_all[order(symbol, action_type, refdate)]

  req <- c("symbol", "yahoo_symbol", "refdate", "action_type", "value", "source")
  miss <- setdiff(req, names(dt_all))
  if (length(miss)) stop("Corporate actions registry missing cols: ", paste(miss, collapse = ", "))

  dt_all
}
