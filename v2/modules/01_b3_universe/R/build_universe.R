# v2/modules/01_b3_universe/R/build_universe.R

af2_b3_cache_key <- function(year, include_types) {
  paste0(
    "cotahist_yearly_",
    year, "_",
    paste(sort(include_types), collapse = "-"),
    ".rds"
  )
}

af2_b3_build_universe_year <- function(year,
                                       include_types = NULL,
                                       cfg = NULL,
                                       verbose = TRUE,
                                       use_cache = TRUE,
                                       force_download = FALSE,
                                       reprocess = FALSE) {
  cfg <- cfg %||% af2_get_config()

  include_types <- include_types %||% cfg$include_types
  include_types <- af2_b3_validate_types(include_types)

  # Cache path
  cache_dir <- file.path(cfg$cache_dir, "b3_universe")
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  cache_file <- file.path(cache_dir, af2_b3_cache_key(year, include_types))

  if (isTRUE(use_cache) && file.exists(cache_file) && !isTRUE(force_download)) {
    if (verbose) af2_log("AF2_B3:", "Using cache: ", cache_file)
    dt_cached <- readRDS(cache_file)
    return(data.table::as.data.table(dt_cached))
  }

  # 1) Lazy fetch
  df_lazy <- af2_b3_fetch_yearly_lazy(
    year = year, cfg = cfg, verbose = verbose,
    force_download = force_download,
    reprocess = reprocess
  )

  # 2) Apply type filters lazily
  lazy_by_type <- af2_b3_apply_type_filters(df_lazy, include_types)

  # 3) Collect each type separately (bounded)
  out_list <- list()

  for (tp in names(lazy_by_type)) {
    if (verbose) af2_log("AF2_B3:", "Collecting type: ", tp, " for ", year)
    df_tp <- dplyr::collect(lazy_by_type[[tp]])
    if (!nrow(df_tp)) next

    dt_min <- af2_b3_select_min_cols(df_tp)
    dt_liq <- af2_b3_unify_liquidity(dt_min)
    dt_liq[, asset_type := tp]

    out_list[[tp]] <- dt_liq
  }

  if (!length(out_list)) {
    stop("af2_b3_build_universe_year: no data returned after filters for year ", year)
  }

  dt_year <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  data.table::setorder(dt_year, asset_type, symbol, refdate)

  # 4) Validate contract
  required <- c("symbol", "refdate", "open", "high", "low", "close", "turnover", "qty", "asset_type")
  miss <- setdiff(required, names(dt_year))
  if (length(miss)) {
    stop("Universe contract violated. Missing cols: ", paste(miss, collapse = ", "))
  }

  # 5) Drop obvious junk rows
  dt_year <- dt_year[!is.na(symbol) & symbol != "" & !is.na(refdate) & is.finite(close)]

  # 5b) Enforce unique (symbol, refdate) keys (upstream rb3 edge-case)
  # Dedupe rule (contract):
  # 1) max turnover, 2) max qty, 3) max close, 4) first deterministic row
  data.table::setorder(dt_year, asset_type, symbol, refdate)
  
  dup <- dt_year[, .N, by = .(symbol, refdate)][N > 1L]
  if (nrow(dup)) {
    if (verbose) {
      af2_log(
        "AF2_B3:",
        "WARNING: duplicated (symbol, refdate) detected in universe_raw year=", year,
        ". Applying dedupe rule. Examples:"
      )
      print(utils::head(dup[order(-N)], 10))
    }
  
    # Make ordering deterministic for tie-breaking
    if (!("turnover" %in% names(dt_year))) dt_year[, turnover := NA_real_]
    if (!("qty" %in% names(dt_year))) dt_year[, qty := NA_real_]
  
    dt_year <- dt_year[
      order(
        symbol, refdate,
        -data.table::fifelse(is.finite(turnover), turnover, -Inf),
        -data.table::fifelse(is.finite(qty), qty, -Inf),
        -data.table::fifelse(is.finite(close), close, -Inf)
      )
    ][, .SD[1L], by = .(symbol, refdate)]
  
    data.table::setorder(dt_year, asset_type, symbol, refdate)
  }

  if (verbose) {
    af2_log("AF2_B3:", "Year ", year, " rows = ", nrow(dt_year))
    af2_log("AF2_B3:", "Unique symbols = ", length(unique(dt_year$symbol)))
    af2_log("AF2_B3:", "Counts by type:")
    print(dt_year[, .N, by = asset_type][order(-N)])
  }

  # 6) Save cache
  if (isTRUE(use_cache)) {
    saveRDS(dt_year, cache_file)
    if (verbose) af2_log("AF2_B3:", "Wrote cache: ", cache_file)
  }

  dt_year
}

af2_b3_build_universe <- function(years = NULL,
                                  include_types = NULL,
                                  cfg = NULL,
                                  verbose = TRUE,
                                  use_cache = TRUE,
                                  force_download = FALSE,
                                  reprocess = FALSE) {
  cfg <- cfg %||% af2_get_config()
  years <- years %||% cfg$years
  years <- sort(unique(as.integer(years)))

  include_types <- include_types %||% cfg$include_types
  include_types <- af2_b3_validate_types(include_types)

  out <- list()
  for (y in years) {
    out[[as.character(y)]] <- af2_b3_build_universe_year(
      year = y,
      include_types = include_types,
      cfg = cfg,
      verbose = verbose,
      use_cache = use_cache,
      force_download = force_download,
      reprocess = reprocess
    )
  }

  dt_all <- data.table::rbindlist(out, use.names = TRUE, fill = TRUE)
  data.table::setorder(dt_all, asset_type, symbol, refdate)

  dt_all
}

af2_b3_cache_key_window <- function(start_date, end_date, include_types) {
  paste0(
    "cotahist_daily_",
    format(as.Date(start_date), "%Y%m%d"), "_",
    format(as.Date(end_date), "%Y%m%d"), "_",
    paste(sort(include_types), collapse = "-"),
    ".rds"
  )
}

af2_b3_build_universe_window <- function(start_date, end_date,
                                         include_types = NULL,
                                         cfg = NULL,
                                         verbose = TRUE,
                                         use_cache = TRUE,
                                         force_download = FALSE,
                                         reprocess = FALSE) {

  cfg <- cfg %||% af2_get_config()

  include_types <- include_types %||% cfg$include_types
  include_types <- af2_b3_validate_types(include_types)

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  # Cache path
  cache_dir <- file.path(cfg$cache_dir, "b3_universe")
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  cache_file <- file.path(cache_dir, af2_b3_cache_key_window(start_date, end_date, include_types))

  if (isTRUE(use_cache) && file.exists(cache_file) && !isTRUE(force_download)) {
    if (verbose) af2_log("AF2_B3:", "Using cache: ", cache_file)
    dt_cached <- readRDS(cache_file)
    return(data.table::as.data.table(dt_cached))
  }

  # 1) Lazy fetch daily window
  df_lazy <- af2_b3_fetch_daily_lazy(
    start_date = start_date,
    end_date   = end_date,
    cfg = cfg,
    verbose = verbose,
    force_download = force_download,
    reprocess = reprocess
  )

  # 2) Apply type filters lazily
  lazy_by_type <- af2_b3_apply_type_filters(df_lazy, include_types)

  # 3) Collect each type separately (bounded)
  out_list <- list()

  for (tp in names(lazy_by_type)) {
    if (verbose) af2_log("AF2_B3:", "Collecting type: ", tp,
                         " for window ", start_date, " to ", end_date)

    df_tp <- dplyr::collect(lazy_by_type[[tp]])
    if (!nrow(df_tp)) next

    dt_min <- af2_b3_select_min_cols(df_tp)
    dt_liq <- af2_b3_unify_liquidity(dt_min)
    dt_liq[, asset_type := tp]

    out_list[[tp]] <- dt_liq
  }

  if (!length(out_list)) {
    stop("af2_b3_build_universe_window: no data returned after filters.")
  }

  dt_win <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  data.table::setorder(dt_win, asset_type, symbol, refdate)

  # 4) Validate contract
  required <- c("symbol", "refdate", "open", "high", "low", "close",
                "turnover", "qty", "asset_type")
  miss <- setdiff(required, names(dt_win))
  if (length(miss)) stop("Universe contract violated. Missing cols: ", paste(miss, collapse = ", "))

  # 5) Drop obvious junk
  dt_win <- dt_win[!is.na(symbol) & symbol != "" & !is.na(refdate) & is.finite(close)]
  
  # 5b) Enforce unique (symbol, refdate) keys (upstream rb3 edge-case)
  data.table::setorder(dt_win, asset_type, symbol, refdate)
  
  dup <- dt_win[, .N, by = .(symbol, refdate)][N > 1L]
  if (nrow(dup)) {
    if (verbose) {
      af2_log(
        "AF2_B3:",
        "WARNING: duplicated (symbol, refdate) detected in universe_raw window ",
        as.character(start_date), " to ", as.character(end_date),
        ". Applying dedupe rule. Examples:"
      )
      print(utils::head(dup[order(-N)], 10))
    }
  
    if (!("turnover" %in% names(dt_win))) dt_win[, turnover := NA_real_]
    if (!("qty" %in% names(dt_win))) dt_win[, qty := NA_real_]
  
    dt_win <- dt_win[
      order(
        symbol, refdate,
        -data.table::fifelse(is.finite(turnover), turnover, -Inf),
        -data.table::fifelse(is.finite(qty), qty, -Inf),
        -data.table::fifelse(is.finite(close), close, -Inf)
      )
    ][, .SD[1L], by = .(symbol, refdate)]
  
    data.table::setorder(dt_win, asset_type, symbol, refdate)
  }

  if (verbose) {
    af2_log("AF2_B3:", "Window rows = ", nrow(dt_win))
    af2_log("AF2_B3:", "Unique symbols = ", length(unique(dt_win$symbol)))
    af2_log("AF2_B3:", "Counts by type:")
    print(dt_win[, .N, by = asset_type][order(-N)])
  }

  if (isTRUE(use_cache)) {
    saveRDS(dt_win, cache_file)
    if (verbose) af2_log("AF2_B3:", "Wrote cache: ", cache_file)
  }

  dt_win
}
