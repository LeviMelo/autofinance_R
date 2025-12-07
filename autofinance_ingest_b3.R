############################################################
# autofinance_ingest_b3.R
# Sincronização COTAHIST -> prices_raw (rb3)
############################################################

if (!exists("af_attach_packages")) {
  if (file.exists("autofinance_config.R")) {
    source("autofinance_config.R")
  } else {
    stop("autofinance_config.R not found; please source it before using ingest.")
  }
}

if (!exists("af_db_connect")) {
  if (file.exists("autofinance_db_core.R")) {
    source("autofinance_db_core.R")
  } else {
    stop("autofinance_db_core.R not found; required for ingest DB access.")
  }
}

af_attach_packages(c("rb3", "data.table", "dplyr", "lubridate"))

af_is_valid_db_con <- function(con) {
  if (is.null(con)) return(FALSE)
  ok_class <- inherits(con, "DBIConnection") || inherits(con, "SQLiteConnection")
  if (!ok_class) return(FALSE)
  ok_valid <- tryCatch(DBI::dbIsValid(con), error = function(...) FALSE)
  isTRUE(ok_valid)
}


# ----------------------------------------------------------------------
# rb3 bootstrap + cache dir
# ----------------------------------------------------------------------

af_rb3_init <- function(cache_dir = "data/rb3_cache", verbose = TRUE) {
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  # rb3 uses this option for local cache
  options(rb3.cachedir = normalizePath(cache_dir, winslash = "/", mustWork = FALSE))

  # Bootstrap templates DB (safe no-op if already done)
  tryCatch({
    rb3::rb3_bootstrap()
    if (verbose) message("rb3 bootstrap OK; cache dir = ", getOption("rb3.cachedir"))
  }, error = function(e) {
    if (verbose) message("rb3 bootstrap warning: ", conditionMessage(e))
  })

  invisible(TRUE)
}

# ----------------------------------------------------------------------
# Fetch + normalize one year
# ----------------------------------------------------------------------

af_fetch_cotahist_year <- function(year,
                                   asset_filter = c("equity", "etf", "fii", "all"),
                                   verbose = TRUE) {
  asset_filter <- match.arg(asset_filter)
  af_rb3_init(verbose = verbose)

  if (verbose) message(sprintf("COTAHIST yearly: preparing %s...", year))

  # 1) Prefer fetch_marketdata (download + read + stage)
  ok_fetch <- TRUE
  tryCatch({
    rb3::fetch_marketdata(
      "b3-cotahist-yearly",
      year = year,
      throttle = TRUE
    )
  }, error = function(e) {
    ok_fetch <<- FALSE
    if (verbose) message("fetch_marketdata failed: ", conditionMessage(e))
  })

  # 2) Fallback to explicit download/read if needed
  if (!ok_fetch) {
    tryCatch({
      meta <- rb3::download_marketdata("b3-cotahist-yearly", year = year)
      rb3::read_marketdata(meta)
    }, error = function(e) {
      warning("rb3 download/read fallback failed for ", year, ": ", conditionMessage(e))
      return(data.table::data.table())
    })
  }

  # 3) Access lazy dataset
  df_raw <- tryCatch({
    rb3::cotahist_get("yearly")
  }, error = function(e) {
    warning("cotahist_get('yearly') failed: ", conditionMessage(e))
    return(NULL)
  })

  if (is.null(df_raw)) return(data.table::data.table())

  # 4) Filter year
  df_year <- df_raw |>
    dplyr::filter(lubridate::year(.data$refdate) == year)

  # 5) Optional asset-class filters (rb3 helpers)
  if (asset_filter == "equity") {
    df_year <- rb3::cotahist_filter_equity(df_year)
  } else if (asset_filter == "etf") {
    df_year <- rb3::cotahist_filter_etf(df_year)
  } else if (asset_filter == "fii") {
    df_year <- rb3::cotahist_filter_fii(df_year)
  }

  df_year <- dplyr::collect(df_year)
  if (!nrow(df_year)) return(data.table::data.table())

  dt <- data.table::as.data.table(df_year)

  # Normalize symbol
  if ("symbol" %in% names(dt)) dt[, symbol := trimws(symbol)]

  # Defensive column picker
  col_pick <- function(alts) {
    cand <- intersect(alts, names(dt))
    if (length(cand)) dt[[cand[1L]]] else NA_real_
  }

  dt_out <- data.table::data.table(
    symbol  = dt[["symbol"]],
    refdate = as.Date(dt[["refdate"]]),
    open    = as.numeric(col_pick(c("open", "price.open", "preco_abertura"))),
    high    = as.numeric(col_pick(c("high", "price.high", "preco_maximo"))),
    low     = as.numeric(col_pick(c("low", "price.low", "preco_minimo"))),
    close   = as.numeric(col_pick(c("close", "price.close", "preco_ultimo"))),
    vol_fin = as.numeric(col_pick(c("financial_volume", "volume", "volume_total"))),
    qty     = as.numeric(col_pick(c("trade_quantity", "quantity", "number_trades", "quantidade_negociada")))
  )

  dt_out <- dt_out[!is.na(symbol) & !is.na(refdate) & !is.na(close)]
  dt_out
}

# ----------------------------------------------------------------------
# Heuristic classifier (kept simple; better metadata can be added later)
# ----------------------------------------------------------------------

af_classify_symbol <- function(symbol_chr) {
  sym <- toupper(trimws(symbol_chr))
  investable <- TRUE
  asset_type <- "OTHER"

  if (grepl("F$", sym) || grepl("[^A-Z0-9]", sym) || nchar(sym) > 8) {
    investable <- FALSE
  }

  if (grepl("(32|33|34|35|36|39)$", sym)) {
    asset_type <- "BDR"
  } else if (grepl("11$", sym)) {
    asset_type <- "ETF_FII"
  } else if (grepl("[0-9]{1,2}$", sym)) {
    asset_type <- "EQUITY"
  }

  if (!investable) asset_type <- "OTHER"

  list(asset_type = asset_type, active = as.integer(investable))
}

# ----------------------------------------------------------------------
# Sync driver
# ----------------------------------------------------------------------

af_sync_b3 <- function(con = NULL,
                       years = NULL,
                       verbose = TRUE) {
  af_attach_packages(c("DBI", "RSQLite", "data.table"))

  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }

  if (!af_is_valid_db_con(con)) {
    stop("af_sync_b3: 'con' is not a valid DBI connection.")
  }

  af_db_init(con)

  if (is.null(years)) {
    max_date_str <- tryCatch({
      DBI::dbGetQuery(con, "SELECT MAX(refdate) as d FROM prices_raw")$d
    }, error = function(e) NA)

    current_year <- as.integer(format(Sys.Date(), "%Y"))

    if (is.na(max_date_str) || is.null(max_date_str)) {
      years <- 2015:current_year
    } else {
      last_year <- as.integer(substr(max_date_str, 1, 4))
      years <- last_year:current_year
    }
  }

  years <- sort(unique(as.integer(years)))

  for (y in years) {
    if (verbose) message("af_sync_b3: processing year ", y)

    dt <- af_fetch_cotahist_year(y, asset_filter = "all", verbose = verbose)

    if (nrow(dt) > 0) {
      if (verbose) message("  inserting ", nrow(dt), " rows...")
      af_db_insert_prices_raw(con, dt)

      # Seed assets_meta for corporate actions sync
      syms <- unique(dt$symbol)
      if (length(syms) > 0L) {
        classified <- lapply(syms, af_classify_symbol)
        meta_dt <- data.table::data.table(
          symbol             = syms,
          asset_type         = vapply(classified, `[[`, "", "asset_type"),
          sector             = NA_character_,
          active             = vapply(classified, `[[`, integer(1), "active"),
          last_update_splits = NA_character_,
          last_update_divs   = NA_character_
        )
        af_db_upsert_assets_meta(con, meta_dt)
      }
    } else {
      if (verbose) message("  no data found for ", y)
    }
  }

  invisible(TRUE)
}
