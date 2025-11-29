############################################################
# autofinance_ingest_b3.R
# Sincronização COTAHIST -> prices_raw (rb3 API real)
############################################################

# The current rb3 API expects a "type" string, not a year. We:
# 1) Ensure the yearly files are available via fetch_marketdata()
# 2) Pull the lazy dataset with cotahist_get("yearly")
# 3) Filter the requested year and asset class
# 4) Normalize column names to prices_raw schema

af_fetch_cotahist_year <- function(year,
                                   asset_filter = c("equity", "etf", "fii", "all"),
                                   verbose = TRUE) {
  asset_filter <- match.arg(asset_filter)
  af_attach_packages(c("rb3", "data.table", "dplyr", "lubridate"))

  if (verbose) message(sprintf("Downloading COTAHIST for %s...", year))

  # 1) Ensure yearly files are present (no-op if already cached)
  tryCatch({
    rb3::fetch_marketdata("b3-cotahist-yearly", year = year)
  }, error = function(e) {
    warning(sprintf("fetch_marketdata failed for %s: %s", year, e$message))
  })

  # 2) Lazy dataset
  df_raw <- tryCatch({
    rb3::cotahist_get("yearly")
  }, error = function(e) {
    warning(sprintf("cotahist_get failed: %s", e$message))
    return(NULL)
  })

  if (is.null(df_raw)) return(data.table::data.table())

  # 3) Filter year and asset type
  df_year <- df_raw |>
    dplyr::filter(lubridate::year(.data$refdate) == year)

  if (asset_filter == "equity") {
    df_year <- rb3::cotahist_filter_equity(df_year)
  } else if (asset_filter == "etf") {
    df_year <- rb3::cotahist_filter_etf(df_year)
  } else if (asset_filter == "fii") {
    df_year <- rb3::cotahist_filter_fii(df_year)
  } # "all" keeps everything

  df_year <- dplyr::collect(df_year)
  if (!nrow(df_year)) return(data.table::data.table())

  dt <- data.table::as.data.table(df_year)

  # Normalize symbol and columns (rb3 uses English names already)
  if ("symbol" %in% names(dt)) dt[, symbol := trimws(symbol)]

  # Handle multiple possible column name variants defensively
  col_pick <- function(nm, alts) {
    cand <- intersect(alts, names(dt))
    if (length(cand)) dt[[cand[1L]]] else NA_real_
  }

  dt_out <- data.table::data.table(
    symbol  = dt[["symbol"]],
    refdate = as.Date(dt[["refdate"]]),
    open    = as.numeric(col_pick("open", c("open", "price.open", "preco_abertura"))),
    high    = as.numeric(col_pick("high", c("high", "price.high", "preco_maximo"))),
    low     = as.numeric(col_pick("low", c("low", "price.low", "preco_minimo"))),
    close   = as.numeric(col_pick("close", c("close", "price.close", "preco_ultimo"))),
    vol_fin = as.numeric(col_pick("vol_fin", c("volume", "financial_volume", "volume_total"))),
    qty     = as.numeric(col_pick(
      "qty",
      c("trade_quantity", "traded_contracts",
        "quantity", "number_trades", "quantidade_negociada")
    ))
  )

  # Filter out rows with missing keys
  dt_out <- dt_out[!is.na(symbol) & !is.na(refdate) & !is.na(close)]

  dt_out
}

# Basic classifier to tag asset type / investable based on ticker pattern.
af_classify_symbol <- function(symbol_chr) {
  sym <- toupper(trimws(symbol_chr))
  investable <- TRUE
  asset_type <- "OTHER"

  # Exclude obvious fractions/receipts/options patterns
  if (grepl("F$", sym) || grepl("[^A-Z0-9]", sym) || nchar(sym) > 8) {
    investable <- FALSE
  }

  # Rights/subscription receipts often end with 3+ digits not in our allowed set
  if (grepl("[0-9]{3,}$", sym) && !grepl("(11|32|33|34|35|36|39)$", sym)) {
    investable <- FALSE
  }

  # BDR patterns: usually end with 32-35 (and similar)
  if (grepl("(32|33|34|35|36|39)$", sym)) {
    asset_type <- "BDR"
  } else if (grepl("11$", sym)) {
    asset_type <- "ETF_FII"  # cannot easily separate here without extra metadata
  } else {
    asset_type <- "EQUITY"
  }

  if (!investable) asset_type <- "OTHER"

  list(asset_type = asset_type, active = as.integer(investable))
}

af_sync_b3 <- function(con = NULL,
                       years = NULL,
                       verbose = TRUE) {
  own_con <- is.null(con)
  if (own_con) {
    con <- af_db_connect()
    on.exit(af_db_disconnect(con), add = TRUE)
  }
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    stop("af_sync_b3: 'con' is not a valid DBI connection.")
  }
  af_attach_packages("data.table")
  
  if (is.null(years)) {
    # Check DB for max date
    max_date_str <- tryCatch({
      DBI::dbGetQuery(con, "SELECT MAX(refdate) as d FROM prices_raw")$d
    }, error = function(e) NA)
    
    current_year <- as.integer(format(Sys.Date(), "%Y"))
    
    if (is.na(max_date_str) || is.null(max_date_str)) {
      # Bootstrap: Start from 2015 (or config default) if empty
      years <- 2015:current_year
    } else {
      last_year <- as.integer(substr(max_date_str, 1, 4))
      years <- last_year:current_year
    }
  }
  
  for (y in years) {
    if (verbose) message("af_sync_b3: processing year ", y)
    dt <- af_fetch_cotahist_year(y, asset_filter = "all")

    if (nrow(dt) > 0) {
      if (verbose) message("  inserting ", nrow(dt), " rows...")
      af_db_insert_prices_raw(con, dt)
      # Seed assets_meta so splits sync can run later
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
