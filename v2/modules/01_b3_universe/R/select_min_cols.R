# v2/modules/01_b3_universe/R/select_min_cols.R
# Normalize column names across potential rb3 schema differences

af2_b3_pick_col <- function(dt, alternatives) {
  cand <- intersect(alternatives, names(dt))
  if (length(cand)) cand[1L] else NA_character_
}

af2_b3_select_min_cols <- function(dt) {
  # dt is a collected data.frame/data.table from rb3

  # Defensive mapping of OHLC + liquidity
  col_symbol <- af2_b3_pick_col(dt, c("symbol", "ticker"))
  col_ref    <- af2_b3_pick_col(dt, c("refdate", "date"))
  col_open   <- af2_b3_pick_col(dt, c("open", "price.open", "preco_abertura"))
  col_high   <- af2_b3_pick_col(dt, c("high", "price.high", "preco_maximo"))
  col_low    <- af2_b3_pick_col(dt, c("low", "price.low", "preco_minimo"))
  col_close  <- af2_b3_pick_col(dt, c("close", "price.close", "preco_ultimo"))

  # Liquidity candidates:
  # - financial_volume / volume in BRL
  # - trade_quantity in units
  col_volfin <- af2_b3_pick_col(dt, c("financial_volume", "volume", "vol_fin"))
  col_qty <- af2_b3_pick_col(dt, c("trade_quantity", "quantity", "qty"))

  missing_core <- c(
    symbol = col_symbol,
    refdate = col_ref,
    close = col_close
  )
  if (any(is.na(missing_core))) {
    stop(
      "COTAHIST schema missing core columns. Found names: ",
      paste(names(dt), collapse = ", ")
    )
  }

  out <- data.table::as.data.table(dt)

  out_min <- data.table::data.table(
    symbol  = trimws(as.character(out[[col_symbol]])),
    refdate = as.Date(out[[col_ref]]),
    open    = if (!is.na(col_open))  as.numeric(out[[col_open]])  else NA_real_,
    high    = if (!is.na(col_high))  as.numeric(out[[col_high]])  else NA_real_,
    low     = if (!is.na(col_low))   as.numeric(out[[col_low]])   else NA_real_,
    close   = as.numeric(out[[col_close]]),
    vol_fin = if (!is.na(col_volfin)) as.numeric(out[[col_volfin]]) else NA_real_,
    qty_raw = if (!is.na(col_qty))    as.numeric(out[[col_qty]])    else NA_real_
  )

  out_min
}
