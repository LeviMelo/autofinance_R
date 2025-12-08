# v2/modules/01_b3_universe/R/unify_liquidity.R
# Convert raw liquidity into standardized 'turnover' and 'qty'

af2_b3_unify_liquidity <- function(dt_min) {
  dt <- data.table::as.data.table(dt_min)

  if (!all(c("symbol", "refdate", "close") %in% names(dt))) {
    stop("Liquidity unify requires at least symbol/refdate/close.")
  }

  # qty meaning in rb3 can be ambiguous across templates.
  # We treat 'qty_raw' as units proxy and keep it as qty.
  if (!("qty_raw" %in% names(dt))) dt[, qty_raw := NA_real_]
  if (!("vol_fin" %in% names(dt))) dt[, vol_fin := NA_real_]

  dt[, qty := qty_raw]

  # turnover in BRL:
  # Prefer provided financial volume when present, otherwise fallback to qty * close.
  dt[, turnover := data.table::fifelse(
    is.finite(vol_fin) & vol_fin > 0,
    vol_fin,
    data.table::fifelse(
      is.finite(qty) & qty > 0 & is.finite(close),
      qty * close,
      NA_real_
    )
  )]

  dt[, c("vol_fin", "qty_raw") := NULL]

  dt
}
