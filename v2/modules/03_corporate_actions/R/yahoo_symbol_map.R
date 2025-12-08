# v2/modules/03_corporate_actions/R/yahoo_symbol_map.R

af2_yahoo_symbol <- function(symbol, asset_type = NULL) {
  s <- toupper(trimws(as.character(symbol)))
  if (is.na(s) || !nzchar(s)) return(NA_character_)

  # If user already passed a Yahoo-style symbol, respect it
  if (grepl("\\.", s)) return(s)

  # Most B3 assets on Yahoo use .SA
  # This covers equities, FIIs, ETFs, many BDRs.
  paste0(s, ".SA")
}

af2_yahoo_symbol_vec <- function(symbols, asset_types = NULL) {
  if (is.null(asset_types)) {
    vapply(symbols, af2_yahoo_symbol, character(1))
  } else {
    mapply(af2_yahoo_symbol, symbols, asset_types, USE.NAMES = FALSE)
  }
}