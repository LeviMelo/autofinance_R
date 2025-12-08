# v2/modules/01_b3_universe/R/validate_types.R

af2_b3_allowed_types <- c("equity", "fii", "etf", "bdr")

af2_b3_validate_types <- function(include_types) {
  include_types <- unique(tolower(include_types))
  bad <- setdiff(include_types, af2_b3_allowed_types)
  if (length(bad)) {
    stop(
      "Invalid include_types: ", paste(bad, collapse = ", "),
      ". Allowed: ", paste(af2_b3_allowed_types, collapse = ", ")
    )
  }
  include_types
}
