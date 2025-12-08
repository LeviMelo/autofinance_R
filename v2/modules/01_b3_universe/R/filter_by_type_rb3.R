# v2/modules/01_b3_universe/R/filter_by_type_rb3.R
# Apply rb3 class filters BEFORE collect

af2_b3_apply_type_filters <- function(df_lazy, include_types) {
  include_types <- af2_b3_validate_types(include_types)

  # Start with empty list of lazy datasets, bind after filtering
  out_list <- list()

  if ("equity" %in% include_types) {
    out_list[["equity"]] <- rb3::cotahist_filter_equity(df_lazy)
  }
  if ("fii" %in% include_types) {
    out_list[["fii"]] <- rb3::cotahist_filter_fii(df_lazy)
  }
  if ("etf" %in% include_types) {
    out_list[["etf"]] <- rb3::cotahist_filter_etf(df_lazy)
  }
  if ("bdr" %in% include_types) {
    out_list[["bdr"]] <- rb3::cotahist_filter_bdr(df_lazy)
  }

  if (!length(out_list)) {
    stop("No include_types provided after validation.")
  }

  out_list
}
