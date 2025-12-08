# v2/modules/00_core/R/utils.R

`%||%` <- function(x, y) if (!is.null(x)) x else y

af2_require <- function(pkgs) {
  pkgs <- unique(pkgs)
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) {
    stop("Missing packages in v2 environment: ",
         paste(missing, collapse = ", "),
         "\nInstall them explicitly to keep v2 deterministic.",
         call. = FALSE)
  }
  invisible(TRUE)
}

af2_assert_cols <- function(dt, cols, name = "object") {
  if (is.null(dt)) stop(name, " is NULL.", call. = FALSE)
  if (!is.data.frame(dt)) stop(name, " must be a data.frame/data.table.", call. = FALSE)
  miss <- setdiff(cols, names(dt))
  if (length(miss)) {
    stop(name, " missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

af2_assert_no_dupes <- function(dt, key_cols, name = "object") {
  af2_require("data.table")
  x <- data.table::as.data.table(dt)
  dup <- x[, .N, by = key_cols][N > 1L]
  if (nrow(dup)) {
    stop(name, " has duplicated keys on: ",
         paste(key_cols, collapse = ", "),
         "\nExample dup rows:\n",
         paste(utils::capture.output(print(utils::head(dup, 10))), collapse = "\n"),
         call. = FALSE)
  }
  invisible(TRUE)
}

af2_weekdays_only <- function(dates) {
  w <- weekdays(dates)
  !(w %in% c("Saturday", "Sunday", "sábado", "domingo"))
}

af2_make_bizdays_seq <- function(start_date, end_date, cal = "Brazil/B3") {
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  # Prefer the official calendar if available
  if (requireNamespace("bizdays", quietly = TRUE)) {
    out <- tryCatch(
      bizdays::bizseq(start_date, end_date, cal),
      error = function(e) NULL
    )
    if (!is.null(out) && length(out)) {
      return(as.Date(out))
    }
  }

  # Fallback: weekdays only
  d <- seq.Date(start_date, end_date, by = "day")
  d[af2_weekdays_only(d)]
}

