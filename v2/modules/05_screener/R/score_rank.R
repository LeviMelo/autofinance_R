# v2/modules/05_screener/R/score_rank.R

af2_score_and_rank <- function(metrics, score_weights) {
  af2_require("data.table")
  dt <- data.table::as.data.table(metrics)

  dt[, score := 0]

  for (nm in names(score_weights)) {
    if (!nm %in% names(dt)) next
    x <- dt[[nm]]
    if (all(is.na(x))) next
    mu <- mean(x, na.rm = TRUE)
    s  <- stats::sd(x, na.rm = TRUE)
    if (!is.finite(s) || s == 0) next

    z <- (x - mu) / s
    z[!is.finite(z) | is.na(z)] <- 0
    dt[, score := score + score_weights[[nm]] * z]
  }

  dt[, rank_overall := rank(-score, ties.method = "first")]
  if ("asset_type" %in% names(dt)) {
    dt[, rank_type := rank(-score, ties.method = "first"), by = asset_type]
  } else {
    dt[, rank_type := NA_integer_]
  }

  dt[order(rank_overall)]
}
