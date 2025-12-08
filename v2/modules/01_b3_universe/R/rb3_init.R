# v2/modules/01_b3_universe/R/rb3_init.R
# Deterministic rb3 cache bootstrap for v2

af2_b3_init_rb3 <- function(cfg = NULL, verbose = TRUE) {
  cfg <- cfg %||% af2_get_config()

  cache_dir <- file.path(cfg$cache_dir, "rb3")
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  options(rb3.cachedir = normalizePath(cache_dir, winslash = "/", mustWork = FALSE))

  if (verbose) {
    af2_log("AF2_B3:", "rb3.cachedir = ", getOption("rb3.cachedir"))
  }

  # Bootstrap templates DB (safe no-op if already initialized)
  tryCatch({
    rb3::rb3_bootstrap()
    if (verbose) af2_log("AF2_B3:", "rb3_bootstrap OK.")
  }, error = function(e) {
    # We don't hard fail here because rb3 can still read cached data;
    # but we DO log clearly.
    af2_log("AF2_B3:", "rb3_bootstrap warning: ", conditionMessage(e))
  })

  invisible(TRUE)
}
