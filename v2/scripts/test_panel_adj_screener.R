# ============================================================
# AF2 clean boot + build panel_adj + run screener (features)
# ============================================================

rm(list = ls(all.names = TRUE)); gc()
options(stringsAsFactors = FALSE)
suppressPackageStartupMessages({
  library(data.table)
})

# ---- 0) Set project root (IMPORTANT) ----
# Run this from your project root where v2/ exists.
proj_root <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!dir.exists(file.path(proj_root, "v2", "modules"))) {
  stop(
    paste0(
      "You are NOT at the project root.\n",
      "Current getwd(): ", proj_root, "\n",
      "Expected to find: ", file.path(proj_root, "v2/modules"), "\n\n",
      "Fix: setwd('.../your_repo_root') and re-run."
    ),
    call. = FALSE
  )
}

# ---- 1) Source ALL modules in order ----
mod_root <- file.path(proj_root, "v2", "modules")
r_files  <- list.files(mod_root, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)

# Order by module folder prefix (e.g., 00_core, 01_db, 04_adjuster, 05_screener), then filename
mod_name <- basename(dirname(r_files))                 # ".../05_screener/R" -> "R" (not good)
# better: module dir is two levels up: v2/modules/<MODULE>/R/<file>.R
mod_dir  <- basename(dirname(dirname(r_files)))        # gets <MODULE>
mod_num  <- suppressWarnings(as.integer(sub("^([0-9]+).*", "\\1", mod_dir)))
ord      <- order(mod_num, mod_dir, basename(r_files), na.last = TRUE)

for (f in r_files[ord]) {
  source(f, local = FALSE)
}

# ---- 2) Make panel_adj (try: cached RDS first, then builder functions) ----
# Common cache guesses (adjust if your repo uses different paths)
candidate_rds <- c(
  file.path(proj_root, "panel_adj.rds"),
  file.path(proj_root, "v2", "panel_adj.rds"),
  file.path(proj_root, "v2", "cache", "panel_adj.rds"),
  file.path(proj_root, "cache", "panel_adj.rds"),
  file.path(proj_root, "output", "panel_adj.rds"),
  file.path(proj_root, "v2", "output", "panel_adj.rds")
)

panel_adj <- NULL

rds_hit <- candidate_rds[file.exists(candidate_rds)][1]
if (!is.na(rds_hit) && nzchar(rds_hit)) {
  message("Loading panel_adj from RDS: ", rds_hit)
  panel_adj <- readRDS(rds_hit)
}

# If no RDS, try to call a builder function (auto-detect by name)
if (is.null(panel_adj)) {
  # Pull config if available (many builders need it)
  cfg <- NULL
  if (exists("af2_get_config", mode = "function")) {
    cfg <- tryCatch(af2_get_config(), error = function(e) NULL)
  }

  # Candidate builders (most likely first)
  candidates <- c(
    "af2_build_panel_adj_selective",
    "af2_build_panel_adj",
    "af2_get_panel_adj",
    "af2_load_panel_adj",
    "af2_panel_adj",
    "af2_make_panel_adj",
    "af2_build_panel",
    "af2_build_panel_adj_from_db",
    "af2_panel_from_db",
    "af2_get_panel"
  )

  try_build <- function(fname) {
    if (!exists(fname, inherits = TRUE)) return(list(val = NULL, err = "not found"))
    fn <- get(fname, inherits = TRUE)
    if (!is.function(fn)) return(list(val = NULL, err = "exists but not a function"))
  
    fml <- names(formals(fn))
    args <- list()
  
    if ("config" %in% fml) args$config <- NULL
    if ("cfg"    %in% fml) args$cfg    <- cfg
    if ("start_date" %in% fml && !is.null(cfg) && !is.null(cfg$start_date)) args$start_date <- cfg$start_date
    if ("end_date"   %in% fml && !is.null(cfg) && !is.null(cfg$end_date))   args$end_date   <- cfg$end_date
    if ("symbols" %in% fml) args$symbols <- NULL
    if ("asset_types" %in% fml) args$asset_types <- NULL
    if ("years" %in% fml && !is.null(cfg) && !is.null(cfg$years)) args$years <- cfg$years
  
    out <- tryCatch(
      do.call(fn, args),
      error = function(e) e
    )
  
    if (inherits(out, "error")) return(list(val = NULL, err = conditionMessage(out)))
    list(val = out, err = NULL)
  }


  errs <- list()
  
  for (nm in candidates) {
    r <- try_build(nm)
    if (!is.null(r$val)) {
      panel_adj <- r$val
      message("Built panel_adj via: ", nm)
      break
    } else {
      errs[[nm]] <- r$err
    }
  }
  
  if (is.null(panel_adj)) {
    message("Candidate builder failures:")
    print(errs)
  
    funs <- ls(pattern = "^af2_.*panel.*adj|^af2_.*adj.*panel|^af2_.*panel", all.names = TRUE)
    funs <- funs[vapply(funs, function(nm) is.function(get(nm, inherits = TRUE)), logical(1))]
  
    stop(
      paste0(
        "Could not auto-create panel_adj.\n\n",
        "Functions that look relevant in your session:\n  - ",
        paste(sort(unique(funs)), collapse = "\n  - "),
        "\n"
      ),
      call. = FALSE
    )
  }
}

# ---- 3) Run screener in features mode ----
res <- af2_run_screener(panel_adj, return = "features")

# ---- 4) Quick sanity prints ----
cat("\n--- features columns ---\n")
print(names(res$features))

cat("\n--- first rows (features) ---\n")
print(res$features[1:min(5L, nrow(res$features))])

cat("\n--- coverage summary ---\n")
if ("coverage" %in% names(res$features)) print(summary(res$features$coverage))

invisible(res)
