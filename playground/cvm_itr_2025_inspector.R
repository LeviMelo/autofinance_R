# ============================
# CVM ITR 2025 - FULL INSPECTOR
# Downloads, unzips, reads ALL CSVs in the ITR 2025 bundle
# and prints a compact overview + previews.
# ============================

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tibble)
})

# ---- 1) Source URL ----
itr_2025_url <- "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2025.zip"

# ---- 2) Working temp paths ----
tf <- tempfile(fileext = ".zip")
exdir <- tempfile("cvm_itr_2025_")
dir.create(exdir)

cat("\n[1/4] Downloading ITR 2025...\n")
download.file(itr_2025_url, tf, mode = "wb", quiet = FALSE)

cat("\n[2/4] Unzipping...\n")
unzip(tf, exdir = exdir)

# ---- 3) Locate CSV files ----
files <- list.files(exdir, full.names = TRUE, pattern = "\\.csv$", ignore.case = TRUE)

if (length(files) == 0) stop("No CSV files found after unzip. Something is off.")

cat("\nFound CSV files:\n")
print(basename(files))

# ---- 4) Robust reader for CVM semicolon CSVs ----
read_cvm_csv <- function(path) {
  # CVM files are typically:
  # - delimiter: ;
  # - encoding: Latin1 (sometimes UTF-8)
  # - decimal: ,  grouping: .
  # We'll try a safe cascade.
  
  # Quick header peek (useful for debugging)
  header <- tryCatch(readLines(path, n = 1, warn = FALSE), error = function(e) NA_character_)
  
  # Primary locale for Brazil-style numbers
  loc_l1 <- locale(encoding = "Latin1", decimal_mark = ",", grouping_mark = ".")
  loc_u8 <- locale(encoding = "UTF-8",  decimal_mark = ",", grouping_mark = ".")
  
  # Try read_csv2 first (built for ; + decimal comma)
  out <- tryCatch(
    read_csv2(path, locale = loc_l1, show_col_types = FALSE, progress = FALSE),
    error = function(e) NULL
  )
  
  if (is.null(out)) {
    out <- tryCatch(
      read_csv2(path, locale = loc_u8, show_col_types = FALSE, progress = FALSE),
      error = function(e) NULL
    )
  }
  
  # Fallback to explicit delim
  if (is.null(out)) {
    out <- tryCatch(
      read_delim(path, delim = ";", locale = loc_l1, show_col_types = FALSE, progress = FALSE),
      error = function(e) NULL
    )
  }
  
  if (is.null(out)) {
    out <- tryCatch(
      read_delim(path, delim = ";", locale = loc_u8, show_col_types = FALSE, progress = FALSE),
      error = function(e) NULL
    )
  }
  
  # Last-resort: read everything as character to avoid coercion failures
  if (is.null(out)) {
    out <- tryCatch(
      read_delim(
        path, delim = ";",
        col_types = cols(.default = col_character()),
        locale = loc_l1,
        show_col_types = FALSE, progress = FALSE
      ),
      error = function(e) NULL
    )
  }
  
  if (is.null(out)) {
    stop("Failed to read file: ", basename(path), "\nHeader seen: ", header)
  }
  
  attr(out, "cvm_header") <- header
  out
}

cat("\n[3/4] Reading all CSVs (this may take a bit)...\n")

# Read all into a named list
itr_list <- set_names(files, nm = basename(files)) %>%
  map(function(f) {
    cat("  - Reading:", basename(f), "\n")
    read_cvm_csv(f)
  })

# ---- 5) Build inventory table ----
inventory <- imap_dfr(itr_list, function(df, nm) {
  tibble(
    file = nm,
    n_rows = nrow(df),
    n_cols = ncol(df)
  )
}) %>%
  arrange(file)

cat("\n[4/4] Inventory (rows/cols):\n")
print(inventory, n = Inf)

# ---- 6) Print a compact preview of each dataset ----
cat("\n============================\n")
cat("PREVIEWS (first columns + sample rows)\n")
cat("============================\n\n")

walk(names(itr_list), function(nm) {
  df <- itr_list[[nm]]
  
  cat("FILE:", nm, "\n")
  cat("Rows:", nrow(df), " | Cols:", ncol(df), "\n")
  
  # Show first 25 column names max, to avoid console spam
  cn <- names(df)
  show_cn <- if (length(cn) > 25) c(cn[1:25], "...") else cn
  cat("Columns:", paste(show_cn, collapse = " | "), "\n")
  
  # Show a few lines
  print(head(df, 3))
  
  # If there are parsing problems recorded by readr, show top 10
  pr <- tryCatch(problems(df), error = function(e) NULL)
  if (!is.null(pr) && nrow(pr) > 0) {
    cat("\nParsing issues (top 10):\n")
    print(head(pr, 10))
  }
  
  cat("\n----------------------------\n\n")
})

# ---- 7) Optional: attach to global environment for interactive browsing ----
# This makes objects like:
#   itr_cia_aberta_2025
#   itr_cia_aberta_BPA_con_2025
# etc.
#
# Comment out if you dislike clutter.
cat("Attaching data frames to .GlobalEnv...\n")
iwalk(itr_list, function(df, nm) {
  obj_name <- nm %>%
    str_replace("\\.csv$", "") %>%
    make.names()
  
  assign(obj_name, df, envir = .GlobalEnv)
})

# ---- 8) Optional: save everything as a single RDS ----
# Useful for caching and fast reload
rds_path <- file.path(tempdir(), "itr_2025_all_tables.rds")
saveRDS(itr_list, rds_path)
cat("\nSaved all tables as RDS at:\n", rds_path, "\n")

cat("\nDONE.\n")
cat("You now have:\n")
cat("- 'inventory' summary table\n")
cat("- 'itr_list' named list of all CSVs\n")
cat("- individual data frames attached in your global env\n")
cat("- an RDS cache for quick reload\n")
