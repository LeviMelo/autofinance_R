# ============================================================
# CVM ITR 2025 — AUDIT + FINANCIALS-ISH DERIVATION (v4)
#
# Designed for:
#   getwd() == "C:/Users/Galaxy/LEVI/Projetos R/autofinance_R"
# Requires:
#   playground/cvm_itr_2025_inspector.R  (your saved script)
#
# Outputs (in .GlobalEnv):
#   - audit_* tables (structure, instability, duplicates, YTD detection)
#   - fin_2025_panel_ytd  (best-available con/ind, as reported)
#   - fin_2025_panel_q    (quarterized if DRE is cumulative)
#   - sanity_checks       (accounting identities etc.)
#
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(tibble)
})

options(tibble.print_max = 25, tibble.print_min = 10, width = 140)

# -----------------------------
# 1) Load itr_list via inspector
# -----------------------------
inspector_path <- file.path("playground", "cvm_itr_2025_inspector.R")
if (!exists("itr_list")) {
  if (!file.exists(inspector_path)) stop("Missing inspector at: ", inspector_path)
  message("Sourcing inspector: ", inspector_path)
  source(inspector_path, encoding = "UTF-8")
}
stopifnot(is.list(itr_list), length(itr_list) > 0)

# -----------------------------
# 2) Helpers: parsing + scale
# -----------------------------
cvm_parse_vl <- function(x) {
  x <- str_trim(as.character(x))
  x[x == "" | x == "NA"] <- NA_character_
  
  has_comma <- str_detect(x, ",")
  has_dot   <- str_detect(x, "\\.")
  both      <- has_comma & has_dot
  
  y <- x
  y[both]      <- str_replace_all(y[both], "\\.", "")  # thousands
  y[has_comma] <- str_replace_all(y[has_comma], ",", ".")
  suppressWarnings(as.numeric(y))
}

cvm_scale_mult <- function(x) {
  z <- toupper(str_trim(as.character(x)))
  dplyr::case_when(
    z %in% c("UN", "UNIDADE") ~ 1,
    z %in% c("MIL") ~ 1e3,
    z %in% c("MILHAO", "MILHÃO", "MILHOES", "MILHÕES") ~ 1e6,
    z %in% c("BILHAO", "BILHÃO", "BILHOES", "BILHÕES") ~ 1e9,
    TRUE ~ 1
  )
}

# Note: CD_CONTA is numeric in your tibbles; keep a clean character key.
cvm_cd_chr <- function(x) {
  y <- format(x, scientific = FALSE, trim = TRUE)
  y <- sub("\\.0+$", "", y)
  y
}

cvm_get <- function(stmt = c("DRE","BPA","BPP","DFC_MI","DFC_MD"), scope = c("con","ind"), year = 2025) {
  stmt  <- match.arg(stmt)
  scope <- match.arg(scope)
  pattern <- sprintf("^itr_cia_aberta_%s_%s_%d\\.csv$", stmt, scope, year)
  nm <- grep(pattern, names(itr_list), value = TRUE)
  if (length(nm) != 1) stop("Expected 1 match for: ", pattern, " | got: ", paste(nm, collapse=", "))
  itr_list[[nm]]
}

cvm_prepare_long <- function(df, only_ultimo = TRUE, only_fixed = TRUE, apply_scale = TRUE) {
  out <- df
  if (only_ultimo && "ORDEM_EXERC" %in% names(out)) out <- out %>% filter(ORDEM_EXERC == "ÚLTIMO")
  if (only_fixed && "ST_CONTA_FIXA" %in% names(out)) out <- out %>% filter(ST_CONTA_FIXA == "S")
  
  out <- out %>%
    mutate(
      CD_CONTA_CHR = if ("CD_CONTA" %in% names(.)) cvm_cd_chr(CD_CONTA) else NA_character_,
      VL_NUM       = if ("VL_CONTA" %in% names(.)) cvm_parse_vl(VL_CONTA) else NA_real_
    )
  
  if (apply_scale && "ESCALA_MOEDA" %in% names(out)) {
    out <- out %>% mutate(VL = VL_NUM * cvm_scale_mult(ESCALA_MOEDA))
  } else {
    out <- out %>% mutate(VL = VL_NUM)
  }
  out
}

# -----------------------------
# 3) AUDIT: inventory + coverage
# -----------------------------
inventory <- tibble(
  file = names(itr_list),
  n_rows = map_int(itr_list, nrow),
  n_cols = map_int(itr_list, ncol)
) %>% arrange(file)

cat("\n=== INVENTORY (itr_list) ===\n")
print(inventory, n = nrow(inventory))

# Index coverage
idx <- itr_list[["itr_cia_aberta_2025.csv"]]
audit_index <- list(
  n_companies_index = n_distinct(idx$CD_CVM),
  dt_range_index    = paste(range(idx$DT_REFER), collapse = " to ")
)
cat("\n=== INDEX COVERAGE ===\n")
print(audit_index)

# Coverage for main statements
stmt_names <- c(
  "itr_cia_aberta_DRE_con_2025.csv",
  "itr_cia_aberta_DRE_ind_2025.csv",
  "itr_cia_aberta_BPA_con_2025.csv",
  "itr_cia_aberta_BPA_ind_2025.csv",
  "itr_cia_aberta_BPP_con_2025.csv",
  "itr_cia_aberta_BPP_ind_2025.csv"
)

audit_coverage <- map_dfr(stmt_names, function(nm) {
  df <- itr_list[[nm]]
  tibble(
    table = nm,
    n_rows = nrow(df),
    n_companies = n_distinct(df$CD_CVM),
    dt_min = min(df$DT_REFER),
    dt_max = max(df$DT_REFER),
    n_ultimo = if ("ORDEM_EXERC" %in% names(df)) sum(df$ORDEM_EXERC == "ÚLTIMO") else NA_integer_,
    n_fixedS = if ("ST_CONTA_FIXA" %in% names(df)) sum(df$ST_CONTA_FIXA == "S") else NA_integer_
  )
})

cat("\n=== COVERAGE BY TABLE ===\n")
print(audit_coverage)

# -----------------------------
# 4) AUDIT: code<->label instability
# -----------------------------
instability_report <- function(df_prepped, label, top_n = 20) {
  # how many different DS_CONTA per CD_CONTA_CHR?
  by_code <- df_prepped %>%
    filter(!is.na(CD_CONTA_CHR), !is.na(DS_CONTA)) %>%
    group_by(CD_CONTA_CHR) %>%
    summarise(
      n_labels = n_distinct(DS_CONTA),
      n_rows = n(),
      example_labels = paste(head(sort(unique(DS_CONTA)), 4), collapse=" | "),
      .groups = "drop"
    ) %>%
    arrange(desc(n_labels), desc(n_rows))
  
  # how many different CD_CONTA_CHR per DS_CONTA?
  by_label <- df_prepped %>%
    filter(!is.na(CD_CONTA_CHR), !is.na(DS_CONTA)) %>%
    group_by(DS_CONTA) %>%
    summarise(
      n_codes = n_distinct(CD_CONTA_CHR),
      n_rows = n(),
      example_codes = paste(head(sort(unique(CD_CONTA_CHR)), 6), collapse=", "),
      .groups = "drop"
    ) %>%
    arrange(desc(n_codes), desc(n_rows))
  
  cat("\n============================\n")
  cat("INSTABILITY:", label, "\n")
  cat("============================\n")
  cat("\nTop codes with many labels (CD_CONTA -> DS_CONTA):\n")
  print(head(by_code, top_n), n = top_n)
  cat("\nTop labels with many codes (DS_CONTA -> CD_CONTA):\n")
  print(head(by_label, top_n), n = top_n)
  
  list(by_code = by_code, by_label = by_label)
}

# Build prepared long tables for con+ind (so we can compare)
dre_con <- cvm_prepare_long(cvm_get("DRE","con",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)
dre_ind <- cvm_prepare_long(cvm_get("DRE","ind",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)
bpa_con <- cvm_prepare_long(cvm_get("BPA","con",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)
bpa_ind <- cvm_prepare_long(cvm_get("BPA","ind",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)
bpp_con <- cvm_prepare_long(cvm_get("BPP","con",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)
bpp_ind <- cvm_prepare_long(cvm_get("BPP","ind",2025), only_ultimo=TRUE, only_fixed=TRUE, apply_scale=TRUE)

audit_instability <- list(
  dre_con = instability_report(dre_con, "DRE con (fixed=S, ultimo)"),
  bpa_con = instability_report(bpa_con, "BPA con (fixed=S, ultimo)"),
  bpp_con = instability_report(bpp_con, "BPP con (fixed=S, ultimo)")
)

# -----------------------------
# 5) AUDIT: is DRE cumulative (YTD) or quarter?
# -----------------------------
# If DT_INI_EXERC exists and for Q2/Q3 it's often 01-01, it's cumulative YTD.
audit_dre_periods <- function(df_prepped, label) {
  if (!("DT_INI_EXERC" %in% names(df_prepped)) || !("DT_FIM_EXERC" %in% names(df_prepped))) {
    cat("\nNo DT_INI_EXERC/DT_FIM_EXERC in ", label, "\n")
    return(NULL)
  }
  tab <- df_prepped %>%
    distinct(CD_CVM, DT_REFER, DT_INI_EXERC, DT_FIM_EXERC) %>%
    count(DT_REFER, DT_INI_EXERC, DT_FIM_EXERC, sort = TRUE)
  
  cat("\n============================\n")
  cat("DRE PERIOD STRUCTURE:", label, "\n")
  cat("============================\n")
  cat("Top (DT_REFER, DT_INI_EXERC, DT_FIM_EXERC) combinations:\n")
  print(head(tab, 20), n = 20)
  
  # YTD heuristic: for DT_REFER after March, DT_INI_EXERC frequently Jan 1 of same year
  ytd_flag <- tab %>%
    mutate(is_ytd_like = (month(DT_INI_EXERC) == 1 & day(DT_INI_EXERC) == 1)) %>%
    group_by(DT_REFER) %>%
    summarise(
      share_ytd_like = sum(n[is_ytd_like], na.rm=TRUE)/sum(n),
      .groups="drop"
    )
  
  cat("\nShare of 'starts Jan 1' by DT_REFER (YTD-likeness):\n")
  print(ytd_flag)
  
  list(combos = tab, ytd_like = ytd_flag)
}

# month/day without lubridate dependency:
month <- function(d) as.integer(format(d, "%m"))
day   <- function(d) as.integer(format(d, "%d"))

audit_dre_con_periods <- audit_dre_periods(dre_con, "DRE con")
audit_dre_ind_periods <- audit_dre_periods(dre_ind, "DRE ind")

# -----------------------------
# 6) Build "best available" statement (con preferred, else ind)
# -----------------------------
best_available <- function(con, ind) {
  key_cols <- c("CD_CVM","DT_REFER","CD_CONTA_CHR","DS_CONTA","DT_INI_EXERC","DT_FIM_EXERC")
  key_cols <- intersect(key_cols, names(con))  # keep only those that exist
  
  con_key <- con %>%
    dplyr::select(dplyr::all_of(key_cols)) %>%
    dplyr::distinct()
  
  ind_missing <- ind %>%
    dplyr::anti_join(con_key, by = key_cols)
  
  dplyr::bind_rows(con, ind_missing)
}

dre_best <- best_available(dre_con, dre_ind)
bpa_best <- best_available(bpa_con, bpa_ind)
bpp_best <- best_available(bpp_con, bpp_ind)

cat("\n=== BEST-AVAILABLE COVERAGE (con preferred, else ind) ===\n")
cat("DRE companies:", n_distinct(dre_best$CD_CVM), "\n")
cat("BPA companies:", n_distinct(bpa_best$CD_CVM), "\n")
cat("BPP companies:", n_distinct(bpp_best$CD_CVM), "\n")

# -----------------------------
# 7) Feature extraction WITHOUT collisions
#    (feature defined by DS_CONTA regex + choose most summary-like line)
# -----------------------------
extract_feature <- function(df_prepped, feature, pattern, exclude_pattern = NULL) {
  x <- df_prepped %>%
    dplyr::filter(!is.na(DS_CONTA), stringr::str_detect(DS_CONTA, pattern))
  
  if (!is.null(exclude_pattern)) {
    x <- x %>% dplyr::filter(!stringr::str_detect(DS_CONTA, exclude_pattern))
  }
  
  # symbols for dynamic names (robust)
  f    <- rlang::sym(feature)
  f_cd <- rlang::sym(paste0(feature, "__CD"))
  f_ds <- rlang::sym(paste0(feature, "__DS"))
  
  picked <- x %>%
    dplyr::mutate(code_len = nchar(CD_CONTA_CHR)) %>%
    dplyr::arrange(code_len, CD_CONTA_CHR) %>%
    dplyr::group_by(CD_CVM, DENOM_CIA, DT_REFER) %>%
    dplyr::summarise(
      !!f    := dplyr::first(VL),
      !!f_cd := dplyr::first(CD_CONTA_CHR),
      !!f_ds := dplyr::first(DS_CONTA),
      .groups = "drop"
    )
  
  audit <- picked %>%
    dplyr::count(!!f_cd, !!f_ds, sort = TRUE) %>%
    dplyr::rename(CD_CONTA_CHR = 1, DS_CONTA = 2)
  
  list(data = picked, audit = audit)
}


# Define features carefully:
# - net income: many name variants; exclude "por ação"
# - revenue goods vs revenue financial are different features and MUST NOT share the same extraction rule
dre_feat_specs <- tribble(
  ~feature,          ~pattern,                                               ~exclude,
  "net_income",      "(?i)^Lucro\\s+ou\\s+Preju[ií]zo.*Per[ií]odo",          "(?i)por\\s+a[cç][aã]o",
  "profit_before_tax","(?i)^Resultado\\s+antes\\s+dos\\s+Tributos\\s+sobre\\s+o\\s+Lucro", NULL,
  "revenue_goods",   "(?i)^Receita\\s+de\\s+Venda\\s+de\\s+Bens",            NULL,
  "revenue_fin",     "(?i)^Receitas\\s+de\\s+Intermedia[cç](ã|a)o\\s+Financeira", NULL
)

bpa_feat_specs <- tribble(
  ~feature,           ~pattern,                       ~exclude,
  "assets_total",     "(?i)^Ativo Total$",            NULL,
  "assets_current",   "(?i)^Ativo Circulante$",       NULL,
  "assets_noncurrent","(?i)^Ativo N(ã|a)o Circulante$",NULL
)

bpp_feat_specs <- tribble(
  ~feature,            ~pattern,                          ~exclude,
  "liab_total",        "(?i)^Passivo Total$",              NULL,
  "liab_current",      "(?i)^Passivo Circulante$",         NULL,
  "liab_noncurrent",   "(?i)^Passivo N(ã|a)o Circulante$", NULL,
  "equity_total",      "(?i)^Patrim(ô|o)nio L(í|i)quido",   NULL
)

# Extract DRE features
dre_extracts <- pmap(dre_feat_specs, function(feature, pattern, exclude) {
  extract_feature(dre_best, feature, pattern, exclude)
})
names(dre_extracts) <- dre_feat_specs$feature

# Extract BPA features
bpa_extracts <- pmap(bpa_feat_specs, function(feature, pattern, exclude) {
  extract_feature(bpa_best, feature, pattern, exclude)
})
names(bpa_extracts) <- bpa_feat_specs$feature

# Extract BPP features
bpp_extracts <- pmap(bpp_feat_specs, function(feature, pattern, exclude) {
  extract_feature(bpp_best, feature, pattern, exclude)
})
names(bpp_extracts) <- bpp_feat_specs$feature

# Merge picked dataframes (drop the audit columns later if you want)
merge_extracts <- function(extract_list) {
  reduce(map(extract_list, "data"), full_join, by = c("CD_CVM","DENOM_CIA","DT_REFER"))
}

dre_feat <- merge_extracts(dre_extracts)
bpa_feat <- merge_extracts(bpa_extracts)
bpp_feat <- merge_extracts(bpp_extracts)

fin_2025_panel_ytd <- bpa_feat %>%
  full_join(bpp_feat, by = c("CD_CVM","DENOM_CIA","DT_REFER")) %>%
  full_join(dre_feat, by = c("CD_CVM","DENOM_CIA","DT_REFER")) %>%
  arrange(CD_CVM, DT_REFER) %>%
  mutate(
    # define a usable "primary revenue" (banks vs non-banks)
    revenue_primary = dplyr::coalesce(revenue_goods, revenue_fin)
  )

cat("\n=== FIN PANEL (YTD/as-reported) ===\n")
cat("Rows:", nrow(fin_2025_panel_ytd), " | Companies:", n_distinct(fin_2025_panel_ytd$CD_CVM), "\n")
cat("Columns:", ncol(fin_2025_panel_ytd), "\n")

# -----------------------------
# 8) Quarterize DRE features if needed
# -----------------------------
# We compute quarter deltas within each company for DRE-like flow features:
flow_vars <- c("net_income","profit_before_tax","revenue_goods","revenue_fin","revenue_primary")

quarterize <- function(df, vars) {
  df %>%
    group_by(CD_CVM) %>%
    arrange(DT_REFER) %>%
    mutate(across(all_of(vars), ~ . - dplyr::lag(.), .names = "{.col}_q")) %>%
    ungroup()
}

fin_2025_panel_q <- quarterize(fin_2025_panel_ytd, flow_vars)

# -----------------------------
# 9) Sanity checks (accounting identities)
# -----------------------------
sanity_checks <- fin_2025_panel_ytd %>%
  mutate(
    # Assets ≈ Liabilities+Equity (depending on how equity_total was picked)
    assets_minus_liab = assets_total - liab_total,
    assets_minus_liab_eq = assets_total - (liab_current + liab_noncurrent + equity_total)
  ) %>%
  summarise(
    n = n(),
    mean_abs_assets_minus_liab = mean(abs(assets_minus_liab), na.rm=TRUE),
    p95_abs_assets_minus_liab  = quantile(abs(assets_minus_liab), 0.95, na.rm=TRUE),
    mean_abs_assets_minus_liab_eq = mean(abs(assets_minus_liab_eq), na.rm=TRUE),
    p95_abs_assets_minus_liab_eq  = quantile(abs(assets_minus_liab_eq), 0.95, na.rm=TRUE)
  )

cat("\n=== SANITY CHECKS ===\n")
print(sanity_checks)

# -----------------------------
# 10) Print audits (compact, not spam)
# -----------------------------
cat("\n=== FEATURE PICK AUDITS (top 10 picked code/label per feature) ===\n")
print(head(dre_extracts$net_income$audit, 10), n = 10)
print(head(dre_extracts$profit_before_tax$audit, 10), n = 10)
print(head(dre_extracts$revenue_goods$audit, 10), n = 10)
print(head(dre_extracts$revenue_fin$audit, 10), n = 10)

cat("\n=== EXAMPLE COMPANY VIEW (Banco do Brasil 001023, if present) ===\n")
example_cvm <- "001023"
ex <- fin_2025_panel_ytd %>% filter(CD_CVM == example_cvm)
if (nrow(ex) > 0) {
  print(ex, n = 10)
} else {
  cat("001023 not present in best-available panel.\n")
}

cat("\nDONE.\n")
cat("Objects created:\n")
cat("- inventory, audit_coverage, audit_instability, audit_dre_con_periods, audit_dre_ind_periods\n")
cat("- fin_2025_panel_ytd  (features as reported)\n")
cat("- fin_2025_panel_q    (same + *_q deltas)\n")
cat("- sanity_checks\n")
