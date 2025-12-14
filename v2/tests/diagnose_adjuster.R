# ======================================================================
# diagnose_adjuster.R
# Build panel_adj_result (best-effort) and run split/adjustment diagnostics
# ======================================================================

options(stringsAsFactors = FALSE)

cat("\n=============================\n")
cat(" Autofinance v2 diagnostics\n")
cat("=============================\n\n")

# ----------------------------
# 0) Small helpers
# ----------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

stopf <- function(fmt, ...) stop(sprintf(fmt, ...), call. = FALSE)
msg  <- function(fmt, ...) cat(sprintf(fmt, ...), "\n")

need_pkg <- function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
        stopf("Missing package '%s'. Install it first: install.packages('%s')", pkg, pkg)
    }
}

need_pkg("data.table")
DT <- data.table::as.data.table

has_cols <- function(dt, cols, name = "object") {
    miss <- setdiff(cols, names(dt))
    if (length(miss)) stopf("%s is missing columns: %s", name, paste(miss, collapse = ", "))
    invisible(TRUE)
}

as_idate <- function(x) {
    if (inherits(x, "IDate")) return(x)
    if (inherits(x, "Date")) return(data.table::as.IDate(x))
    if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) return(data.table::as.IDate(as.Date(x)))
    if (is.character(x)) return(data.table::as.IDate(as.Date(x)))
    return(data.table::as.IDate(as.Date(x)))
}

maybe_find_first <- function(paths) {
    for (p in paths) if (file.exists(p)) return(p)
    return(NULL)
}

# ----------------------------
# 1) Locate project root / source v2 code
# ----------------------------
root <- getwd()

if (!dir.exists(file.path(root, "v2"))) {
    msg("Working directory: %s", root)
    stopf("I don't see a ./v2 directory here. Setwd() to your project root, then re-run.")
}

source_dir <- function(path) {
    if (!dir.exists(path)) return(invisible(FALSE))
    files <- list.files(path, pattern = "\\.R$", full.names = TRUE, recursive = FALSE)
    files <- sort(files)
    for (f in files) source(f, local = FALSE)
    invisible(TRUE)
}

source_v2_modules <- function() {
    modules_root <- file.path(root, "v2", "modules")
    if (!dir.exists(modules_root)) stopf("Expected directory not found: %s", modules_root)
    mods <- list.dirs(modules_root, full.names = TRUE, recursive = FALSE)
    mods <- sort(mods)
    for (m in mods) {
        rdir <- file.path(m, "R")
        if (dir.exists(rdir)) {
            msg("Sourcing module: %s", basename(m))
            source_dir(rdir)
        }
    }
    invisible(TRUE)
}

source_v2_modules()

# ----------------------------
# 2) Acquire inputs (universe_raw, corp_actions)
# ----------------------------
load_or_build_inputs <- function() {
    universe_raw <- NULL
    corp_actions <- NULL
    
    if (exists("universe_raw", envir = .GlobalEnv, inherits = TRUE)) {
        universe_raw <- get("universe_raw", envir = .GlobalEnv, inherits = TRUE)
        msg("\nUsing existing object: universe_raw")
    }
    
    if (exists("corp_actions", envir = .GlobalEnv, inherits = TRUE)) {
        corp_actions <- get("corp_actions", envir = .GlobalEnv, inherits = TRUE)
        msg("Using existing object: corp_actions")
    }
    
    u_guess <- maybe_find_first(c(
        file.path(root, "v2/data/cache/universe_raw_latest.rds"),
        file.path(root, "v2/data/cache/universe_raw.rds")
    ))
    if (is.null(universe_raw) && !is.null(u_guess)) {
        universe_raw <- readRDS(u_guess)
        msg("Loaded universe_raw from: %s", u_guess)
    }
    
    # Check if selective builder exists (to skip global fetch)
    has_selective <- exists("af2_build_panel_adj_selective", envir = .GlobalEnv)
    
    if (is.null(corp_actions) && !has_selective) {
        c_guess <- maybe_find_first(file.path(root, "v2/data/cache/corp_actions.rds"))
        if (!is.null(c_guess)) corp_actions <- readRDS(c_guess)
    } else if (has_selective) {
        msg("Skipping global corp_actions fetch because Selective Builder is available.")
    }
    
    if (is.null(universe_raw)) stopf("Couldn't obtain universe_raw.")
    
    list(universe_raw = universe_raw, corp_actions = corp_actions)
}

inp <- load_or_build_inputs()
universe_raw <- inp$universe_raw
corp_actions <- inp$corp_actions

# ----------------------------
# 3) Build panel_adj_result
# ----------------------------
build_panel_adj_result <- function(universe_raw, corp_actions) {
    if (exists("af2_get_config", envir = .GlobalEnv)) {
        cfg <- tryCatch(af2_get_config(), error = function(e) NULL)
    } else { cfg <- NULL }
    
    if (exists("af2_build_panel_adj_selective", envir = .GlobalEnv)) {
        msg("\nBuilding panel_adj_result with af2_build_panel_adj_selective() ...")
        # Selective builder does its own fetching
        return(af2_build_panel_adj_selective(
            universe_raw = universe_raw,
            manual_events = NULL,
            cfg = cfg,
            verbose = TRUE
        ))
    }
    
    if (exists("af2_build_panel_adj", envir = .GlobalEnv)) {
        if (is.null(corp_actions)) stopf("af2_build_panel_adj requires corp_actions")
        return(af2_build_panel_adj(
            universe_raw = universe_raw,
            corp_actions = corp_actions,
            manual_events = NULL,
            cfg = cfg,
            verbose = TRUE
        ))
    }
    stopf("No builder found.")
}

panel_adj_result <- build_panel_adj_result(universe_raw, corp_actions)

# ----------------------------
# 4) Extract
# ----------------------------
panel_adj <- DT(panel_adj_result$panel_adj)
panel_adj[, refdate := as_idate(refdate)]
corp_actions_apply <- if(!is.null(panel_adj_result$corp_actions_apply)) DT(panel_adj_result$corp_actions_apply) else NULL

if (!is.null(corp_actions_apply)) {
    corp_actions_apply[, refdate := as_idate(refdate)]
}

# ----------------------------
# 5) Jump Diagnostics (Fixed)
# ----------------------------
msg("\n--- JUMP REDUCTION AROUND SPLITS ---")

# Find adj close col
cols <- names(panel_adj)
adj_close_col <- if("close_adj_final" %in% cols) "close_adj_final" else grep("close_adj", cols, value=TRUE)[1]

if (!is.null(adj_close_col) && !is.null(corp_actions_apply)) {
    splits <- corp_actions_apply[action_type == "split"]
    if (nrow(splits) > 0) {
        
        data.table::setkey(panel_adj, symbol, refdate)
        panel_adj[, `:=`(
            close_raw = as.numeric(close),
            close_adj = as.numeric(get(adj_close_col))
        )]
        
        panel_adj[, lr_raw := log(close_raw / data.table::shift(close_raw)), by = symbol]
        panel_adj[, lr_adj := log(close_adj / data.table::shift(close_adj)), by = symbol]
        
        split_keys <- unique(splits[, .(symbol, refdate)])
        jump_at_split <- panel_adj[split_keys, on = .(symbol, refdate), nomatch = 0L]
        jump_at_split <- jump_at_split[!is.na(lr_raw) & !is.na(lr_adj)]
        
        if (nrow(jump_at_split) > 0) {
            # FIX: Two-step calculation to avoid "object not found"
            jump_at_split[, abs_raw := abs(lr_raw)]
            jump_at_split[, abs_adj := abs(lr_adj)]
            
            jump_at_split[, `:=`(
                improve = abs_raw - abs_adj,
                ratio   = fifelse(abs_raw > 0, abs_adj / abs_raw, NA_real_)
            )]
            
            bad <- jump_at_split[abs_raw >= 0.15 & abs_adj >= 0.10]
            msg("BAD split-days (raw big, adj still big): %d", nrow(bad))
            
            if (nrow(bad) > 0) print(head(bad[, .(symbol, refdate, abs_raw, abs_adj)]))
        }
    }
}

msg("\nDone.")