# v2/modules/02_diagnostics/R/diag_symbol.R
# Deterministic per-symbol diagnostics (raw vs adjusted, events, biggest jumps)

af2_diag_symbol <- function(symbol,
                            panel_adj,
                            events = NULL,
                            corp_actions_apply = NULL,
                            split_audit = NULL,
                            show_plot = TRUE,
                            top_n_jumps = 5L) {

  af2_require("data.table")
  sym <- toupper(trimws(as.character(symbol)))

  dt <- data.table::as.data.table(panel_adj)
  dt <- dt[symbol == sym]
  if (!nrow(dt)) {
    message("AF2_DIAG: symbol not found in panel_adj: ", sym)
    return(invisible(NULL))
  }

  data.table::setorder(dt, refdate)

  # Basic summary
  cat("\n========================\n")
  cat("AF2_DIAG SYMBOL: ", sym, "\n", sep = "")
  cat("========================\n")
  cat("Date range: ", as.character(min(dt$refdate)), " -> ", as.character(max(dt$refdate)), "\n", sep = "")
  cat("Rows: ", nrow(dt), "\n", sep = "")
  if ("asset_type" %in% names(dt)) cat("Asset type: ", unique(dt$asset_type), "\n", sep = "")
  if ("adjustment_state" %in% names(dt)) cat("Adjustment state(s): ", paste(unique(dt$adjustment_state), collapse = ", "), "\n", sep = "")

  # Compute jumps
  get_jump_table <- function(x, dates) {
    x <- as.numeric(x)
    ok <- is.finite(x) & x > 0
    if (sum(ok) < 3) return(data.table::data.table())
    lr <- diff(log(x))
    d  <- dates[-1]
    out <- data.table::data.table(
      refdate = as.Date(d),
      logret  = as.numeric(lr),
      abs_logret = abs(as.numeric(lr))
    )
    out[is.finite(abs_logret)]
  }

  jt_raw   <- if ("close_raw" %in% names(dt)) get_jump_table(dt$close_raw, dt$refdate) else data.table::data.table()
  jt_split <- if ("close_adj_split" %in% names(dt)) get_jump_table(dt$close_adj_split, dt$refdate) else data.table::data.table()
  jt_final <- if ("close_adj_final" %in% names(dt)) get_jump_table(dt$close_adj_final, dt$refdate) else data.table::data.table()

  if (nrow(jt_raw)) {
    cat("\nTop raw jumps (abs logret):\n")
    print(jt_raw[order(-abs_logret)][1:top_n_jumps])
  }
  if (nrow(jt_split)) {
    cat("\nTop split-adjusted jumps (abs logret):\n")
    print(jt_split[order(-abs_logret)][1:top_n_jumps])
  }
  if (nrow(jt_final)) {
    cat("\nTop final-adjusted jumps (abs logret):\n")
    print(jt_final[order(-abs_logret)][1:top_n_jumps])
  }

  # Events slice
  if (!is.null(events)) {
    ev <- data.table::as.data.table(events)[symbol == sym]
    if (nrow(ev)) {
      data.table::setorder(ev, refdate)
      cat("\nEvents for symbol:\n")
      print(ev)
    } else {
      cat("\nEvents for symbol: (none)\n")
    }
  }

  # CA slice
  if (!is.null(corp_actions_apply)) {
    ca <- data.table::as.data.table(corp_actions_apply)[symbol == sym]
    if (nrow(ca)) {
      data.table::setorder(ca, refdate, action_type)
      cat("\ncorp_actions_apply rows:\n")
      print(ca)
    } else {
      cat("\ncorp_actions_apply: (none)\n")
    }
  }

  # Split audit slice
  if (!is.null(split_audit) && nrow(split_audit)) {
    sa <- data.table::as.data.table(split_audit)[symbol == sym]
    if (nrow(sa)) {
      data.table::setorder(sa, vendor_refdate)
      cat("\nSplit audit rows:\n")
      print(sa)
    }
  }

  # Plot
  if (isTRUE(show_plot) && interactive()) {
    op <- par(no.readonly = TRUE)
    on.exit(par(op), add = TRUE)

    par(mfrow = c(2, 1))

    if ("close_raw" %in% names(dt)) {
      plot(dt$refdate, dt$close_raw, type = "l",
           main = paste0(sym, " — close_raw"),
           xlab = "", ylab = "")
    } else {
      plot.new(); title(main = paste0(sym, " — close_raw missing"))
    }

    if ("close_adj_final" %in% names(dt)) {
      plot(dt$refdate, dt$close_adj_final, type = "l",
           main = paste0(sym, " — close_adj_final"),
           xlab = "", ylab = "")
    } else {
      plot.new(); title(main = paste0(sym, " — close_adj_final missing"))
    }
  }

  invisible(list(
    symbol = sym,
    panel = dt,
    jumps_raw = jt_raw,
    jumps_split = jt_split,
    jumps_final = jt_final
  ))
}
