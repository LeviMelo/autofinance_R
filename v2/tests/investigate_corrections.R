# v2/tests/investigate_corrections.R

# Ensure we have the objects from the build run
if (!exists("panel_adj_result")) {
  stop("Please run the build script first so 'panel_adj_result' exists in memory.")
}

library(data.table)

# Extract tables
events <- panel_adj_result$events
adj_tl <- panel_adj_result$adjustments
panel  <- panel_adj_result$panel_adj
audit  <- panel_adj_result$split_audit
jump   <- panel_adj_result$residual_jump_audit

# ==============================================================================
# 1. DEEP DIVE: Why is ITUB4 "suspect_unresolved"?
# ==============================================================================
target <- "ITUB4"
message("\n=== INVESTIGATION: ", target, " ===")

# A) Check Dividend Safety Logic (issue_div)
# Logic: Bad if (Div >= PrevClose) OR (PrevClose is NA/Zero)

# 1. Reconstruct the decision data
# We need 'close_adj_split' from the panel to see what the adjuster saw.
cols_needed <- c("symbol", "refdate", "close_raw", "close_adj_split", "close_adj_final")
p_target <- panel[symbol == target, ..cols_needed]

# 2. Join with the specific event parameters that triggered the flag
a_target <- adj_tl[symbol == target & issue_div == TRUE]

if (nrow(a_target) > 0) {
  message("Found ", nrow(a_target), " UNSAFE dividend events for ", target, ".")
  
  # Join to see prices at that moment
  # Note: The adjuster compares div_cash vs lag(close_adj_split)
  check <- merge(a_target, p_target, by = c("symbol", "refdate"), all.x = TRUE)
  
  # Fetch previous day's price (the denominator)
  check[, prev_date := refdate - 1] # Approximation for display
  # Get actual lag from panel
  p_target[, close_prev := shift(close_adj_split, 1, type = "lag")]
  
  check_final <- merge(check, p_target[, .(symbol, refdate, close_prev)], by = c("symbol", "refdate"))
  
  print(check_final[, .(
    refdate, 
    div_cash, 
    close_prev_adj = round(close_prev, 4), 
    ratio = round(div_cash / close_prev, 2),
    verdict = fifelse(is.na(close_prev), "PrevClose NA",
              fifelse(close_prev <= 0, "PrevClose <= 0",
              fifelse(div_cash >= close_prev, "Div >= Price", "Unknown")))
  )])
} else {
  message("No 'issue_div' flags found for ", target, ". Checking Safety Net...")
}

# B) Check Residual Jump (Safety Net)
j_target <- jump[symbol == target]
if (isTRUE(j_target$residual_jump_flag)) {
  message("\n", target, " also failed the RESIDUAL JUMP check.")
  print(j_target)
} else {
  message("\n", target, " PASSED the residual jump check.")
}


# ==============================================================================
# 2. SYSTEM-WIDE: Who else had "Corrections"?
# ==============================================================================

message("\n=== GLOBAL CORRECTION REPORT ===")

# A) Rejected Splits (The "Snapper" doing its job)
if (!is.null(audit)) {
  rejected <- audit[status == "rejected"]
  if (nrow(rejected) > 0) {
    message("\n[REJECTED SPLITS] Vendor data didn't match price gaps (Top 10):")
    # Calculate the gap it SHOULD have seen
    print(head(rejected[, .(symbol, vendor_refdate, yahoo_value, chosen_err, status)], 10))
  }
}

# B) Unsafe Dividends (The "Dividend Ghost" trap)
unsafe_divs <- adj_tl[issue_div == TRUE]
if (nrow(unsafe_divs) > 0) {
  message("\n[UNSAFE DIVIDENDS] Ignored to prevent negative prices (Top 10):")
  print(head(unsafe_divs[, .(symbol, refdate, div_cash, issue_div)], 10))
  
  message("Total unsafe dividend events: ", nrow(unsafe_divs))
  message("Unique symbols affected: ", length(unique(unsafe_divs$symbol)))
}

# C) Safety Net Kills (The "Bombs")
killed <- jump[residual_jump_flag == TRUE]
if (nrow(killed) > 0) {
  message("\n[SAFETY NET KILLS] Symbols dropped due to unresolvable jumps (Top 10):")
  print(head(killed[order(-residual_max_abs_logret), .(symbol, residual_max_abs_logret, residual_jump_date)], 10))
}