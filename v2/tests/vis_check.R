# v2/tests/vis_check.R
# Visual confirmation of Adjustments

# 1. Load data
if (!exists("panel_adj_dt")) {
  panel_adj_dt <- readRDS("v2/data/cache/universe_raw_latest.rds") # Or whatever your output cache is named
  # Actually, the test script leaves 'panel_adj_dt' in memory. 
  # If you restarted R, re-run the build or load the object.
}

# 2. Define Plotter
plot_adjustment <- function(sym) {
  dt <- panel_adj_dt[symbol == sym][order(refdate)]
  
  if (nrow(dt) == 0) return(message("Symbol not found: ", sym))
  
  # Normalize to start at 100 for comparison
  start_p <- dt$close_raw[1]
  dt[, norm_raw := close_raw / start_p * 100]
  dt[, norm_adj := close_adj_final / close_adj_final[1] * 100]
  
  # Plot
  plot(dt$refdate, dt$norm_raw, type = "l", col = "grey", lwd = 1,
       main = paste("Adjustment Impact:", sym), ylab = "Normalized Price", xlab = "Date")
  lines(dt$refdate, dt$norm_adj, col = "blue", lwd = 2)
  
  # Add Events
  events <- panel_adj_result$events[symbol == sym]
  if (nrow(events) > 0) {
    abline(v = events$refdate, col = "red", lty = 2)
    text(events$refdate, par("usr")[3], labels = ifelse(events$split_value != 1, "S", "D"), 
         col = "red", pos = 3, cex = 0.8)
  }
  
  legend("topleft", legend = c("Raw", "Adjusted"), col = c("grey", "blue"), lwd = c(1, 2))
}

# 3. Check specific cases
# PETR4: Should see a massive difference due to huge dividends
plot_adjustment("PETR4")

# SEQL3: Should see the raw price jump (reverse split) smoothed out
plot_adjustment("SEQL3")

# IFCM3: Should see the raw price jump smoothed out
plot_adjustment("IFCM3")