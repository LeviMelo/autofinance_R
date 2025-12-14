# v2/scripts/00_make_fixtures.R
# Generates deterministic mock panels for screener development.
# Run from project root:
#   source("v2/modules/00_core/R/utils.R")
#   source("v2/modules/00_core/R/config.R")
#   source("v2/scripts/00_make_fixtures.R")

af2_make_price_path <- function(dates, start_price = 10,
                                drift = 0.0003, vol = 0.01,
                                seed = 1L) {
  set.seed(seed)
  n <- length(dates)
  eps <- rnorm(n, mean = drift, sd = vol)
  px <- start_price * cumprod(1 + eps)
  pmax(px, 0.01)
}

af2_write_fixture <- function(dt, name, cfg) {
  p <- file.path(cfg$fixtures_dir, paste0(name, ".rds"))
  saveRDS(dt, p)
  message("Wrote fixture: ", p)
}

af2_make_fixtures <- function(config = NULL) {
  af2_require(c("data.table", "lubridate"))
  cfg <- af2_get_config(config)

  # ~260 business days ending today
  end_date <- Sys.Date()
  start_date <- end_date - 400
  dates <- af2_make_bizdays_seq(start_date, end_date)
  dates <- tail(dates, 260)

  symbols <- c("AAA4", "BBB11", "CCC34", "DDD11")
  asset_type_map <- c(
    AAA4 = "equity",
    BBB11 = "fii",
    CCC34 = "bdr",
    DDD11 = "etf"
  )

  # Construct 4 different behaviors
  px_A <- af2_make_price_path(dates, 10, drift = 0.0008, vol = 0.007, seed = 11)  # strong uptrend
  px_B <- af2_make_price_path(dates, 100, drift = 0.0001, vol = 0.003, seed = 22) # low vol/flat
  px_C <- af2_make_price_path(dates, 30, drift = 0.0003, vol = 0.020, seed = 33)  # volatile
  px_D <- af2_make_price_path(dates, 50, drift = -0.0002, vol = 0.010, seed = 44) # mild down

  price_list <- list(AAA4 = px_A, BBB11 = px_B, CCC34 = px_C, DDD11 = px_D)

  # Base liquidity
  liq_level <- c(AAA4 = 2e6, BBB11 = 8e5, CCC34 = 1.5e6, DDD11 = 1.2e6)

  rows <- list()
  for (sym in symbols) {
    px <- price_list[[sym]]
    turnover <- liq_level[[sym]] * (1 + rnorm(length(px), 0, 0.1))
    turnover <- pmax(turnover, 1e4)

    dt <- data.table::data.table(
      symbol = sym,
      refdate = dates,
      close_raw = px,
      close_adj_split = px,
      close_adj_final = px,
      turnover = turnover,
      qty = round(turnover / px),
      asset_type = asset_type_map[[sym]],
      adjustment_state = "ok"
    )
    rows[[sym]] <- dt
  }

  panel_mock_small <- data.table::rbindlist(rows)
  data.table::setorder(panel_mock_small, symbol, refdate)

  # Edge cases fixture
  panel_mock_edge <- data.table::copy(panel_mock_small)

  # Make one symbol illiquid
  panel_mock_edge[symbol == "BBB11", turnover := 1e4]

  # Insert missing block for CCC34
  miss_idx <- panel_mock_edge[symbol == "CCC34", .I][50:70]
  panel_mock_edge[miss_idx, close_adj_final := NA_real_]

  # Mark DDD11 unresolved suspect
  panel_mock_edge[symbol == "DDD11", adjustment_state := "suspect_unresolved"]

  # "Split case" fixture (simulates pre-adjustment discontinuity,
  # but we store final as already fixed for screener use)
  panel_mock_split <- data.table::copy(panel_mock_small)
  # create raw discontinuity for AAA4 around mid
  mid <- panel_mock_split[symbol == "AAA4", .I][130]
  panel_mock_split[mid:nrow(panel_mock_split)][symbol == "AAA4", close_raw := close_raw / 5]
  # but keep adjusted final coherent
  panel_mock_split[symbol == "AAA4", close_adj_split := close_adj_final]
  panel_mock_split[symbol == "AAA4", adjustment_state := "split_only"]

  # Save
  af2_write_fixture(panel_mock_small, "panel_mock_small", cfg)
  af2_write_fixture(panel_mock_edge, "panel_mock_edge_cases", cfg)
  af2_write_fixture(panel_mock_split, "panel_mock_split_case", cfg)

  invisible(TRUE)
}

af2_make_fixtures()
