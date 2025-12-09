# v2/modules/04_adjuster/R/build_adjustments.R

# Build normalized event table for the adjuster.
# Input corp_actions contract (from Module 03):
#   symbol, yahoo_symbol, refdate, action_type, value, source
#
# Output event table:
#   symbol, refdate, split_value, div_cash, source_mask, has_manual

af2_adj_normalize_corp_actions <- function(corp_actions, cfg = NULL) {
  cfg <- cfg %||% af2_get_config()
  if (is.null(corp_actions)) {
    return(data.table(
      symbol = character(),
      refdate = as.Date(character()),
      action_type = character(),
      value = numeric(),
      source = character()
    ))
  }

  dt <- data.table::as.data.table(corp_actions)

  # Allow either registry schema with yahoo_symbol or not
  if ("yahoo_symbol" %in% names(dt)) dt[, yahoo_symbol := NULL]

  # Basic required
  af2_assert_cols(dt, c("symbol", "refdate", "action_type", "value", "source"),
                  name = "corp_actions")

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, action_type := tolower(trimws(as.character(action_type)))]
  dt[, refdate := as.Date(refdate)]
  dt[, value := as.numeric(value)]
  dt[, source := tolower(trimws(as.character(source)))]

  dt <- dt[!is.na(symbol) & nzchar(symbol) & !is.na(refdate)]
  dt <- dt[action_type %in% c("split", "dividend")]
  # --------------------------------------------
  # Normalize split values to PRICE FACTORS
  # Yahoo/quantmod typically returns split ratios
  # (e.g., 2 for 2:1). For backward price adjustment,
  # we need the inverse (1/ratio).
  # --------------------------------------------
  dt[action_type == "split" & is.finite(value) & value > 0,
     value := 1 / value]

  # -------------------------------
  # PATCH: plausibility gate (post-normalization)
  # -------------------------------
  if (isTRUE(cfg$enable_split_plausibility_gate)) {
    vmin <- as.numeric(cfg$split_value_min %||% 0.05)
    vmax <- as.numeric(cfg$split_value_max %||% 10)

    bad <- dt[
      action_type == "split" &
      source == "yahoo" &
      (value < vmin | value > vmax)
    ]

    if (nrow(bad)) {
      af2_log(
        "AF2_ADJ:",
        "WARNING: quarantining ", nrow(bad),
        " Yahoo split rows outside [", vmin, ", ", vmax, "]."
      )
      # Drop them for now; manual can re-add later
      dt <- dt[!(action_type == "split" &
                 source == "yahoo" &
                 (value < vmin | value > vmax))]
    }
  }

  dt
}

af2_adj_normalize_manual_events <- function(manual_events) {
  if (is.null(manual_events)) {
    return(data.table(
      symbol = character(),
      refdate = as.Date(character()),
      action_type = character(),
      value = numeric(),
      source = character()
    ))
  }

  dt <- data.table::as.data.table(manual_events)

  af2_assert_cols(dt, c("symbol", "refdate", "action_type", "value"),
                  name = "manual_events")

  if (!"source" %in% names(dt)) dt[, source := "manual"]

  dt[, symbol := toupper(trimws(as.character(symbol)))]
  dt[, action_type := tolower(trimws(as.character(action_type)))]
  dt[, refdate := as.Date(refdate)]
  dt[, value := as.numeric(value)]
  dt[, source := "manual"]

  dt <- dt[!is.na(symbol) & nzchar(symbol) & !is.na(refdate)]
  dt <- dt[action_type %in% c("split", "dividend")]
  dt[action_type == "split" & is.finite(value) & value > 0,
     value := 1 / value]

  # -------------------------------
  # PATCH: plausibility gate (post-normalization)
  # -------------------------------
  if (isTRUE(cfg$enable_split_plausibility_gate)) {
    vmin <- as.numeric(cfg$split_value_min %||% 0.05)
    vmax <- as.numeric(cfg$split_value_max %||% 10)

    bad <- dt[
      action_type == "split" &
      source == "yahoo" &
      (value < vmin | value > vmax)
    ]

    if (nrow(bad)) {
      af2_log(
        "AF2_ADJ:",
        "WARNING: quarantining ", nrow(bad),
        " Yahoo split rows outside [", vmin, ", ", vmax, "]."
      )
      # Drop them for now; manual can re-add later
      dt <- dt[!(action_type == "split" &
                 source == "yahoo" &
                 (value < vmin | value > vmax))]
    }
  }

  dt
}

af2_adj_build_events <- function(corp_actions,
                                 manual_events = NULL,
                                 cfg = NULL,
                                 verbose = TRUE) {

  ca <- af2_adj_normalize_corp_actions(corp_actions)
  me <- af2_adj_normalize_manual_events(manual_events)
  cfg <- cfg %||% af2_get_config()

  dt_all <- data.table::rbindlist(list(ca, me), use.names = TRUE, fill = TRUE)
  if (!nrow(dt_all)) {
    return(data.table(
      symbol = character(),
      refdate = as.Date(character()),
      split_value = numeric(),
      div_cash = numeric(),
      source_mask = character(),
      has_manual = logical()
    ))
  }

  # ------------------------------------------------------------
  # Optional Yahoo split plausibility gate (quarantine, not edit)
  # Applies ONLY to Yahoo-sourced split rows before aggregation.
  # ------------------------------------------------------------
  if (isTRUE(cfg$enable_split_plausibility_gate)) {

    minv <- as.numeric(cfg$split_gate_min %||% 0.05)
    maxv <- as.numeric(cfg$split_gate_max %||% 20)

    if (!is.finite(minv) || minv <= 0) minv <- 0.05
    if (!is.finite(maxv) || maxv <= 0) maxv <- 20

    bad_splits <- dt_all[
      action_type == "split" &
        source == "yahoo" &
        (value < minv | value > maxv)
    ]

    if (nrow(bad_splits) && verbose) {
      af2_log("AF2_ADJ:",
              "Split gate active. Quarantining ", nrow(bad_splits),
              " Yahoo split rows outside [", minv, ", ", maxv, "].")
      # Print a small sample for visibility
      print(utils::head(bad_splits[order(symbol, refdate)], 10))
    }

    # Drop only the suspicious Yahoo split rows from factor construction
    if (nrow(bad_splits)) {
      dt_all <- dt_all[!(
        action_type == "split" &
          source == "yahoo" &
          (value < minv | value > maxv)
      )]
    }
  }

  # Aggregate same-day events:
  # - splits: multiply price factors
  # - dividends: sum cash amounts
  splits <- dt_all[action_type == "split",
                   .(split_value = prod(value, na.rm = TRUE),
                     source_mask = paste(sort(unique(source)), collapse = "+")),
                   by = .(symbol, refdate)]

  divs <- dt_all[action_type == "dividend",
                 .(div_cash = sum(value, na.rm = TRUE),
                   source_mask_div = paste(sort(unique(source)), collapse = "+")),
                 by = .(symbol, refdate)]

  # Merge into unified event table
  ev <- merge(
    splits, divs,
    by = c("symbol", "refdate"),
    all = TRUE
  )

  ev[is.na(split_value), split_value := 1]
  ev[is.na(div_cash), div_cash := 0]

  # Compose source mask
  ev[, source_mask := fifelse(
    !is.na(source_mask) & !is.na(source_mask_div),
    paste0(source_mask, "+", source_mask_div),
    fifelse(!is.na(source_mask), source_mask, source_mask_div)
  )]
  ev[is.na(source_mask), source_mask := "unknown"]

  ev[, c("source_mask_div") := NULL]

  ev[, has_manual := grepl("manual", source_mask)]

  # Clean obvious nonsense
  ev <- ev[!is.na(symbol) & nzchar(symbol) & !is.na(refdate)]
  ev <- ev[is.finite(split_value) & split_value > 0]
  ev <- ev[is.finite(div_cash) & div_cash >= 0]

  if (verbose) {
    af2_log("AF2_ADJ:", "Built events: ", nrow(ev), " rows for ",
            length(unique(ev$symbol)), " symbols.")
  }

  data.table::setorder(ev, symbol, refdate)
  ev
}
