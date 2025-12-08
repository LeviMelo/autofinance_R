# v2/modules/00_core/R/logging.R

af2_log <- function(prefix, ...) {
  msg <- paste0(prefix, " ", paste(..., collapse = ""))
  message(msg)
}

af2_log_cfg <- function(config) {
  af2_log("AF2_CFG:", "\n", paste(utils::capture.output(str(config)), collapse = "\n"))
}
