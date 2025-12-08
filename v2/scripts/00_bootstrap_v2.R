# v2/scripts/00_bootstrap_v2.R
# Creates the v2 directory structure + minimal placeholder READMEs.
# Run from project root: source("v2/scripts/00_bootstrap_v2.R")

af2_bootstrap_v2 <- function(root = "v2", verbose = TRUE) {
  dirs <- c(
    root,
    file.path(root, "README.md"),
    file.path(root, "contracts"),
    file.path(root, "data", "raw"),
    file.path(root, "data", "cache"),
    file.path(root, "data", "fixtures"),
    file.path(root, "data", "manual"),
    file.path(root, "logs"),
    file.path(root, "modules"),
    file.path(root, "modules", "00_core", "R"),
    file.path(root, "modules", "00_core", "tests"),
    file.path(root, "modules", "01_universe", "R"),
    file.path(root, "modules", "01_universe", "tests"),
    file.path(root, "modules", "02_diagnostics", "R"),
    file.path(root, "modules", "02_diagnostics", "tests"),
    file.path(root, "modules", "03_corporate_actions", "R"),
    file.path(root, "modules", "03_corporate_actions", "tests"),
    file.path(root, "modules", "04_adjuster", "R"),
    file.path(root, "modules", "04_adjuster", "tests"),
    file.path(root, "modules", "05_screener", "R"),
    file.path(root, "modules", "05_screener", "tests"),
    file.path(root, "modules", "06_risk", "R"),
    file.path(root, "modules", "06_risk", "tests"),
    file.path(root, "modules", "07_portfolio", "R"),
    file.path(root, "modules", "07_portfolio", "tests"),
    file.path(root, "modules", "08_backtest", "R"),
    file.path(root, "modules", "08_backtest", "tests"),
    file.path(root, "scripts")
  )

  # Create directories (ignore .md entries)
  dir_paths <- dirs[!grepl("\\.md$", dirs)]
  for (d in dir_paths) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE, showWarnings = FALSE)
      if (verbose) message("Created: ", d)
    }
  }

  # Minimal contracts placeholders
  contract_files <- c(
    "universe_raw.md",
    "adjustments.md",
    "panel_adj.md",
    "screener_input.md",
    "screener_output.md"
  )
  for (f in contract_files) {
    p <- file.path(root, "contracts", f)
    if (!file.exists(p)) {
      writeLines(
        c(
          paste0("# ", f),
          "",
          "This is a placeholder. Fill with the explicit schema + guarantees."
        ),
        con = p
      )
      if (verbose) message("Seeded contract: ", p)
    }
  }

  # Module READMEs
  module_readmes <- c(
    file.path(root, "modules", "00_core", "README.md"),
    file.path(root, "modules", "01_universe", "README.md"),
    file.path(root, "modules", "02_diagnostics", "README.md"),
    file.path(root, "modules", "03_corporate_actions", "README.md"),
    file.path(root, "modules", "04_adjuster", "README.md"),
    file.path(root, "modules", "05_screener", "README.md")
  )
  for (p in module_readmes) {
    if (!file.exists(p)) {
      writeLines(
        c(
          "# Module",
          "",
          "Purpose:",
          "- Define INPUT contract",
          "- Define OUTPUT contract",
          "- List functions",
          "- List tests"
        ),
        con = p
      )
      if (verbose) message("Seeded README: ", p)
    }
  }

  # Root README
  root_readme <- file.path(root, "README.md")
  if (!file.exists(root_readme)) {
    writeLines(
      c(
        "# autofinance_R v2",
        "",
        "Principles:",
        "- In-memory first",
        "- Contract-first",
        "- Fixtures-first",
        "- Screener cannot fetch or adjust data",
        "- Adjuster is the single owner of history mutation"
      ),
      con = root_readme
    )
    if (verbose) message("Seeded: ", root_readme)
  }

  invisible(TRUE)
}

af2_bootstrap_v2()
