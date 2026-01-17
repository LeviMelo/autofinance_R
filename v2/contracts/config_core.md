# config_core.md (Module 00 / data-layer knobs)

## Purpose
Defines the configuration surface area that affects the data layer and must remain stable under the freeze.

This contract documents:
- knobs that are *actually consumed by code paths in v2*,
- knobs present but currently unused (reserved).

## Core config object
Created by `af2_get_config()`, with shallow override support.

## Actively used keys (frozen behavior)

### Universe scope
- `years` (int vector): years to fetch/build (default: current year and previous)
- `include_types` (chr vector): `equity`, `fii`, `etf`, `bdr`

### Liquidity thresholds (used by CA candidate selection)
- `min_turnover` (dbl): default 5e5
- `min_days_traded_ratio` (dbl): default 0.8

### Corporate actions toggles
- `enable_splits` (bool): whether to include vendor events at all
- `enable_manual_events` (bool): whether to include manual events

### Selective actions switch (critical)
- `enable_selective_actions` (bool): if TRUE, uses candidate prefilter
- `ca_cache_mode` (chr): `batch`, `by_symbol`, or `none`
- `ca_fetch_mode` (chr): `chart` or `quantmod`

### Candidate prefilter knobs (used)
- `ca_prefilter_liq_window_days` (int): default 63 (liquidity window)
- `ca_prefilter_jump_log_thr` (dbl): default 1.0 (reverse-split-like jump)
- `ca_prefilter_gap_equity`, `ca_prefilter_gap_fii`, `ca_prefilter_gap_etf`, `ca_prefilter_gap_bdr`:
  negative thresholds on simple return to flag forward-split-like drops

### Split-gap validation (selective pipeline)
- `enable_split_gap_validation` (bool)
- `split_gap_tol_log` (dbl): default 0.35
- `split_gap_max_forward_days` (int): default 5
- `split_gap_max_back_days` (int): default 3
- `split_gap_use_open` (bool): prefer open/lag close

### Residual jump safety net
- `adj_residual_jump_tol_log` (dbl): default 1.0

### Screener unresolved policy default
- `allow_unresolved_in_screener` (bool): default FALSE

### Paths (cache/raw/logs)
- `cache_dir`, `raw_dir`, `fixtures_dir`, `manual_dir`, `logs_dir`

## Reserved / currently unused keys (present but not enforced)
These exist in config defaults but are not currently used by code paths:
- `ca_prefilter_recent_days`
- `ca_prefilter_top_n_overall`, `ca_prefilter_top_n_by_type`, `ca_prefilter_max_candidates`
- `ca_prefilter_active_days`
- `enable_split_plausibility_gate`, `split_gate_min`, `split_gate_max`

They must either be:
- implemented and added to the contract as active, or
- removed in a breaking change (version bump).

## Determinism rule
Under the freeze, the meaning of the active keys above must not change silently.
