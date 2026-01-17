# config_screener.md (Module 05 knobs)

## Purpose
Defines the configuration keys controlling feature computation and scoring.

## Screener config object
Created by `af2_get_screener_config()`.

## Keys (frozen)

### Windowing
- `lookback_days` (int): default 252
- `horizons_days` (int vector): default c(21, 63, 126, 252)

### Liquidity screen
- `min_turnover` (dbl): default 5e5
- `min_days_traded_ratio` (dbl): default 0.8

### Scoring
- `score_weights` (named numeric list/vector)
  - keys must match feature column names
  - scoring is z-score aggregation per feature

## Guardrails (as implemented)
- Screener validates input contract and stops if unresolved suspects exist unless explicitly allowed.
- Screener checks that `score_weights` names exist in `features`.
