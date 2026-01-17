# panel_adj.md (Module 04 output)

## Purpose
`panel_adj` is the adjusted daily panel produced by Module 04 (Adjuster).
It contains raw OHLC aliases, split-adjusted OHLC, and final adjusted OHLC (Split + Dividend).
It is the only input accepted by the Screener (Module 05).

## Table contract (schema)

### Primary key
- `(symbol, refdate)` must be unique.

### Required columns (minimum)
| column           | type      | required | semantics |
|------------------|-----------|----------|----------|
| symbol           | character | yes      | uppercase ticker |
| refdate          | Date      | yes      | trading date |
| asset_type       | character | yes      | `equity` / `fii` / `etf` / `bdr` |
| close_adj_final  | numeric   | yes      | final adjusted close (splits + dividends) |
| adjustment_state | character | yes      | see states below |
| turnover         | numeric   | yes*     | liquidity volume in BRL |
| qty              | numeric   | yes      | liquidity quantity proxy |

\* Either `turnover` or `vol_fin` may exist in upstream objects, but v2 normalizes to `turnover` in practice.

### Standard columns (v2 production)
Raw aliases:
- `open_raw, high_raw, low_raw, close_raw`

Split-adjusted (intermediate):
- `open_adj_split, high_adj_split, low_adj_split, close_adj_split`

Final adjusted (split + dividend):
- `open_adj_final, high_adj_final, low_adj_final, close_adj_final`

### Optional / non-contract columns
Depending on internal steps, `panel_adj` may also contain:
- original `open/high/low/close` (raw duplicates),
- additional intermediate columns added during debug.
Consumers must not rely on them.

## adjustment_state (authoritative set; must not drift)

v2 emits exactly one of:

- `no_actions`
  - no split and no dividend events detected in the panel window, and no manual events
- `dividend_only`
  - at least one dividend event applied; no splits in-window
- `split_only`
  - at least one split event applied; no dividends in-window
- `split_dividend`
  - both splits and dividends in-window
- `manual_override`
  - at least one manual event exists for the symbol (takes precedence over the above state labels)
- `suspect_unresolved`
  - safety valve triggered by either:
    1) any dividend day had `issue_div == TRUE` in `adjustments`, OR
    2) residual jump safety net flagged (abs 1-day log-return in `close_adj_final` >= tolerance)

### Precedence rules (exact)
1) Start as `no_actions`.
2) Assign `dividend_only` / `split_only` / `split_dividend` by event presence.
3) If any manual event for symbol ⇒ `manual_override`.
4) If any dividend issue OR residual jump flagged ⇒ `suspect_unresolved` (overrides everything).

## Invariants / Guarantees
- `(symbol, refdate)` uniqueness is enforced (dedupe + assert).
- If `adjustment_state == "suspect_unresolved"`, the Screener must exclude symbol by default.

## Relationship to other outputs
Module 04 returns also:
- `adjustments` (factor timeline)
- `events` (normalized daily event table)
- `residual_jump_audit` (safety net per symbol)

Selective build additionally returns:
- `split_audit`
- `corp_actions_apply`
- `corp_actions_quarantine`

## Consumer rule (hard)
The Screener must not attempt to “fix” prices; it must treat:
- `suspect_unresolved` as poison unless explicitly overridden by config.
