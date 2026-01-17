# Contracts (Data Layer Freeze) — autofinance_R v2

This folder is the **authoritative specification** for the v2 data layer:
- what each table/object **must contain** (schema),
- what it **means** (semantics),
- what is **guaranteed** (invariants),
- what consumers are **allowed to assume**,
- how the “selective corporate actions” pipeline behaves.

If code and contracts disagree: **contracts win** (or code must be fixed before release).

---

## Naming & shared conventions

### Time axis
- `refdate`: trading date (Date-like). May be `Date` or `data.table::IDate`; treated as Date.
- All series are assumed to be **one row per (symbol, refdate)** after dedupe.

### Symbols
- `symbol`: uppercase B3 ticker (e.g., `VALE3`, `KNCR11`).
- `yahoo_symbol`: vendor ticker (usually `SYMBOL.SA`).

### Price factor convention (critical)
For splits, v2 uses **price factors**:
- A forward split (2-for-1) halves the price ⇒ factor is **0.5**.
- A reverse split (1-for-10) multiplies price by 10 ⇒ factor is **10**.

The adjuster applies cumulative factors **backwards** (events affect earlier dates), using an **exclusive** convention:
- An event on date `t` affects dates `< t` (not the same day).

### Schema stability policy
- Adding new columns is generally **backwards compatible**.
- Renaming/removing columns, changing key definitions, or changing factor orientation is **breaking**.
- `adjustment_state` is part of the contract and must remain stable.

---

## Pipeline overview (data layer)

1) **Module 01** produces `universe_raw` (raw OHLC + liquidity + type).
2) **Module 03** produces `corp_actions_registry` (vendor splits/dividends; Yahoo).
3) **Module 04** produces:
   - `events` (normalized per-day split_value/div_cash per symbol),
   - `adjustments` (per-day cumulative factors audit),
   - `panel_adj` (adjusted OHLC),
   - `residual_jump_audit` (safety net output),
   - plus, in selective mode:
     - `split_audit`, `corp_actions_apply`, `corp_actions_quarantine`.

4) **Module 05** consumes only `panel_adj` and produces `screener_features`.

The Screener **must not** fetch or mutate data.

---

## Safety valves

### 1) Dividend computation safety
If dividend adjustment cannot be computed safely (e.g., dividend >= prev close), the dividend factor is skipped and `issue_div = TRUE` is recorded in `adjustments`. Symbols with any `issue_div` are marked `suspect_unresolved`.

### 2) Residual jump safety net
Even after applying all events, if a symbol still shows an extreme 1-day move (abs log return >= configured tolerance), it is marked `suspect_unresolved`.

### 3) Screener hard gate
By default, the screener **rejects** any `panel_adj` containing `adjustment_state == "suspect_unresolved"` unless explicitly overridden.

---

## Contract index

Core tables:
- `universe_raw.md`
- `corp_actions_registry.md`
- `manual_events.md`
- `events.md`
- `adjustments.md`
- `panel_adj.md`
- `residual_jump_audit.md`
- `split_audit.md` (selective mode only)

Screener interface:
- `screener_input.md`
- `screener_output.md`

Config surface (freeze of knobs actually used):
- `config_core.md`
- `config_screener.md`
