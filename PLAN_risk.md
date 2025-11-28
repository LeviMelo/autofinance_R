Perfeito, vamos atualizar o plano levando **GARCH/DCC/VAR + portfolio engine + backtest** para o centro da arquitetura, não como “futuro opcional”.

Vou te entregar um `PLAN.md` já revisado, autocontido e consistente com:

* DB + COTAHIST como base.
* Screener “research-grade”.
* **Risk models avançados (GARCH/DCC/VAR) como primeira classe**.
* Portfolio engine com restrições.
* Backtest core que cola tudo isso **ao longo do tempo**.
* Camada fiscal/trading construída em cima do backtest.

---

# Autofinance – Full Architecture (DB + Screener + VAR/GARCH/DCC + Portfolio + Backtest)

## 0. Escopo e objetivos

**Objetivo global:**
Construir um sistema local, em R, que funcione como uma **estação quantitativa de pesquisa e simulação** para ativos da B3 (ações, FIIs, BDRs, ETFs), com:

1. **Camada de dados sólida**:

   * COTAHIST via `rb3` como **fita oficial** de OHLCV.
   * Ações corporativas (splits, depois dividendos) via `quantmod::getSplits` / `getDividends`.
   * Fatores macro via **BCB SGS** (CDI, Selic, IPCA, FX, etc.).
   * Tudo persistido em **SQLite** (sem CSVs frágeis).

2. **Screener avançado**:

   * Trabalha sobre preços ajustados por splits.
   * Usa métricas densas: retornos em múltiplos horizontes, volatilidade, skew/kurtosis, VaR/CVaR, drawdown, Ulcer Index, betas a IBOV, SPX, USD, DI, etc.
   * Faz ranking **por classe de ativo** (Equity/FII/BDR/ETF), sem misturar tudo num top global tosco.

3. **Modelos de risco/retorno avançados (centrais, não laterais)**:

   * **GARCH** univariado para volatilidades condicionais σᵢ,t.
   * **DCC** para correlações condicionais Cᵗ e covariância Σₜ = Dₜ Cₜ Dₜ.
   * **VAR** para dinâmica conjunta de retornos ou fatores e previsão de μₜ (expected returns).
   * Tudo empacotado num módulo `autofinance_risk_models.R` com interface clara para o portfolio engine.

4. **Portfolio engine sério**:

   * Respeita restrições: long-only, w_max por ativo, limite de alavancagem, possíveis restrições por classe (mínimo FIIs, máximo BDRs, etc.).
   * Suporta múltiplos modos: equal, inv_vol, min_var, mean_var, max_sharpe.
   * Usa μ e Σ vindos dos risk models (sample/shrinkage/GARCH+DCC/VAR).

5. **Backtest core**:

   * Loop temporal com janelas de lookback (ex: 3 anos) para recalibrar screener + risk models + portfolio em cada rebalance.
   * Usa **somente SQLite + memória**; sem chamadas HTTP dentro da simulação.
   * Entrega curva de patrimônio, trades, stats, decomposição por período.

6. **Camada fiscal/trading**:

   * Aplica regras de impostos (ações x BDR x FIIs), custos de transação, limites operacionais **sobre o backtest**.
   * Não contamina screener nem risk models com fiscal.

---

## 1. Camadas e módulos

### 1.1 Visão em camadas

1. **Data & DB layer**

   * Arquivos:

     * `autofinance_db_core.R`
     * `autofinance_ingest_b3.R`
     * `autofinance_ingest_splits.R`
     * `autofinance_ingest_macro.R`
   * Responsável por:

     * Sincronizar COTAHIST → `prices_raw`.
     * Sincronizar splits/dividends → `corporate_actions`.
     * Sincronizar macro → `macro_series`.
     * Manter `assets_meta`.

2. **Adjustment & Panel layer**

   * Arquivo: `autofinance_panel.R`
   * Responsável por:

     * Construir painéis de preços **ajustados por split** (OHLC) a partir de `prices_raw` + `corporate_actions`.
     * Calcular retornos simples/log, excess returns vs RF, agregar macros.

3. **Screener layer**

   * Arquivo: `autofinance_screener.R` (ou `autofinance_universe_screener.R`)
   * Responsável por:

     * Filtrar universo por liquidez/ativos válidos.
     * Calcular todo o vetor de métricas cross-section.
     * Rankear por classe de ativo.

4. **Risk models layer (μ, Σ)**

   * Arquivo: `autofinance_risk_models.R`
   * Responsável por:

     * Estimar **Σ (covariância)** por vários métodos: sample, shrinkage, GARCH+DCC.
     * Estimar **μ (expected returns)**: média histórica, momentum-based, VAR-based.
     * Fornecer interfaces estáveis pro portfolio engine.

5. **Portfolio engine layer**

   * Arquivo: `autofinance_portfolio_engine.R`
   * Responsável por:

     * Dado μ, Σ + restrições, resolver problemas de alocação (equal, inv_vol, min_var, mean_var, max_sharpe).
     * Garantir long-only, w_max, leverage_max, restrições por grupo.

6. **Backtest core layer**

   * Arquivo: `autofinance_backtest_core.R`
   * Responsável por:

     * Loop temporal: em cada rebalance, rodar screener, risk model, portfolio engine usando só dados até t.
     * Aplicar retornos realizados entre t e t_next.
     * Calcular métricas de performance, drawdown, turnover, etc.

7. **Fiscal & trading rules layer**

   * Arquivo: `autofinance_fiscal_trading.R`
   * Responsável por:

     * Modelar impostos e custos em cima de sequências de trades/posições.
     * Dar versão “net-of-tax” da curva de patrimônio produzida pelo backtest core.

---

## 2. Data & DB – Modelo e ingestão

### 2.1 Esquema SQLite (resumo consolidado)

**Tabela `assets_meta`**

* `symbol` (PK) – `"PETR4"`, `"IVVB11"`, `"KNCR11"`, `"A1IV34"`, etc.
* `asset_type` – `"EQUITY" | "FII" | "BDR" | "ETF" | ...`.
* `sector` – texto opcional (B3 sector/segmento).
* `active` – `1` ativo, `0` deslistado (mantém para evitar survivorship bias).
* `last_update_splits` – data da última checagem do `getSplits`.
* `last_update_divs` – idem para dividendos (quando usado).

**Tabela `prices_raw`** – COTAHIST

* `symbol`
* `refdate` (YYYY-MM-DD)
* `open, high, low, close` (REAL)
* `vol_fin` (REAL)
* `qty` (INTEGER)
* **PK** `(symbol, refdate)` `WITHOUT ROWID`.

**Tabela `corporate_actions`**

* `symbol`
* `date` (ex-date)
* `type` – `"SPLIT"` / `"DIVIDEND"`.
* `value` – razão do split (ex: 0.1) ou valor do dividendo.
* **PK** `(symbol, date, type)`.

**Tabela `macro_series`**

* `series_id` – ex: `"CDI"`, `"SELIC"`, `"IPCA"`, `"USD_P TAX"`, ou ID numérico BCB.
* `refdate`
* `value`
* **PK** `(series_id, refdate)`.

### 2.2 Ingestão e sincronização (on-demand, sem servidor)

**Funções principais:**

* `af_db_init()`
  Cria o banco e as tabelas se não existirem.

* `af_sync_b3(years = NULL)`

  * Se `years` é `NULL`, checa `max(refdate)` em `prices_raw` e baixa apenas anos que têm datas faltantes (ex: ano corrente).
  * Usa `rb3::cotahist_get` para pegar COTAHIST e insere via `INSERT OR REPLACE`.

* `af_sync_splits(symbols = NULL, force = FALSE, max_age_days = 7)`

  * Se `symbols = NULL`, pega da `assets_meta` (ativos ativos).
  * Para cada symbol, checa `last_update_splits`:

    * Se recente e `force = FALSE` → pula.
    * Caso contrário, chama `quantmod::getSplits(paste0(symbol, ".SA"))` (full history ou incremental, a gosto).
  * Normaliza em `corporate_actions` com `type="SPLIT"`, atualiza `last_update_splits`.

* `af_sync_dividends(...)` (opcional, depois)
  Mesma lógica, mas para dividendos.

* `af_sync_macro(series_ids, start_date, end_date)`

  * Para cada série do BCB, busca dados faltantes e injeta em `macro_series`.

---

## 3. Panel e retornos

### 3.1 Construção do painel ajustado

Arquivo: `autofinance_panel.R`

Função central:

```r
af_build_adjusted_panel <- function(symbols, start_date, end_date, con) {
  # 1) Query prices_raw
  # 2) Query corporate_actions (SPLIT)
  # 3) Para cada symbol:
  #    - OHLC xts raw
  #    - splits xts
  #    - adjRatios -> fatores de ajuste
  #    - aplica em O/H/L/C
  # 4) Devolve data.table com:
  #    symbol, refdate, open_adj, high_adj, low_adj, close_adj, vol_fin, qty
}
```

### 3.2 Retornos e excess returns

Ainda em `autofinance_panel.R` (ou submódulo):

* `af_compute_returns(panel_adj, rf_series = NULL)`:

  * Calcula:

    * `r_simple_t = close_adj_t / close_adj_{t-1} - 1`.
    * `r_log_t = log(close_adj_t / close_adj_{t-1})`.
  * Se `rf_series` (CDI, Selic) fornecido:

    * Calcula `excess_ret = r_simple - r_rf_diário`.

* Pode opcionalmente já acrescentar:

  * Benchmarks (ex: IBOV, SPX_BR, USD) na mesma estrutura.

---

## 4. Screener – métrica, filtros, ranking

Arquivo: `autofinance_screener.R` / `autofinance_universe_screener.R`

### 4.1 Fluxo conceitual

1. **Definir janela de lookback**: ex: últimos 252 dias úteis.
2. **Filtrar universo por liquidez** usando `prices_raw`:

   * `median_vol_fin_lookback >= min_liquidity`.
   * Opcional: days_traded_ratio, Amihud, etc.
3. **Construir painel ajustado** para esse universo e janela via `af_build_adjusted_panel`.
4. **Carregar macro & benchmarks** para o período.
5. **Calcular vetor de métricas** por símbolo.
6. **Rankear por classe de ativo** conforme config de pesos/fatores.

### 4.2 Vetor de métricas (cross-section)

Por símbolo (dado painel ajustado e retornos):

* **Retornos / momentum**:

  * Janela de lookback `T` (dias).
  * Conjunto de horizontes `H = {21, 63, 126, 252, ...}`.
  * Para cada `h ∈ H` tal que `h <= T`:

    * `ret_h` = retorno acumulado geométrico nos últimos h dias.
    * Opcional: “ret_rel_ibov_h” = retorno do ativo – retorno do IBOV no mesmo período.

* **Volatilidades**:

  * `vol_21d`, `vol_63d`, `vol_252d` (annualizadas).

* **Distribuição / cauda**:

  * `skew_252d`.
  * `kurt_252d`.

* **Risco de cauda**:

  * `var_95_252d`.
  * `cvar_95_252d`.

* **Drawdowns**:

  * `max_dd_lookback`.
  * `ulcer_index` (UI).
  * `avg_time_underwater`.

* **Liquidez**:

  * `median_vol_fin_63d` (ou 252d).
  * `amihud_illiquidity` = média(|ret| / vol_fin).
  * `days_traded_ratio` = dias com `qty > 0` / total de dias.

* **Betas e correlações**:

  * Contra IBOV, USD, SPX, DI, etc., usando retorno diário:

    * `beta_ibov`, `beta_usd`, `beta_spx`, `beta_di`.
    * `corr_ibov`, `corr_usd`, etc.

### 4.3 Interface do screener

```r
af_screener_config_default <- list(
  lookback_days  = 252L,
  horizons_days  = c(21L, 63L, 126L, 252L),
  min_liquidity  = 5e5,    # BRL
  min_days_traded = 0.8,   # 80% dos dias com negócio
  score_weights  = list(
    ret_252d      = +1.0,
    vol_252d      = -0.5,
    ulcer_index   = -0.7,
    beta_ibov     = -0.2,
    amihud        = -0.3
    # etc, ajustáveis
  )
)

af_run_screener <- function(config = af_screener_config_default, con) {
  # 1) determina janela
  # 2) filtra liquidez usando prices_raw
  # 3) panel_adj + returns + macro
  # 4) metrics
  # 5) ranking por asset_type
  # 6) retorna lista:
  #    list(equity = df_e, fii = df_f, bdr = df_b, etf = df_etf)
}
```

---

## 5. Risk Models – μ e Σ (incluindo GARCH/DCC/VAR)

Arquivo: `autofinance_risk_models.R`

Meta: separar claramente **cálculo de métricas cross-section** (screener) de **modelagem de μ e Σ** para otimização de portfólio.

### 5.1 Interface geral

```r
af_risk_config_default <- list(
  cov_method   = "sample",      # "sample", "shrinkage", "garch_dcc"
  mu_method    = "mean",        # "mean", "momentum", "var"
  window_years = 3,
  rf_series_id = "CDI"
)

af_risk_estimate <- function(
  panel_returns,  # retornos diários (já ajustados), subset de símbolos
  macro_panel,    # opcional, para VAR/fatores
  config = af_risk_config_default
) {
  # 1) Seleciona janela (ex: últimos 3 anos)
  # 2) Estima Σ conforme cov_method
  # 3) Estima μ conforme mu_method
  # 4) Retorna list(mu = μ_vec, Sigma = Σ_mat, meta = list(...))
}
```

### 5.2 Covariance (Σ)

**cov_method = "sample"**
Covariância de amostra simples de excess returns:

* Σ = cov(R_excesso).

**cov_method = "shrinkage"**
Ex: Ledoit-Wolf:

* Σ_shrink = λ·F + (1 − λ)·Σ_sample,
  onde F pode ser matriz diagonal (variâncias apenas) ou “identidade escalada”.

**cov_method = "garch_dcc"** (núcleo avançado):

1. Para cada ativo i:

   * Ajusta um GARCH(1,1) a rᵢ,t:

     * σᵢ,t² = ω + α εᵢ,t−1² + β σᵢ,t−1²
   * Obtém resíduos padronizados:

     * zᵢ,t = εᵢ,t / σᵢ,t.

2. Conjunto de z_t = (z₁,t, z₂,t, ..., zₙ,t):

   * Ajusta modelo DCC para a matriz de correlações condicionais Cₜ.

3. Em um rebalance em t*:

   * Pegamos σᵢ,t* para cada ativo → Dₜ* = diag(σ₁,t*,...,σₙ,t*).
   * Pegamos Cₜ* do DCC.
   * Covariância condicional:

     * Σₜ* = Dₜ* Cₜ* Dₜ*.

**Design importante:**
No backtest, **não recalibrar GARCH/DCC todo dia**; recalibrar **por rebalance** (ex.: mensal). Para cada rebalance t:

* Usar janela (ex.: últimos 3 anos) para calibrar GARCH/DCC.
* Extrair Σₜ pronto para o portfolio engine.

### 5.3 Expected returns (μ)

**mu_method = "mean"**
Média histórica de excess returns na janela:

* μᵢ = média(rᵢ_excesso) (anualizada) para cada ativo.

**mu_method = "momentum"**
Imputar μ com base em métricas de momentum vindas do screener:

* μᵢ ∝ combinação linear de `ret_21d`, `ret_63d`, `ret_252d`.
* Pode normalizar e calibrar escalas.

**mu_method = "var"** (VAR-based, avançado):

1. Definir vetor de variáveis para o VAR:

   * Pode ser:

     * diretamente retornos de ativos, ou
     * retornos de fatores (IBOV, SPX_BR, FX, yield, etc.) e mapear ativos para fatores.

2. Ajustar VAR(p):

   * yₜ = A₁ yₜ₋₁ + ... + Aₚ yₜ₋ₚ + εₜ.

3. Prever **E[yₜ₊₁ | info até t]**:

   * μ_fatores,t+1 para fatores.
   * Se usar fatores, projetar μ_fatores em μ_ativos usando exposições de fator (betas, regressão cross-section).

4. Resultado:

   * μ_t (vector) com expected returns condicionais.

No contexto do backtest, também recalibrado **por rebalance**, usando janela móvel.

---

## 6. Portfolio Engine – alocação com restrições

Arquivo: `autofinance_portfolio_engine.R`

### 6.1 Config

```r
af_port_default_config <- list(
  cov_method    = "sample",      # repassado para risk_models
  mu_method     = "mean",        # idem
  window_years  = 3,
  mode          = "min_var",     # "equal", "inv_vol", "min_var", "mean_var", "max_sharpe"
  long_only     = TRUE,
  w_max         = 0.20,          # máximo por ativo
  leverage_max  = 1.0,           # sum(w_i) <= 1, sem alavancagem
  group_constraints = NULL       # ex: list(FII_min = 0.2, BDR_max = 0.3)
)
```

### 6.2 Interface

```r
af_build_portfolio <- function(
  panel_returns,     # retornos diários para selected_symbols
  selected_symbols,  # vetores de tickers escolhidos (p.ex. top N do screener)
  port_config,
  macro_panel = NULL # se risk_models precisar
) {
  # 1) Chama af_risk_estimate() com methods do port_config
  # 2) Resolve problema de otimização conforme mode:
  #    - equal, inv_vol, min_var, mean_var, max_sharpe
  # 3) Aplica restrições: long_only, w_max, leverage_max, group_constraints
  # 4) Retorna list(weights, cov_mat, mu_vec, stats)
}
```

### 6.3 Modos de alocação

* **"equal"**:

  * wᵢ = 1/N com restrições de grupo (se houver).

* **"inv_vol"**:

  * wᵢ ∝ 1/σᵢ (σᵢ pode vir de Σ ou de estimador simples).

* **"min_var"**:

  * Resolver:

    * min wᵀ Σ w
      s.a.

      * wᵢ ≥ 0 (se long_only)
      * ∑ wᵢ = 1
      * wᵢ ≤ w_max
      * * group_constraints (por ex. ∑ wᵢ (i em BDR) ≤ 0.3).

* **"mean_var"**:

  * Problema de Markowitz:

    * max [wᵀ μ − λ · wᵀ Σ w], para algum λ (aversion risk).

* **"max_sharpe"**:

  * max ( (wᵀ μ − r_f) / √(wᵀ Σ w) )
    com mesmas restrições.

Implementação com `quadprog`, `ROI` + plugins, ou `nloptr`, conforme preferência.

---

## 7. Backtest Core – colando screener + risk + portfolio ao longo do tempo

Arquivo: `autofinance_backtest_core.R`

### 7.1 Config geral

```r
af_backtest_config_default <- list(
  rebalance_freq = "monthly",    # "monthly", "quarterly", etc.
  lookback_years = 3,
  screener_config = af_screener_config_default,
  risk_config     = af_risk_config_default,
  port_config     = af_port_default_config,
  rf_series_id    = "CDI"
)
```

### 7.2 Interface

```r
af_backtest <- function(
  symbols_universe,    # pode ser NULL -> usar assets_meta ativos
  start_date,
  end_date,
  bt_config = af_backtest_config_default,
  con
) {
  # 1) Prepara painel ajustado + retornos + macros (de af_build_adjusted_panel / af_compute_returns)
  # 2) Gera sequência de datas de rebalance (ex: fim de cada mês)
  # 3) Loop ao longo dos rebalances:
  #    - t_reb é data de rebalance
  #    - define janela [t_reb - lookback_years, t_reb] (sem look-ahead)
  #    - roda screener nessa janela
  #    - escolhe selected_symbols
  #    - chama af_build_portfolio(...) para obter w_t
  #    - aplica retornos realizados de (t_reb, t_next] para atualizar carteira
  #    - registra trades, equity, etc.
  # 4) Retorna list(equity_curve, trades, stats, by_period)
}
```

### 7.3 Propriedades importantes

* **Sem HTTP** dentro do loop:

  * Toda a sincronização (`af_sync_*`) deve ter ocorrido **antes** do backtest.
  * O backtest só lê do SQLite e trabalha em memória.

* **Sem look-ahead**:

  * Screener, risk_models, portfolio engine sempre usam dados apenas até t_reb.

* **Recalibração por rebalance**:

  * GARCH/DCC/VAR são recalibrados apenas em t_reb, usando janela de lookback (ex: 3 anos).
  * Isso torna o custo computacional administrável.

---

## 8. Fiscal & Trading Rules – camada “net-of-tax”

Arquivo: `autofinance_fiscal_trading.R`

### 8.1 Papel

Não altera screener, Σ ou μ.
Pega a **sequência de trades/posições** do backtest e aplica:

* Regras de IR para:

  * Ações locais (isenção dos 20k, 15% ganho, etc., conforme legislação que você quiser codar).
  * BDRs (sem isenção, alíquotas diferentes).
  * FIIs (tratamento de rendimentos vs ganho de capital).
* Custos operacionais:

  * Corretagem, emolumentos, spread, etc.

### 8.2 Interface

```r
af_apply_fiscal_trading <- function(
  trades,          # data.table: date, symbol, qty, price, side
  rf_series = NULL,
  rules_config
) {
  # 1) Reconstruir posição por ativo ao longo do tempo
  # 2) Identificar eventos tributáveis por classe
  # 3) Calcular imposto devido, custos, eventualmente acumular "caixa"
  # 4) Ajustar equity_curve do backtest para "net-of-tax"
  # 5) Retornar curva líquida + stats
}
```

---

## 9. Configuração central e orquestração

Arquivo: `autofinance_config.R`

* `af_default_paths` – caminho do DB.
* `af_screener_config_default`.
* `af_risk_config_default`.
* `af_port_default_config`.
* `af_backtest_config_default`.

Função helper:

```r
af_sync_all <- function(con) {
  af_sync_b3(con = con)
  af_sync_macro(con = con, ...)
  af_sync_splits(con = con)
  # (dividends mais tarde, se/quando usar)
}
```

---

## 10. Roadmap de desenvolvimento com GARCH/DCC/VAR já planejados

### Fase 1 – DB + B3

* Implementar `af_db_init`, `af_db_connect`.
* Implementar `af_sync_b3` + ingestão COTAHIST → `prices_raw`.
* Preencher `assets_meta` inicial (tipo de ativo, active/deslistado).

### Fase 2 – Splits + painel ajustado

* Implementar `af_sync_splits` → `corporate_actions` (SPLIT).
* Implementar `af_build_adjusted_panel` e validar com alguns tickers vs Yahoo Adjusted Close.

### Fase 3 – Macro

* Implementar `af_sync_macro` (CDI, Selic, IPCA, USD, IBOV, etc.).
* Validar séries.

### Fase 4 – Retornos + Screener base

* Implementar `af_compute_returns`.
* Implementar `af_compute_metrics` (momentum, vol, drawdown, betas simples, etc.).
* Implementar `af_run_screener` com ranking por classe.

### Fase 5 – Risk models (sample & shrinkage) + portfolio engine básico

* Implementar `af_risk_estimate` com `cov_method = "sample"/"shrinkage"`, `mu_method = "mean"`.
* Implementar `af_build_portfolio` com modos "equal", "inv_vol", "min_var" sob restrições básicas.
* Validar com testes unitários e casos simples.

### Fase 6 – GARCH + DCC

* Integrar `rugarch` para GARCH univariado.
* Integrar `rmgarch` para DCC.
* Implementar `cov_method = "garch_dcc"`:

  * Por rebalance: calibrar GARCH/DCC na janela de lookback.
  * Extrair Σₜ condicional.

### Fase 7 – VAR para μ

* Definir vetor de variáveis (retornos de fatores ou ativos principais).
* Usar `vars` ou equivalente para ajustar VAR(p).
* Implementar `mu_method = "var"`:

  * Prever μₜ+₁ para fatores.
  * Mapear μ_fatores → μ_ativos (via betas/factor loadings).

### Fase 8 – Backtest core

* Implementar `af_backtest`:

  * Loop por rebalance.
  * Em cada rebalance:

    * screener → selected_symbols
    * risk_models → μ, Σ
    * portfolio_engine → weights
  * Aplicar retornos, registrar trades, equity, stats.

### Fase 9 – Fiscal/trading

* Implementar `af_apply_fiscal_trading`.
* Configurar regras de impostos/custos para Ações, FIIs, BDRs.
* Gerar curva bruta vs curva líquida.

### Fase 10 – Tuning e grid search

* Sobre o backtest core:

  * Rodar variações de `score_weights` do screener, métodos de Σ e μ.
  * Encontrar combinações que maximizem métricas (Sharpe, MAR, etc.).
  * Isso vira um hyperparameter search, não “ML”, mas suficiente para calibrar o sistema.

---

Esse plano já assume, desde o começo, que:

* **GARCH/DCC e VAR são cidadãos de primeira classe** no módulo de modelos de risco.
* O **portfolio engine** é construído explicitamente para receber Σₜ e μₜ condicional desses modelos.
* O **backtest** é o orquestrador real, com janelas móveis e re-estimação em cada rebalance.
* A camada fiscal/trading é acoplada apenas ao nível de trades/posições, sem contaminar screener ou risk models.

Se você quiser, o próximo passo natural é pegarmos **Fase 1–2** e já rascunharmos os skeletons de funções e arquivos (sem implementação pesada) para começar a preencher o projeto de forma consistente com esse `PLAN.md`.
