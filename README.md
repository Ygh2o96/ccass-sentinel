# 🛰️ CCASS Sentinel

**Automated HKEX CCASS shareholding concentration scanner for Hong Kong IPOs.**

An open-source surveillance tool that scrapes daily CCASS data, computes concentration metrics, detects anomalous broker accumulation patterns, and identifies coordinated operator networks across 130+ IPOs.

> **March 2026 Context:** On March 10-11, 2026, the SFC and ICAC launched **Operation 熔断 (Circuit Breaker)** — raiding 14 locations and arresting 8 individuals including the ECM head of a major Chinese broker. The operation targeted coordinated IPO manipulation: bribery for placement intelligence, concentrated allocation structures, and pump-and-dump schemes. CCASS Sentinel independently identified several of the structural patterns now under prosecution, using only publicly available CCASS data.

## 📊 What This Does

- **Daily automated collection** of CCASS shareholding data for all tracked IPOs via GitHub Actions
- **Option A concentration framework**: strips only immobilized CSDC shares (A00005) from adjusted float; preserves Stock Connect as tradable liquidity
- **Dual-radar anomaly detection**: RADAR 1 (secondary market cornering) + RADAR 2 (lock-up expiry funnel)
- **Broker network topology**: co-occurrence analysis to identify coordinated operator clusters
- **Real-time alerting**: Telegram push for concentration spikes, cluster broker movements, participant drops, system errors

## 🔬 Case Study: 02706 海致科技 (BooleanAI)

Listed February 26, 2026. Cornerstone investor **Infini Capital (无极资本)** — whose principals were subsequently arrested in Operation 熔断.

**CCASS Day 1 snapshot:**

| Participant | D1 % | D14/Latest % | Pattern |
|---|---|---|---|
| SPDB International | 26.4% | 8.3% | Aggressive distribution (-18pp) |
| FUTU (retail) | 17.7% | 18.5% | Retail accumulating INTO the dump |
| Mouette Securities | 17.1% | 4.8% | Dumped 12pp in 14 days |
| Livermore Holdings | 2.6% | — | Present on D1 (cluster node) |

**BrkT5 at D1: 77.4%** — top quintile of our 132-IPO universe. Our empirical finding: D1 BrkT5 > 69% correlates with 50% probability of >50% drawdown (r = -0.376).

**Livermore Holdings (B02120)** appears on Day 1 with 2.6% of adjusted float. This is the same broker identified by CCASS Sentinel as a node in a 4-broker cluster across multiple IPOs, and as sole underwriter on another deal with structural placement concerns.

The stock rose +242% on Day 1, +500% over 3 days, then halved. The 90/10 international/public split with no clawback mechanism ensured concentrated allocation.

## 📈 Key Findings (132 IPOs, 109K holder records)

**46 empirical findings** across 8 categories. Selected highlights:

**Tradable Signals:**
- D1 Broker_Top5 predicts max drawdown with **r = −0.376**. BrkT5 > 69% → 50% crash probability.
- FUTU flow is a **contrarian signal**: stocks where FUTU accumulates post-IPO return median **−36.9%**; stocks where FUTU exits return **+2.1%** (39pp spread).
- Three independent concentration metrics (BrkT5, Shannon Entropy, Zipf α) all predict returns. BrkT5 and Zipf α are uncorrelated (r = 0.001).

**Network Analysis:**
- 4-broker cluster controlling 60–84% of adjusted float across 5 IPOs
- "Canary brokers" exit 91% of stocks that subsequently crash >50%
- Broker loyalty analysis reveals distinct entourages for each sponsor/placing agent

**Market Structure:**
- 6 IPO archetype taxonomy: Institutional, Stock Connect Darling, Generic, FUTU-Dominated, Operator-Controlled, Illiquid Orphan
- FUTU present in 131/131 IPOs — largest omnibus surveillance blind spot
- 5.9% of holders own 80% of float (Gini = 0.921)

## 🏗️ Architecture

```
.github/workflows/
  daily_collect.yml          ← Mon-Fri 18:00 HKT
  weekly_discover.yml        ← Saturday, auto-finds new listings
scripts/
  daily_runner.py            ← Scrape → analyze → detect → Telegram push
  discover_new_listings.py   ← Probes CCASS for new IPO codes
  telegram_push.py           ← Notification module
  ccass_scraper.py           ← Interactive single-stock scraper
  collector.py               ← Parallel bulk collector
  macro_topology.py          ← Market-wide anomaly analysis
  ipo_scanner.py             ← Day 1 protocol for new listings
data/
  watchlist.json             ← 137+ stocks, tiered priority
  ccass_timeseries.json      ← Tier 1: daily metrics
  holders/                   ← Tier 2: full holder lists per day
```

## 📐 Methodology

**Adjusted Float** = Total CCASS shares − A00005 (CSDC immobilized shares)

**Broker_Top5** = Top 5 B-prefix participants / Adjusted Float

**Important caveats:**
- CCASS shows **participant-level** holdings, not beneficial ownership. Broker accounts are omnibus.
- "Broker X holds Y%" means Y% flows through Broker X's account, not that Broker X owns Y%.
- Statistical co-occurrence does not prove common beneficial ownership or illegal coordination.

## 🚀 Quick Start

```bash
git clone https://github.com/Ygh2o96/ccass-sentinel.git
cd ccass-sentinel
pip install requests pandas
python scripts/daily_runner.py --date 2026/03/19
```

GitHub Actions runs automatically Mon-Fri 18:00 HKT.

## ⚠️ Disclaimer

This tool uses publicly available CCASS data from HKEX. It is an academic research project, not investment advice. No buy/sell recommendations are made. Statistical patterns do not constitute evidence of wrongdoing.

## 📄 License

MIT
