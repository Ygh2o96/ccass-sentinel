# 🛰️ CCASS Sentinel

**Automated HKEX CCASS shareholding concentration scanner for Hong Kong IPOs.**

Scrapes daily CCASS data, computes concentration metrics, detects anomalous broker accumulation patterns, and identifies coordinated operator networks across 132+ IPOs.

## 📊 What This Does

- **Daily automated collection** of CCASS shareholding data for all tracked IPOs via GitHub Actions
- **Option A concentration framework**: strips only immobilized CSDC shares (A00005) from adjusted float; preserves Stock Connect as tradable liquidity
- **Dual-radar anomaly detection**: RADAR 1 (secondary market cornering) + RADAR 2 (lock-up expiry funnel)
- **Broker network topology**: co-occurrence analysis to identify coordinated operator clusters
- **Real-time alerting**: spikes in broker concentration, cluster broker movements, participant drops

## 🏗️ Architecture

```
.github/workflows/daily_collect.yml   ← Runs Mon-Fri 18:00 HKT
scripts/
  daily_runner.py                      ← Scrape → analyze → detect → alert
  ccass_scraper.py                     ← Interactive single-stock scraper
  collector.py                         ← Parallel bulk collector
  macro_topology.py                    ← Market-wide anomaly analysis
data/
  watchlist.json                       ← 132 stocks, tiered priority
  ccass_timeseries.json                ← All historical snapshots + full holders
alerts/
  alerts_YYYY-MM-DD.json              ← Daily anomaly alerts
```

## 🚀 Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/ccass-sentinel.git
cd ccass-sentinel

# Install
pip install requests pandas

# Run manually
python scripts/daily_runner.py                    # yesterday's data
python scripts/daily_runner.py --date 2026/03/19  # specific date
python scripts/daily_runner.py --dry-run           # test without saving

# Interactive single-stock scan
python scripts/ccass_scraper.py --stock 02565 --date 2026/03/19

# Market-wide analysis
python scripts/macro_topology.py --dataset data/ccass_timeseries.json --output-dir results/
```

## 📐 Methodology

**Adjusted Float** = Total CCASS shares − A00005 (CSDC immobilized shares)

**Adj_Top5** = Top 5 non-CSDC participants / Adjusted Float (includes Stock Connect)

**Broker_Top5** = Top 5 B-prefix participants / Adjusted Float (the cornering signal)

**RADAR 1 triggers**: Static float (<5% change) + Broker_Top5 increase >2pp post-D30 + Broker_Top5 >30%

**RADAR 2 triggers**: Float expansion >20% + FUTU >10% (lock-up expiry deposits)

See the [research paper](CCASS_Sentinel_Research_Paper.docx) for full methodology.

## 📈 Key Findings (132 IPOs, March 2025 – March 2026)

- Median adjusted free-float concentration: **~73%, flat from D1 through D90+**
- D1 Broker_Top5 predicts max drawdown with **r = −0.376**
- **4-broker cluster** (Yellow River, Livermore, Yuen Meta, Zhongtai) controlling 60–84% of adjusted float across 5 IPOs
- Yuen Meta is a subsidiary of Roma Group (8072.HK), whose former Chairman was **arrested by ICAC** in 2017
- **46 empirical findings** across 8 categories (see Master Findings Registry)

## ⚠️ Disclaimer

This tool uses publicly available CCASS data from HKEX. It does not constitute investment advice. Statistical co-occurrence does not prove common beneficial ownership or illegal coordination.

## 📄 License

MIT
