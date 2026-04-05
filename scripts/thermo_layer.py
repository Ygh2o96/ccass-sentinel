#!/usr/bin/env python3
"""
CCASS Thermodynamic Layer
=========================
Runs AFTER daily_runner.py in the CCASS Sentinel pipeline.
Reads holders data, computes entropy/sigma/TE, pushes alerts.

Signals:
  S4: sigma < 0 for 3+ consecutive days → operator accumulating → watch for pump
  S5: |ΔS| > 3σ → phase transition → structural break
  S6: TE(stock_A, stock_B) significant → coordinated operation
"""

import json, os, sys, glob, urllib.request
import numpy as np
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

# Paths relative to ccass-sentinel repo root
REPO = Path(__file__).parent.parent  # assumes scripts/thermo_layer.py
HOLDERS_DIR = REPO / "data" / "holders"
THERMO_STATE = REPO / "data" / "thermo_state.json"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

# Config
SIGMA_WINDOW = 5          # days for dS/dt
PHASE_THRESHOLD_SIGMA = 3  # |ΔS| > 3σ = phase transition
VIOLATION_STREAK = 3       # consecutive σ<0 days to alert
TE_TOP_PAIRS = 20          # number of pairs to test for TE
TE_BINS = 5                # bins for transfer entropy discretization


def tg(text):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        d = json.dumps({"chat_id": int(TG_CHAT), "text": text}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", d,
            headers={"Content-Type": "application/json"}), timeout=10)
    except:
        pass


def load_holders(date_str):
    """Load all holders for a given date."""
    path = HOLDERS_DIR / f"{date_str}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def shannon_entropy(shares_list):
    """Compute Shannon entropy from shares distribution."""
    total = sum(shares_list)
    if total <= 0 or len(shares_list) == 0:
        return 0.0
    probs = np.array(shares_list) / total
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))


def compute_daily_entropy(holders_data):
    """Compute Shannon entropy for each stock from one day's holders."""
    result = {}
    for stock, holders in holders_data.items():
        shares = [h.get("shares", 0) for h in holders if h.get("shares", 0) > 0]
        if len(shares) >= 2:
            result[stock] = {
                "S": shannon_entropy(shares),
                "n_holders": len(shares),
                "top5_pct": sum(sorted(shares, reverse=True)[:5]) / sum(shares) * 100,
                "hhi": sum((s / sum(shares))**2 for s in shares),
            }
    return result


def compute_transfer_entropy(src_series, tgt_series, k=1, bins=5):
    """Transfer entropy from src to tgt."""
    n = min(len(src_series), len(tgt_series))
    if n < 20:
        return 0.0
    src, tgt = src_series[:n], tgt_series[:n]
    
    s_d = np.clip(np.digitize(src, np.linspace(min(src)-1e-10, max(src)+1e-10, bins+1)) - 1, 0, bins-1)
    t_d = np.clip(np.digitize(tgt, np.linspace(min(tgt)-1e-10, max(tgt)+1e-10, bins+1)) - 1, 0, bins-1)
    
    j_tts, j_tt, j_ts, m_t = Counter(), Counter(), Counter(), Counter()
    for i in range(k, n):
        tf, tp, sp = t_d[i], tuple(t_d[i-k:i]), tuple(s_d[i-k:i])
        j_tts[(tf,tp,sp)] += 1; j_tt[(tf,tp)] += 1
        j_ts[(tp,sp)] += 1; m_t[tp] += 1
    
    te = 0.0
    nn = n - k
    for (tf,tp,sp), c in j_tts.items():
        p = c/nn; ptt = j_tt[(tf,tp)]/nn
        pts = j_ts[(tp,sp)]/nn; pt = m_t[tp]/nn
        if p > 0 and ptt > 0 and pts > 0 and pt > 0:
            te += p * np.log2((p*pt)/(ptt*pts))
    return te


def load_state():
    if THERMO_STATE.exists():
        with open(THERMO_STATE) as f:
            return json.load(f)
    return {"entropy_history": {}, "alerts": [], "te_alerts": []}


def save_state(state):
    with open(THERMO_STATE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[{datetime.now()}] Thermo Layer starting...")

    # Find available dates (last 15 days)
    all_files = sorted(glob.glob(str(HOLDERS_DIR / "*.json")))
    available_dates = [Path(f).stem for f in all_files[-15:]]
    
    if len(available_dates) < 2:
        print("Not enough data yet (need ≥2 days)")
        return

    print(f"Available dates: {available_dates[-5:]}")
    
    state = load_state()
    alerts = []

    # ============================================================
    # STEP 1: Compute entropy for each available date
    # ============================================================
    entropy_by_date = {}
    for date_str in available_dates:
        holders = load_holders(date_str)
        if holders:
            entropy_by_date[date_str] = compute_daily_entropy(holders)

    # Get all stocks that appear in recent data
    recent_dates = sorted(entropy_by_date.keys())[-10:]
    all_stocks = set()
    for d in recent_dates:
        all_stocks.update(entropy_by_date[d].keys())

    print(f"Computing entropy for {len(all_stocks)} stocks across {len(recent_dates)} dates")

    # ============================================================
    # STEP 2: Compute sigma (dS/dt) and detect violations
    # ============================================================
    stock_metrics = {}
    for stock in all_stocks:
        s_series = []
        for d in recent_dates:
            if stock in entropy_by_date.get(d, {}):
                s_series.append((d, entropy_by_date[d][stock]["S"]))

        if len(s_series) < 3:
            continue

        dates_s = [x[0] for x in s_series]
        vals_s = np.array([x[1] for x in s_series])
        
        # dS/dt (daily change)
        ds = np.diff(vals_s)
        sigma = np.mean(ds[-SIGMA_WINDOW:]) if len(ds) >= SIGMA_WINDOW else np.mean(ds)
        
        # Count consecutive negative days (2nd Law violations)
        neg_streak = 0
        for v in reversed(ds):
            if v < 0:
                neg_streak += 1
            else:
                break

        # Phase transition: |ΔS| > 3σ of historical |ΔS|
        if len(ds) > 5:
            ds_std = np.std(ds)
            latest_ds = ds[-1]
            is_phase_transition = abs(latest_ds) > PHASE_THRESHOLD_SIGMA * ds_std if ds_std > 0 else False
        else:
            is_phase_transition = False
            latest_ds = ds[-1] if len(ds) > 0 else 0

        stock_metrics[stock] = {
            "S_current": float(vals_s[-1]),
            "sigma": float(sigma),
            "neg_streak": neg_streak,
            "latest_dS": float(latest_ds),
            "is_phase_transition": is_phase_transition,
            "is_violation": neg_streak >= VIOLATION_STREAK,
            "n_obs": len(s_series),
            "top5_pct": entropy_by_date[recent_dates[-1]].get(stock, {}).get("top5_pct", 0),
        }

        # Alert: 2nd Law violation (operator accumulating)
        if neg_streak >= VIOLATION_STREAK:
            alerts.append(
                f"🔴 {stock}: σ<0 for {neg_streak} consecutive days "
                f"(S: {vals_s[-neg_streak-1]:.3f}→{vals_s[-1]:.3f}). "
                f"Operator accumulating? Top5={stock_metrics[stock]['top5_pct']:.1f}%"
            )

        # Alert: Phase transition
        if is_phase_transition:
            direction = "concentrating" if latest_ds < 0 else "dispersing"
            alerts.append(
                f"⚡ {stock}: Phase transition! ΔS={latest_ds:.4f} "
                f"({abs(latest_ds)/ds_std:.1f}σ). {direction.upper()}"
            )

    # ============================================================
    # STEP 3: Transfer Entropy between top-concentration stocks
    # ============================================================
    # Pick stocks with highest concentration for TE analysis
    high_conc = sorted(stock_metrics.items(),
                       key=lambda x: x[1].get("top5_pct", 0), reverse=True)[:TE_TOP_PAIRS]
    high_conc_codes = [x[0] for x in high_conc]

    te_results = []
    if len(recent_dates) >= 8 and len(high_conc_codes) >= 2:
        # Build entropy time series for TE
        entropy_ts = {}
        for stock in high_conc_codes:
            ts = []
            for d in recent_dates:
                if stock in entropy_by_date.get(d, {}):
                    ts.append(entropy_by_date[d][stock]["S"])
                else:
                    ts.append(np.nan)
            entropy_ts[stock] = np.array(ts)

        # Pairwise TE (only test top pairs to save time)
        n_tested = 0
        for i, sa in enumerate(high_conc_codes):
            for sb in high_conc_codes[i+1:]:
                ts_a = entropy_ts[sa]
                ts_b = entropy_ts[sb]
                
                # Remove NaN aligned
                valid = ~(np.isnan(ts_a) | np.isnan(ts_b))
                if np.sum(valid) < 8:
                    continue
                
                a_clean = ts_a[valid]
                b_clean = ts_b[valid]
                
                te_ab = compute_transfer_entropy(a_clean, b_clean, bins=TE_BINS)
                te_ba = compute_transfer_entropy(b_clean, a_clean, bins=TE_BINS)
                
                # Significance: compare to shuffled baseline
                te_shuffled = []
                for _ in range(10):
                    perm = np.random.permutation(a_clean)
                    te_shuffled.append(compute_transfer_entropy(perm, b_clean, bins=TE_BINS))
                
                baseline = np.mean(te_shuffled)
                threshold = np.mean(te_shuffled) + 2 * np.std(te_shuffled)
                
                net = te_ab - te_ba
                is_significant = max(te_ab, te_ba) > threshold
                
                if is_significant:
                    leader = sa if te_ab > te_ba else sb
                    follower = sb if te_ab > te_ba else sa
                    te_results.append({
                        "pair": f"{sa}-{sb}",
                        "te_ab": float(te_ab),
                        "te_ba": float(te_ba),
                        "net": float(net),
                        "baseline": float(baseline),
                        "leader": leader,
                        "follower": follower,
                    })
                    alerts.append(
                        f"🔗 TE: {leader}→{follower} "
                        f"(TE={max(te_ab,te_ba):.4f} > baseline {baseline:.4f}). "
                        f"Coordinated operation?"
                    )
                
                n_tested += 1

        print(f"Tested {n_tested} pairs for transfer entropy, {len(te_results)} significant")

    # ============================================================
    # STEP 4: Save state + push alerts
    # ============================================================
    # Update state
    state["last_run"] = today
    state["n_stocks"] = len(stock_metrics)
    state["n_violations"] = sum(1 for m in stock_metrics.values() if m["is_violation"])
    state["n_phase_transitions"] = sum(1 for m in stock_metrics.values() if m["is_phase_transition"])
    state["n_te_significant"] = len(te_results)
    state["te_pairs"] = te_results
    
    # Store latest metrics (compact)
    state["latest_metrics"] = {
        stock: {"S": m["S_current"], "σ": m["sigma"], "streak": m["neg_streak"],
                "top5": m["top5_pct"], "violation": m["is_violation"]}
        for stock, m in stock_metrics.items()
    }
    
    save_state(state)

    # Summary
    n_viol = state["n_violations"]
    n_phase = state["n_phase_transitions"]
    n_te = state["n_te_significant"]
    
    summary = (
        f"🌡️ CCASS Thermo — {today}\n"
        f"Stocks: {len(stock_metrics)} | Dates: {len(recent_dates)}\n"
        f"2nd Law violations: {n_viol} | Phase transitions: {n_phase}\n"
        f"TE significant pairs: {n_te}"
    )
    
    if alerts:
        summary += "\n\n" + "\n".join(alerts[:10])  # cap at 10 alerts
        if len(alerts) > 10:
            summary += f"\n... +{len(alerts)-10} more"
    else:
        summary += "\n\n✅ No anomalies detected"

    print(summary)
    tg(summary)
    
    print(f"[{datetime.now()}] Thermo Layer done.")


if __name__ == "__main__":
    main()
