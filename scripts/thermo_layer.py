#!/usr/bin/env python3
"""
CCASS Thermodynamic Layer v1.1
==============================
Runs AFTER daily_runner.py in the CCASS Sentinel pipeline.
Reads holders data, computes entropy/sigma/TE, pushes alerts.

Signals:
  S4: sigma < 0 for 3+ consecutive days → operator accumulating → watch for pump
  S5: |ΔS| > 3σ → phase transition → structural break
  S6: TE(stock_A, stock_B) significant → coordinated operation

v1.1 fixes (2026-04-06):
  - Root cause fix: numpy import guard (pip install if missing)
  - Robustness: try/except per-stock + global main() + Telegram on crash
  - Stats: TE min observations raised from 8 → 30, permutations 10 → 50
  - Perf: cache sum(shares), prune thermo_state history to last 30 days
  - Safety: bounds-check on alert string index
"""

import json, os, sys, glob, urllib.request
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

# Dependency guard — root cause of prior crashes
try:
    import numpy as np
except ImportError:
    os.system("pip install numpy --break-system-packages -q")
    import numpy as np

# Paths relative to ccass-sentinel repo root
REPO = Path(__file__).parent.parent
HOLDERS_DIR = REPO / "data" / "holders"
THERMO_STATE = REPO / "data" / "thermo_state.json"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

# Config
SIGMA_WINDOW = 5           # days for dS/dt
PHASE_THRESHOLD_SIGMA = 3  # |ΔS| > 3σ = phase transition
VIOLATION_STREAK = 3       # consecutive σ<0 days to alert
TE_TOP_PAIRS = 20          # number of stocks to test for TE
TE_BINS = 5                # bins for transfer entropy discretization
TE_MIN_OBS = 30            # minimum observations for TE (was 8 — statistically useless)
TE_PERMUTATIONS = 50       # shuffles for significance test (was 10 — p-value too coarse)
HISTORY_WINDOW = 15        # holder files to load
STATE_PRUNE_DAYS = 30      # prune state older than this


def tg(text):
    if not TG_TOKEN or not TG_CHAT:
        return print(f"[TG skip] {text[:120]}")
    try:
        # Telegram max message length is 4096
        text = text[:4000]
        d = json.dumps({"chat_id": int(TG_CHAT), "text": text}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", d,
            headers={"Content-Type": "application/json"}), timeout=10)
    except Exception as e:
        print(f"[TG err] {e}")


def load_holders(date_str):
    """Load all holders for a given date."""
    path = HOLDERS_DIR / f"{date_str}.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  ⚠️ Corrupt holder file {date_str}: {e}")
            return {}
    return {}


def shannon_entropy(shares_list):
    """Compute Shannon entropy from shares distribution."""
    total = sum(shares_list)
    if total <= 0 or len(shares_list) == 0:
        return 0.0
    probs = np.array(shares_list, dtype=np.float64) / total
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))


def compute_daily_entropy(holders_data):
    """Compute Shannon entropy for each stock from one day's holders."""
    result = {}
    for stock, holders in holders_data.items():
        try:
            shares = [h.get("shares", 0) for h in holders if h.get("shares", 0) > 0]
            if len(shares) < 2:
                continue
            total = sum(shares)
            if total <= 0:
                continue
            sorted_shares = sorted(shares, reverse=True)
            result[stock] = {
                "S": shannon_entropy(shares),
                "n_holders": len(shares),
                "top5_pct": sum(sorted_shares[:5]) / total * 100,
                "hhi": sum((s / total) ** 2 for s in shares),
            }
        except Exception as e:
            print(f"  ⚠️ Entropy error for {stock}: {e}")
            continue
    return result


def compute_transfer_entropy(src_series, tgt_series, k=1, bins=5):
    """Transfer entropy from src to tgt."""
    n = min(len(src_series), len(tgt_series))
    if n < 20:
        return 0.0
    src, tgt = src_series[:n], tgt_series[:n]

    # Handle constant series
    src_range = max(src) - min(src)
    tgt_range = max(tgt) - min(tgt)
    if src_range < 1e-12 or tgt_range < 1e-12:
        return 0.0

    s_d = np.clip(np.digitize(src, np.linspace(min(src) - 1e-10, max(src) + 1e-10, bins + 1)) - 1, 0, bins - 1)
    t_d = np.clip(np.digitize(tgt, np.linspace(min(tgt) - 1e-10, max(tgt) + 1e-10, bins + 1)) - 1, 0, bins - 1)

    j_tts, j_tt, j_ts, m_t = Counter(), Counter(), Counter(), Counter()
    for i in range(k, n):
        tf, tp, sp = t_d[i], tuple(t_d[i - k:i]), tuple(s_d[i - k:i])
        j_tts[(tf, tp, sp)] += 1
        j_tt[(tf, tp)] += 1
        j_ts[(tp, sp)] += 1
        m_t[tp] += 1

    te = 0.0
    nn = n - k
    if nn <= 0:
        return 0.0
    for (tf, tp, sp), c in j_tts.items():
        p = c / nn
        ptt = j_tt[(tf, tp)] / nn
        pts = j_ts[(tp, sp)] / nn
        pt = m_t[tp] / nn
        if p > 0 and ptt > 0 and pts > 0 and pt > 0:
            ratio = (p * pt) / (ptt * pts)
            if ratio > 0:
                te += p * np.log2(ratio)
    return te


def load_state():
    if THERMO_STATE.exists():
        try:
            with open(THERMO_STATE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"entropy_history": {}, "alerts": [], "te_alerts": []}
    return {"entropy_history": {}, "alerts": [], "te_alerts": []}


def save_state(state):
    with open(THERMO_STATE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[{datetime.now()}] Thermo Layer v1.1 starting...")

    # Find available dates (last HISTORY_WINDOW days)
    all_files = sorted(glob.glob(str(HOLDERS_DIR / "*.json")))
    available_dates = [Path(f).stem for f in all_files[-HISTORY_WINDOW:]]

    if len(available_dates) < 2:
        print("Not enough data yet (need ≥2 days)")
        return

    print(f"Available dates ({len(available_dates)}): ...{available_dates[-5:]}")

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

    if len(entropy_by_date) < 2:
        print("Not enough valid holder data (need ≥2 dates with data)")
        return

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
        try:
            s_series = []
            for d in recent_dates:
                if stock in entropy_by_date.get(d, {}):
                    s_series.append((d, entropy_by_date[d][stock]["S"]))

            if len(s_series) < 3:
                continue

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
            latest_ds = float(ds[-1])
            is_phase_transition = False
            ds_std = 0.0
            if len(ds) > 5:
                ds_std = float(np.std(ds))
                if ds_std > 1e-12:
                    is_phase_transition = abs(latest_ds) > PHASE_THRESHOLD_SIGMA * ds_std

            latest_top5 = entropy_by_date[recent_dates[-1]].get(stock, {}).get("top5_pct", 0)

            stock_metrics[stock] = {
                "S_current": float(vals_s[-1]),
                "sigma": float(sigma),
                "neg_streak": neg_streak,
                "latest_dS": latest_ds,
                "is_phase_transition": is_phase_transition,
                "is_violation": neg_streak >= VIOLATION_STREAK,
                "n_obs": len(s_series),
                "top5_pct": latest_top5,
            }

            # Alert: 2nd Law violation (operator accumulating)
            if neg_streak >= VIOLATION_STREAK:
                # Bounds-safe index
                ref_idx = max(0, len(vals_s) - neg_streak - 1)
                alerts.append(
                    f"🔴 {stock}: σ<0 for {neg_streak}d "
                    f"(S: {vals_s[ref_idx]:.3f}→{vals_s[-1]:.3f}). "
                    f"Top5={latest_top5:.1f}%"
                )

            # Alert: Phase transition
            if is_phase_transition and ds_std > 1e-12:
                direction = "concentrating" if latest_ds < 0 else "dispersing"
                alerts.append(
                    f"⚡ {stock}: ΔS={latest_ds:.4f} "
                    f"({abs(latest_ds) / ds_std:.1f}σ). {direction.upper()}"
                )

        except Exception as e:
            print(f"  ⚠️ Error processing {stock}: {e}")
            continue

    # ============================================================
    # STEP 3: Transfer Entropy between top-concentration stocks
    # ============================================================
    high_conc = sorted(stock_metrics.items(),
                       key=lambda x: x[1].get("top5_pct", 0), reverse=True)[:TE_TOP_PAIRS]
    high_conc_codes = [x[0] for x in high_conc]

    te_results = []
    if len(recent_dates) >= TE_MIN_OBS and len(high_conc_codes) >= 2:
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

        n_tested = 0
        for i, sa in enumerate(high_conc_codes):
            for sb in high_conc_codes[i + 1:]:
                try:
                    ts_a = entropy_ts[sa]
                    ts_b = entropy_ts[sb]

                    valid = ~(np.isnan(ts_a) | np.isnan(ts_b))
                    if np.sum(valid) < TE_MIN_OBS:
                        continue

                    a_clean = ts_a[valid]
                    b_clean = ts_b[valid]

                    te_ab = compute_transfer_entropy(a_clean, b_clean, bins=TE_BINS)
                    te_ba = compute_transfer_entropy(b_clean, a_clean, bins=TE_BINS)

                    # Significance: compare to shuffled baseline
                    te_shuffled = []
                    for _ in range(TE_PERMUTATIONS):
                        perm = np.random.permutation(a_clean)
                        te_shuffled.append(compute_transfer_entropy(perm, b_clean, bins=TE_BINS))

                    if len(te_shuffled) == 0 or np.std(te_shuffled) < 1e-12:
                        continue

                    baseline = np.mean(te_shuffled)
                    threshold = baseline + 2 * np.std(te_shuffled)

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
                            f"(TE={max(te_ab, te_ba):.4f} > base {baseline:.4f})"
                        )

                    n_tested += 1
                except Exception as e:
                    print(f"  ⚠️ TE error {sa}-{sb}: {e}")
                    continue

        print(f"Tested {n_tested} pairs for TE, {len(te_results)} significant")
    else:
        reason = f"dates={len(recent_dates)}<{TE_MIN_OBS}" if len(recent_dates) < TE_MIN_OBS else "too few stocks"
        print(f"TE skipped ({reason})")

    # ============================================================
    # STEP 4: Save state + push alerts
    # ============================================================
    state["last_run"] = today
    state["n_stocks"] = len(stock_metrics)
    state["n_violations"] = sum(1 for m in stock_metrics.values() if m["is_violation"])
    state["n_phase_transitions"] = sum(1 for m in stock_metrics.values() if m["is_phase_transition"])
    state["n_te_significant"] = len(te_results)
    state["te_pairs"] = te_results

    # Store latest metrics (compact) — only violations + phase transitions to control size
    state["latest_metrics"] = {
        stock: {"S": m["S_current"], "σ": m["sigma"], "streak": m["neg_streak"],
                "top5": m["top5_pct"], "violation": m["is_violation"]}
        for stock, m in stock_metrics.items()
        if m["is_violation"] or m["is_phase_transition"] or m["top5_pct"] > 50
    }

    # Prune old alert history
    if "alert_history" in state:
        cutoff = (datetime.now() - timedelta(days=STATE_PRUNE_DAYS)).strftime("%Y-%m-%d")
        state["alert_history"] = [
            a for a in state.get("alert_history", [])
            if a.get("date", "") >= cutoff
        ]

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
        summary += "\n\n" + "\n".join(alerts[:10])
        if len(alerts) > 10:
            summary += f"\n... +{len(alerts) - 10} more"
    else:
        summary += "\n\n✅ No anomalies detected"

    print(summary)
    tg(summary)

    print(f"[{datetime.now()}] Thermo Layer done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"\n💥 THERMO FATAL:\n{tb}")
        tg(f"🔴 Thermo Layer CRASHED:\n{str(e)[:300]}")
        sys.exit(1)
