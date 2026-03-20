#!/usr/bin/env python3
"""CCASS Sentinel — Daily Runner

Scrapes today's CCASS for all watchlist stocks, appends to time-series
dataset, runs anomaly detection, outputs alerts.

Usage:
    python daily_runner.py                        # auto-detect yesterday's settlement date
    python daily_runner.py --date 2026/03/19      # specific date
    python daily_runner.py --dry-run              # scrape but don't commit
"""

import argparse, json, os, sys, time, re, math
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import threading, random

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
TIMESERIES_FILE = DATA_DIR / "ccass_timeseries.json"  # Tier 1: metrics only (~40KB/day)
HOLDERS_DIR = DATA_DIR / "holders"                     # Tier 2: full holders (~1.3MB/day, git-lfs)
ALERTS_DIR = REPO_ROOT / "alerts"

HKEX_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
PAT = re.compile(
    r'<div class="mobile-list-body">([ABC]\d{5})</div>\s*</td>\s*'
    r'<td class="col-participant-name">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-address">.*?</td>\s*'
    r'<td class="col-shareholding text-right">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">([\d,]+)</div>', re.DOTALL)
WORKERS = 3  # Conservative for daily runs
JITTER = (0.8, 2.0)

# ── Scraping (reuse proven architecture from collector_v5) ──────────────

_tls = threading.local()

def get_session():
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
        _tls.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
    return _tls.session

def get_viewstate():
    s = get_session()
    r = s.get(HKEX_URL, timeout=(5, 15))
    r.raise_for_status()
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    ev = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]*)"', r.text)
    if not all([vs, vsg]):
        return None
    return {"vs": vs.group(1), "vsg": vsg.group(1), "ev": ev.group(1) if ev else ""}

def scrape_stock(code, date_str, viewstate):
    """Scrape a single stock's CCASS data. Returns list of holders or None."""
    s = get_session()
    time.sleep(random.uniform(*JITTER))
    
    # Match proven collector POST body exactly
    data = {
        "__EVENTTARGET": "btnSearch",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate["vs"],
        "__VIEWSTATEGENERATOR": viewstate["vsg"],
        "today": date_str.replace("/", ""),
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "originalShareholdingDate": "",
        "alertMsg": "",
        "txtShareholdingDate": date_str,
        "txtStockCode": code,
        "txtStockName": "",
        "txtParticipantID": "",
        "txtParticipantName": "",
        "txtSelPartID": "",
    }
    # Only include EventValidation if present
    if viewstate.get("ev"):
        data["__EVENTVALIDATION"] = viewstate["ev"]
    
    try:
        r = s.post(HKEX_URL, data=data, timeout=(3.05, 20))
        if r.status_code != 200:
            return None
        if len(r.text) < 15000:
            return None
        
        matches = PAT.finditer(r.text)
        holders = sorted(
            [{"pid": m.group(1), "name": m.group(2).strip(),
              "shares": int(m.group(3).replace(",", ""))}
             for m in matches],
            key=lambda x: x["shares"], reverse=True
        )
        return holders if holders else None
    except Exception as e:
        print(f"  ❌ {code}: {e}")
        return None


def analyze(holdings):
    """Option A concentration metrics."""
    if not holdings:
        return {}
    total = sum(h["shares"] for h in holdings)
    if total == 0:
        return {}

    a5_shares = sum(h["shares"] for h in holdings if h["pid"] == "A00005")
    adjusted_float = total - a5_shares
    is_h_share = a5_shares / total > 0.3

    adj = [h for h in holdings if h["pid"] != "A00005"]
    brokers = [h for h in adj if h["pid"].startswith("B")]

    if adjusted_float > 0:
        adj_top5 = sum(h["shares"] for h in adj[:5]) / adjusted_float * 100
        broker_top5 = sum(h["shares"] for h in brokers[:5]) / adjusted_float * 100
        adj_hhi = sum((h["shares"] / adjusted_float * 100) ** 2 for h in adj)
    else:
        adj_top5 = broker_top5 = adj_hhi = 0

    top_broker = brokers[0] if brokers else None
    futu = next((h["shares"] for h in holdings if h["pid"] == "B01955"), 0)

    return {
        "date": None,  # filled by caller
        "total_shares": total,
        "a00005_pct": round(a5_shares / total * 100, 2),
        "is_h_share": is_h_share,
        "adjusted_float": adjusted_float,
        "adj_top5_pct": round(adj_top5, 2),
        "adj_hhi": round(adj_hhi, 1),
        "broker_top5_pct": round(broker_top5, 2),
        "top_broker_id": top_broker["pid"] if top_broker else "",
        "top_broker_name": top_broker["name"][:40] if top_broker else "",
        "top_broker_pct": round(top_broker["shares"] / adjusted_float * 100, 2) if top_broker and adjusted_float > 0 else 0,
        "futu_pct": round(futu / adjusted_float * 100, 2) if adjusted_float > 0 else 0,
        "participant_count": len(holdings),
        "holders": [{"pid": h["pid"], "name": h["name"], "shares": h["shares"]} for h in holdings],
    }


# ── Anomaly Detection ──────────────────────────────────────────────────

def detect_anomalies(code, today_data, history):
    """Compare today's snapshot against historical baseline. Returns list of alerts."""
    alerts = []
    if not today_data or not history:
        return alerts
    
    # Get most recent prior snapshot
    prior_dates = sorted(history.keys())
    if not prior_dates:
        return alerts
    prior = history[prior_dates[-1]]
    
    bt5_now = today_data.get("broker_top5_pct", 0)
    bt5_prior = prior.get("broker_top5_pct", 0)
    bt5_delta = bt5_now - bt5_prior
    
    # Alert 1: Broker concentration jumped >3pp in one day
    if bt5_delta > 3:
        alerts.append({
            "type": "BROKER_SPIKE",
            "severity": "HIGH",
            "message": f"{code}: BrkT5 jumped {bt5_delta:+.1f}pp in 1 day ({bt5_prior:.1f}%→{bt5_now:.1f}%)",
        })
    
    # Alert 2: Participant count dropped >10% in one day
    parts_now = today_data.get("participant_count", 0)
    parts_prior = prior.get("participant_count", 0)
    if parts_prior > 0 and (parts_now - parts_prior) / parts_prior < -0.10:
        alerts.append({
            "type": "PARTICIPANT_DROP",
            "severity": "MEDIUM",
            "message": f"{code}: Participant count dropped {parts_now-parts_prior} ({parts_prior}→{parts_now})",
        })
    
    # Alert 3: Cluster broker appeared or grew >5pp
    # Note: prior data from Tier 1 may not have holders. Load from Tier 2 if available.
    cluster_pids = {"B02082", "B02120", "B02165", "B01959"}
    af = today_data.get("adjusted_float", 0)
    prior_holders = prior.get("holders", [])
    prior_af = prior.get("adjusted_float", 0)
    if af > 0 and prior_holders and prior_af > 0:
        prior_map = {h["pid"]: h["shares"] / prior_af * 100 for h in prior_holders}
        for h in today_data.get("holders", []):
            if h["pid"] in cluster_pids:
                pct_now = h["shares"] / af * 100
                pct_prior = prior_map.get(h["pid"], 0)
                if pct_now - pct_prior > 5:
                    alerts.append({
                        "type": "CLUSTER_ALERT",
                        "severity": "CRITICAL",
                        "message": f"{code}: Cluster broker {h['pid']} grew {pct_prior:.1f}%→{pct_now:.1f}% (+{pct_now-pct_prior:.1f}pp)",
                    })
    
    # Alert 4: Float expanded >5% (lock-up deposit)
    total_now = today_data.get("total_shares", 0)
    total_prior = prior.get("total_shares", 0)
    if total_prior > 0 and (total_now - total_prior) / total_prior > 0.05:
        pct = (total_now - total_prior) / total_prior * 100
        alerts.append({
            "type": "FLOAT_EXPANSION",
            "severity": "MEDIUM",
            "message": f"{code}: Float expanded {pct:+.1f}% in 1 day ({total_prior:,}→{total_now:,})",
        })
    
    return alerts


# ── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CCASS Sentinel Daily Runner")
    parser.add_argument("--date", help="CCASS date (YYYY/MM/DD). Default: yesterday")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but don't save")
    args = parser.parse_args()
    
    # Date
    if args.date:
        date_str = args.date
    else:
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y/%m/%d")
    
    date_key = date_str.replace("/", "-")
    print(f"🛰️  CCASS Sentinel Daily Runner — {date_str}")
    
    # Load watchlist
    if not WATCHLIST_FILE.exists():
        print(f"  ❌ No watchlist at {WATCHLIST_FILE}")
        sys.exit(1)
    
    watchlist = json.loads(WATCHLIST_FILE.read_text())
    codes = [w["code"] for w in watchlist]
    print(f"  Watchlist: {len(codes)} stocks")
    
    # Load existing time-series
    if TIMESERIES_FILE.exists():
        ts = json.loads(TIMESERIES_FILE.read_text())
    else:
        ts = {}
    
    # Check if already collected
    already = sum(1 for c in codes if c in ts and date_key in ts[c])
    if already == len(codes):
        print(f"  ✅ Already collected {date_str} for all {len(codes)} stocks")
        return
    
    print(f"  Already have: {already}/{len(codes)}. Collecting remainder...")
    
    # Get ViewState
    vs = get_viewstate()
    if not vs:
        print("  ❌ Failed to get ViewState")
        sys.exit(1)
    
    # Scrape
    collected = 0
    errors = 0
    all_alerts = []
    
    targets = [c for c in codes if c not in ts or date_key not in ts.get(c, {})]
    
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for code in targets:
            f = pool.submit(scrape_stock, code, date_str, vs)
            futures[f] = code
        
        done_set = set()
        while futures:
            done, _ = wait(futures.keys() - done_set, return_when=FIRST_COMPLETED)
            for f in done:
                done_set.add(f)
                code = futures[f]
                holders = f.result()
                
                if holders:
                    metrics = analyze(holders)
                    metrics["date"] = date_key
                    
                    # Initialize stock in timeseries
                    if code not in ts:
                        ts[code] = {}
                    
                    # Inject prior holders from Tier 2 for cluster detection
                    prior_dates = sorted(ts.get(code, {}).keys())
                    if prior_dates:
                        last_date = prior_dates[-1]
                        if "holders" not in ts[code][last_date]:
                            holder_file = HOLDERS_DIR / f"{last_date}.json"
                            if holder_file.exists():
                                try:
                                    hdata = json.loads(holder_file.read_text())
                                    if code in hdata:
                                        ts[code][last_date]["holders"] = hdata[code]
                                except: pass
                    
                    # Detect anomalies vs prior
                    alerts = detect_anomalies(code, metrics, ts[code])
                    all_alerts.extend(alerts)
                    
                    # Store
                    ts[code][date_key] = metrics
                    collected += 1
                else:
                    errors += 1
                
                if (collected + errors) % 10 == 0:
                    print(f"  Progress: {collected} collected, {errors} errors")
            
            if done_set == set(futures.keys()):
                break
    
    print(f"\n  ✅ Collected: {collected} | Errors: {errors}")
    
    # Save
    if not args.dry_run:
        # Tier 2: Save full holders separately
        HOLDERS_DIR.mkdir(parents=True, exist_ok=True)
        holders_file = HOLDERS_DIR / f"{date_key}.json"
        daily_holders = {}
        for code in codes:
            if code in ts and date_key in ts[code]:
                daily_holders[code] = ts[code][date_key].get("holders", [])
        if daily_holders:
            with open(holders_file, "w") as f:
                json.dump(daily_holders, f, ensure_ascii=False)
        
        # Tier 1: Strip holders from timeseries (metrics only)
        for code in ts:
            for dk in ts[code]:
                ts[code][dk].pop("holders", None)
        
        # Atomic write
        tmp = str(TIMESERIES_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(ts, f, ensure_ascii=False)
        os.replace(tmp, str(TIMESERIES_FILE))
        print(f"  💾 Metrics → {TIMESERIES_FILE}")
        print(f"  💾 Holders → {holders_file}")
        print(f"  📊 Total: {len(ts)} stocks, {sum(len(v) for v in ts.values())} snapshots")
    
    # Alerts
    if all_alerts:
        print(f"\n  🚨 {len(all_alerts)} ALERTS:")
        for a in sorted(all_alerts, key=lambda x: {"CRITICAL":0,"HIGH":1,"MEDIUM":2}.get(x["severity"],3)):
            icon = {"CRITICAL":"🔴","HIGH":"🟠","MEDIUM":"🟡"}.get(a["severity"], "⚪")
            print(f"    {icon} [{a['type']}] {a['message']}")
        
        # Save alerts
        if not args.dry_run:
            alert_file = ALERTS_DIR / f"alerts_{date_key}.json"
            alert_file.parent.mkdir(parents=True, exist_ok=True)
            alert_file.write_text(json.dumps(all_alerts, indent=2, ensure_ascii=False))
            print(f"  📝 Alerts saved to {alert_file}")
    else:
        print(f"\n  ✅ No anomalies detected")
    
    # Summary stats
    print(f"\n  📈 DAILY SUMMARY:")
    highlights = []
    for code in codes[:10]:  # Top 10 watchlist
        if code in ts and date_key in ts[code]:
            d = ts[code][date_key]
            line = (f"    {code}: BT5={d.get('broker_top5_pct',0):.1f}% AT5={d.get('adj_top5_pct',0):.1f}% "
                    f"Parts={d.get('participant_count',0)} TopBrk={d.get('top_broker_name','')[:15]}")
            print(line)
            highlights.append(f"{code} BT5={d.get('broker_top5_pct',0):.1f}% {d.get('top_broker_name','')[:12]}")
    
    # ── Telegram Push ──
    if not args.dry_run:
        try:
            from telegram_push import push_daily_summary, push_alerts, push_error
            total_snaps = sum(len(v) for v in ts.values())
            push_daily_summary(date_key, collected, errors, len(ts), total_snaps, highlights)
            if all_alerts:
                push_alerts(date_key, all_alerts)
            print(f"  📱 Telegram push sent")
        except Exception as e:
            print(f"  ⚠️ Telegram push failed: {e}")


if __name__ == "__main__":
    main()
