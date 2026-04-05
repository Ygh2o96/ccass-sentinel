#!/usr/bin/env python3
"""CCASS Sentinel — New Listing Auto-Discovery

Scans CCASS for stock codes NOT in the current watchlist.
If CCASS returns data, it's a new listing → auto-add to watchlist.

Runs weekly (Saturday) via GitHub Actions.
Also callable manually: python scripts/discover_new_listings.py

Strategy:
  HKEX assigns stock codes in ranges. Recent IPOs cluster around:
  - 00xxx (legacy, rare for new IPOs)
  - 01xxx (Main Board, mixed)  
  - 02xxx (Main Board, high frequency 2024-2026)
  - 03xxx (Main Board)
  - 06xxx (Main Board)
  - 09xxx (Main Board, Chapter 18A/19C/21)
  
  We scan ~200 candidate codes around the latest watchlist codes.
  If CCASS returns holders for a code we don't track, it's new.
  False positive rate is near zero (CCASS only returns data for listed stocks).
"""

import json, os, re, time, random, sys
from pathlib import Path
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_FILE = REPO_ROOT / "data" / "watchlist.json"
HKEX_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"

# Configurable via env vars (set in GitHub Actions workflow)
MAX_CANDIDATES = int(os.environ.get("DISCOVERY_MAX_CANDIDATES", "300"))
WALL_CLOCK_SECS = int(os.environ.get("DISCOVERY_WALL_CLOCK_SECS", "4200"))  # 70 min default
VIEWSTATE_REFRESH_EVERY = 80  # refresh session every N checks


def get_viewstate():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    r = s.get(HKEX_URL, timeout=(5, 15))
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    ev = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]*)"', r.text)
    if not all([vs, vsg]):
        return None, s
    return {"vs": vs.group(1), "vsg": vsg.group(1), "ev": ev.group(1) if ev else ""}, s


def check_code(session, code, date_str, viewstate):
    """Returns True if CCASS has data for this code."""
    time.sleep(random.uniform(1.0, 2.5))
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
    if viewstate.get("ev"):
        data["__EVENTVALIDATION"] = viewstate["ev"]
    try:
        r = session.post(HKEX_URL, data=data, timeout=(3.05, 15))
        if r.status_code != 200:
            return False
        if len(r.text) < 15000:
            return False
        if "col-participant-id" in r.text:
            return True
        return False
    except:
        return False


def generate_candidates(existing_codes):
    """Generate stock codes to probe based on existing watchlist."""
    existing_nums = sorted(int(c) for c in existing_codes)
    candidates = set()
    
    # Strategy 1: Scan ±20 around the highest codes in each range
    for prefix in [1, 2, 3, 6, 9]:
        range_codes = [n for n in existing_nums if n // 1000 == prefix]
        if range_codes:
            max_code = max(range_codes)
            # Scan codes above the max (most likely new listings)
            for offset in range(1, 30):
                candidates.add(max_code + offset)
            # Also scan a few below (might have missed some)
            for offset in range(1, 10):
                candidates.add(max_code - offset)
    
    # Strategy 2: Common recent IPO ranges
    # 02700-02750, 03300-03400, 09600-09700
    for start, end in [(2700, 2750), (3300, 3400), (3600, 3700), (9600, 9700), (9900, 9990)]:
        for n in range(start, end + 1):
            candidates.add(n)
    
    # Remove existing
    candidates -= set(existing_nums)
    
    # Format as 5-digit strings, cap at MAX_CANDIDATES
    all_cands = sorted(f"{n:05d}" for n in candidates if 1 <= n <= 99999)
    if len(all_cands) > MAX_CANDIDATES:
        # Prioritize: codes just above max in each range (most likely new IPOs)
        # then fill with Strategy 2 ranges
        priority = []
        remainder = []
        for c in all_cands:
            n = int(c)
            prefix = n // 1000
            range_max = max((x for x in existing_nums if x // 1000 == prefix), default=0)
            if range_max and n > range_max and n <= range_max + 30:
                priority.append(c)
            else:
                remainder.append(c)
        all_cands = (priority + remainder)[:MAX_CANDIDATES]
    
    return all_cands


def main():
    start_time = time.time()
    print("🔍 CCASS Sentinel — New Listing Discovery")
    print(f"  Config: max_candidates={MAX_CANDIDATES}, wall_clock={WALL_CLOCK_SECS}s")
    
    # Load watchlist
    wl = json.loads(WATCHLIST_FILE.read_text())
    existing = {w["code"] for w in wl}
    print(f"  Current watchlist: {len(existing)} stocks")
    
    # Generate candidates
    candidates = generate_candidates(existing)
    print(f"  Candidate codes to probe: {len(candidates)}")
    
    # Yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    # If Saturday/Sunday, use Friday
    while yesterday.weekday() >= 5:
        yesterday -= timedelta(days=1)
    date_str = yesterday.strftime("%Y/%m/%d")
    print(f"  Probing date: {date_str}")
    
    # Get ViewState
    viewstate, session = get_viewstate()
    if not viewstate:
        print("  ❌ Failed to get ViewState")
        sys.exit(1)
    
    # Probe candidates
    discovered = []
    checked = 0
    for code in candidates:
        # Wall clock guard
        elapsed = time.time() - start_time
        if elapsed > WALL_CLOCK_SECS:
            print(f"  ⏰ Wall clock limit reached ({elapsed:.0f}s). Checked {checked}/{len(candidates)}.")
            break
        
        # Refresh viewstate periodically to avoid session expiry
        if checked > 0 and checked % VIEWSTATE_REFRESH_EVERY == 0:
            print(f"  🔄 Refreshing session (checked {checked})...")
            try:
                viewstate, session = get_viewstate()
                if not viewstate:
                    print("  ⚠️ ViewState refresh failed, continuing with old session")
            except Exception as e:
                print(f"  ⚠️ ViewState refresh error: {e}")
        
        if check_code(session, code, date_str, viewstate):
            discovered.append(code)
            print(f"  🆕 DISCOVERED: {code}")
        checked += 1
        if checked % 50 == 0:
            elapsed = time.time() - start_time
            print(f"  Checked {checked}/{len(candidates)} ({elapsed:.0f}s elapsed)...")
    
    print(f"\n  Checked: {checked} | Discovered: {len(discovered)}")
    
    if not discovered:
        print("  No new listings found.")
        return
    
    # Add to watchlist
    for code in discovered:
        wl.append({
            "code": code,
            "name": f"[AUTO-DISCOVERED {date_str}]",
            "tier": "HIGH",  # New listings get HIGH priority for close monitoring
        })
        print(f"  ➕ Added {code} to watchlist [HIGH]")
    
    wl.sort(key=lambda x: x["code"])
    WATCHLIST_FILE.write_text(json.dumps(wl, indent=2, ensure_ascii=False))
    print(f"  💾 Watchlist updated: {len(wl)} stocks")
    
    # Telegram push
    try:
        from telegram_push import push_discovery
        push_discovery(date_str, discovered)
        print(f"  📱 Telegram push sent")
    except Exception as e:
        print(f"  ⚠️ Telegram push failed: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback, sys
        tb = traceback.format_exc()
        print(f"\n  💥 FATAL ERROR:\n{tb}")
        try:
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            from telegram_push import push_error
            from datetime import datetime as dt
            push_error(dt.now().strftime("%Y-%m-%d"), f"discover_new_listings.py crashed:\n{str(e)[:200]}")
        except:
            pass
        sys.exit(1)
