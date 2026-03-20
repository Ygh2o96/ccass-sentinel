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

def get_viewstate():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    r = s.get(HKEX_URL, timeout=(5, 15))
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]+)"', r.text)
    vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]+)"', r.text)
    ev = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]+)"', r.text)
    if not all([vs, vsg, ev]):
        return None, s
    return {"vs": vs.group(1), "vsg": vsg.group(1), "ev": ev.group(1)}, s


def check_code(session, code, date_str, viewstate):
    """Returns True if CCASS has data for this code."""
    time.sleep(random.uniform(1.0, 2.5))
    data = {
        "__VIEWSTATE": viewstate["vs"],
        "__VIEWSTATEGENERATOR": viewstate["vsg"],
        "__EVENTVALIDATION": viewstate["ev"],
        "today": date_str.replace("/", ""),
        "txtShareholdingDate": date_str,
        "txtStockCode": code,
        "btnSearch": "Search",
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "alertMsg": "",
    }
    try:
        r = session.post(HKEX_URL, data=data, timeout=(3, 15))
        if r.status_code != 200:
            return False
        if "No match record" in r.text or "No record" in r.text:
            return False
        # Check if there's actual participant data
        if "participant-id" in r.text:
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
    
    # Format as 5-digit strings
    return sorted(f"{n:05d}" for n in candidates if 1 <= n <= 99999)


def main():
    print("🔍 CCASS Sentinel — New Listing Discovery")
    
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
        if check_code(session, code, date_str, viewstate):
            discovered.append(code)
            print(f"  🆕 DISCOVERED: {code}")
        checked += 1
        if checked % 50 == 0:
            print(f"  Checked {checked}/{len(candidates)}...")
    
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


if __name__ == "__main__":
    main()
