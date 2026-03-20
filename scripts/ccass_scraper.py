#!/usr/bin/env python3
"""
CCASS Sentinel — Interactive Scraper v2.0

Single-stock / watchlist / date-range CCASS queries with Adjusted_HHI.
Frozen ViewState (stateless) + persistent session.

Usage:
    python ccass_scraper.py --stock 02706 --date 2026/03/18
    python ccass_scraper.py --stock 02706 --from 2026/02/13 --to 2026/03/18 --track B02116,B01955
    python ccass_scraper.py --watchlist 02659,02677,01641 --date 2026/03/20
"""
import requests, re, csv, json, time, argparse, sys
from datetime import datetime, timedelta

BASE_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": BASE_URL,
}
PAT = re.compile(
    r'<td class="col-participant-id">\s*<div class="mobile-list-heading">.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-participant-name">\s*<div class="mobile-list-heading">.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-address">\s*<div class="mobile-list-heading">.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-shareholding text-right">\s*<div class="mobile-list-heading">.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-shareholding-percent text-right">\s*<div class="mobile-list-heading">.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>',
    re.DOTALL
)

def harvest(session):
    r = session.get(BASE_URL, headers=HDR, timeout=15)
    r.raise_for_status()
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    td = re.search(r'id="today"\s+value="([^"]*)"', r.text)
    if not vs or not vg or not td:
        raise RuntimeError(f"Token harvest failed (resp={len(r.text)}B)")
    return vs.group(1), vg.group(1), td.group(1)

def fetch(session, vs, vg, td, code, date_str):
    r = session.post(BASE_URL, headers=HDR, timeout=(3.05, 30), data={
        "__EVENTTARGET": "btnSearch", "__EVENTARGUMENT": "",
        "__VIEWSTATE": vs, "__VIEWSTATEGENERATOR": vg, "today": td,
        "sortBy": "shareholding", "sortDirection": "desc",
        "originalShareholdingDate": "", "alertMsg": "",
        "txtShareholdingDate": date_str, "txtStockCode": code,
        "txtStockName": "", "txtParticipantID": "",
        "txtParticipantName": "", "txtSelPartID": "",
    })
    r.raise_for_status()
    if len(r.text) < 15000:
        return None
    holdings = []
    for m in PAT.finditer(r.text):
        holdings.append({
            "pid": m.group(1).strip(), "name": m.group(2).strip(),
            "address": m.group(3).strip(),
            "shares": int(m.group(4).strip().replace(",", "")),
            "pct": float(m.group(5).strip().replace("%", "") or 0),
        })
    return {
        "date": date_str.replace("/", "-"), "stock": code,
        "num_participants": len(holdings),
        "holdings": sorted(holdings, key=lambda x: x["shares"], reverse=True),
    }

def metrics(result):
    if not result or not result["holdings"]:
        return {}
    h = result["holdings"]
    total = sum(x["shares"] for x in h)
    if total == 0:
        return {}
    # Raw
    raw_top5 = sum(x["shares"] for x in h[:5]) / total * 100
    raw_hhi = sum((x["shares"] / total * 100) ** 2 for x in h)
    # Denominator: total - A00005 only (Stock Connect stays as tradable liquidity)
    a5 = sum(x["shares"] for x in h if x["pid"] == "A00005")
    af = total - a5
    is_h = a5 / total > 0.3 if total > 0 else False
    # Numerator ranking: exclude ALL clearing houses from Top N
    adj = [x for x in h if not x["pid"].startswith("A")]
    if af > 0:
        at5 = sum(x["shares"] for x in adj[:5]) / af * 100
        at10 = sum(x["shares"] for x in adj[:10]) / af * 100
        ahhi = sum((x["shares"] / af * 100) ** 2 for x in adj)
    else:
        at5 = at10 = ahhi = 0
    tb = next((x for x in adj if x["pid"].startswith("B")), None)
    return {
        "raw_top5": round(raw_top5, 2), "raw_hhi": round(raw_hhi, 0),
        "a00005_pct": round(a5 / total * 100, 2),
        "is_h_share": is_h, "adj_float": af,
        "adj_top5": round(at5, 2), "adj_top10": round(at10, 2), "adj_hhi": round(ahhi, 0),
        "tb_id": tb["pid"] if tb else "", "tb_name": tb["name"] if tb else "",
        "tb_pct": round(tb["shares"] / af * 100, 2) if tb and af > 0 else 0,
    }

def flags(m, n):
    f = []
    if m.get("adj_top5", 0) > 95: f.append("🔴")
    elif m.get("adj_top5", 0) > 90: f.append("🟡")
    if m.get("tb_pct", 0) > 10: f.append("⚠️")
    if n < 80: f.append("📌")
    return "".join(f) or "✅"

def date_range(s, e):
    cur = datetime.strptime(s, "%Y/%m/%d")
    end = datetime.strptime(e, "%Y/%m/%d")
    while cur <= end:
        yield cur.strftime("%Y/%m/%d")
        cur += timedelta(days=1)

def show(result, tracked=None):
    if not result: return
    m = metrics(result)
    f = flags(m, result["num_participants"])
    htag = " [H-share]" if m.get("is_h_share") else ""
    print(f"\n{'='*85}")
    print(f"  {result['stock']}  |  {result['date']}  |  "
          f"{result['num_participants']} part  |  "
          f"Adj_HHI: {m.get('adj_hhi',0):.0f}  |  "
          f"Adj_Top5: {m.get('adj_top5',0):.1f}%  {f}{htag}")
    if m.get("is_h_share"):
        print(f"  A00005: {m['a00005_pct']:.1f}%  |  Adj Float: {m['adj_float']:,d}")
    print(f"{'='*85}")
    print(f"  {'ID':10s} {'Name':40s} {'Shares':>14s} {'%':>7s}")
    print(f"  {'-'*75}")
    shown = 0
    for h in result["holdings"]:
        is_t = tracked and h["pid"] in tracked
        if shown < 15 or is_t:
            tag = " ◀◀◀" if is_t else ""
            print(f"  {h['pid']:10s} {h['name'][:40]:40s} "
                  f"{h['shares']:>14,d} {h['pct']:>6.2f}%{tag}")
            shown += 1

def show_tracking(results, tracked):
    if not tracked or not results: return
    print(f"\n{'='*90}")
    print(f"  TRACKING: {', '.join(tracked)}")
    print(f"{'='*90}")
    hdr = f"  {'Date':12s} {'#P':>5s} {'AdjHHI':>7s}"
    for t in tracked: hdr += f" {t:>14s}"
    print(hdr)
    print(f"  {'-'*86}")
    prev = {}
    for r in results:
        if not r: continue
        m = metrics(r)
        row = f"  {r['date']:12s} {r['num_participants']:>5d} {m.get('adj_hhi',0):>7.0f}"
        hmap = {h["pid"]: h["shares"] for h in r["holdings"]}
        for t in tracked:
            v = hmap.get(t, 0)
            p = prev.get(t, v)
            d = v - p
            row += f" {v:>10,d}{'↑' if d>0 else ('↓' if d<0 else ' ')}" if d != 0 else f" {v:>14,d}"
            prev[t] = v
        print(row)

def main():
    p = argparse.ArgumentParser(description="CCASS Sentinel Scraper v2.0")
    p.add_argument("--stock", help="Stock code (e.g. 02706)")
    p.add_argument("--watchlist", help="Comma-separated codes")
    p.add_argument("--date", help="YYYY/MM/DD")
    p.add_argument("--from", dest="from_date", help="Range start")
    p.add_argument("--to", dest="to_date", help="Range end")
    p.add_argument("--track", help="Comma-separated participant IDs")
    p.add_argument("--csv", dest="csv_file", help="CSV output")
    p.add_argument("--json", dest="json_file", help="JSON output")
    p.add_argument("--delay", type=float, default=1.0, help="Request delay (s)")
    args = p.parse_args()

    tracked = set(args.track.split(",")) if args.track else None
    sess = requests.Session()
    print("🔍 CCASS Sentinel v2.0")
    vs, vg, td = harvest(sess)
    print(f"   Ready. Date: {td}")

    results = []

    if args.watchlist:
        codes = [c.strip() for c in args.watchlist.split(",")]
        d = args.date or datetime.now().strftime("%Y/%m/%d")
        print(f"\n📊 Watchlist: {len(codes)} stocks on {d}")
        for i, c in enumerate(codes):
            sys.stdout.write(f"\r   [{i+1}/{len(codes)}] {c}...")
            sys.stdout.flush()
            try:
                r = fetch(sess, vs, vg, td, c, d)
                if r:
                    results.append(r)
                    show(r, tracked)
                else:
                    print(f" NO DATA")
                time.sleep(args.delay)
            except Exception as e:
                print(f" ERR: {e}")
                time.sleep(2)

    elif args.stock:
        if args.date and not args.from_date:
            print(f"\n📊 {args.stock} on {args.date}")
            r = fetch(sess, vs, vg, td, args.stock, args.date)
            if r:
                results.append(r)
                show(r, tracked)
            else:
                print("   No data")

        elif args.from_date and args.to_date:
            dates = list(date_range(args.from_date, args.to_date))
            print(f"\n📊 {args.stock}: {args.from_date} → {args.to_date} ({len(dates)} days)")
            for i, d in enumerate(dates):
                sys.stdout.write(f"\r   [{i+1}/{len(dates)}] {d}...")
                sys.stdout.flush()
                try:
                    r = fetch(sess, vs, vg, td, args.stock, d)
                    if r: results.append(r)
                    time.sleep(args.delay)
                except Exception as e:
                    print(f"\n   ⚠️ {d}: {e}")
                    time.sleep(2)
            print(f"\r   ✅ {len(results)} snapshots")
            if tracked:
                show_tracking(results, tracked)
            elif results:
                show(results[0])
                if len(results) > 1: show(results[-1])

    if args.csv_file and results:
        with open(args.csv_file, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(["date","stock","participants","adj_hhi","pid","name","shares","pct"])
            for r in results:
                m = metrics(r)
                for h in r["holdings"]:
                    if tracked and h["pid"] not in tracked: continue
                    w.writerow([r["date"],r["stock"],r["num_participants"],
                               m.get("adj_hhi",0),h["pid"],h["name"],h["shares"],h["pct"]])
        print(f"✅ CSV → {args.csv_file}")

    if args.json_file and results:
        out = [{**metrics(r), "date":r["date"], "stock":r["stock"],
                "participants":r["num_participants"],
                "top20":[{"pid":h["pid"],"name":h["name"],"shares":h["shares"]}
                         for h in r["holdings"][:20]]}
               for r in results if r]
        with open(args.json_file, 'w') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"✅ JSON → {args.json_file}")

if __name__ == "__main__":
    main()
