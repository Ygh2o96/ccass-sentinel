#!/usr/bin/env python3
"""
CCASS Sentinel — IPO Concentration Scanner v1.0
Batch scans all 2026 IPOs for concentration anomalies.

Usage:
    # Full scan of all 2026 IPOs
    python ipo_scanner.py --date 2026/03/18 --output results.json

    # Day 1 protocol for a new listing
    python ipo_scanner.py --day1 03355 --date 2026/03/23 --output day1.json

    # Add a stock not in the default list
    python ipo_scanner.py --date 2026/03/25 \
        --add-stock 03355:FS.COM:2026-03-23:38.40:CICC/中证/招商
"""

import requests, re, time, json, sys, argparse
from datetime import datetime

BASE_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": BASE_URL,
}

PARSE_PATTERN = re.compile(
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

# ─── Default 2026 IPO Registry ───────────────────────────────────────────────
# Format: code, name, listing_date, ipo_price, sponsor
IPOS_2026 = [
    ("06082", "壁仞科技 Biren Technology", "2026-01-02", 19.60, "CICC/Ping An/BOCI"),
    ("02513", "知识图谱 Knowledge Atlas", "2026-01-08", 116.20, "CICC"),
    ("09903", "天数智芯 Iluvatar CoreX", "2026-01-08", 144.60, "华泰"),
    ("02675", "安锐医疗 Edge Medical", "2026-01-08", 43.24, "Morgan Stanley/广发"),
    ("00100", "MiniMax", "2026-01-09", 165.00, "CICC/UBS"),
    ("06938", "瑞博生命 Ribo Life Science", "2026-01-09", 57.97, "CICC/Citi"),
    ("03636", "云南金讯 Yunnan Jinxun", "2026-01-09", 30.00, "华泰"),
    ("00501", "韦尔半导体 OmniVision", "2026-01-12", 104.80, "UBS/CICC/Ping An/广发"),
    ("03986", "兆易创新 GigaDevice", "2026-01-13", 162.00, "CICC/华泰"),
    ("01641", "红星冷链 Hongxing Coldchain", "2026-01-13", 12.26, "建银/农银"),
    ("09611", "龙旗科技 Longcheer Tech", "2026-01-22", 31.00, "Citi/海通/国君"),
    ("01768", "繁明集团 Busy Ming", "2026-01-28", 236.60, "Goldman/华泰"),
    ("09980", "东鹏饮料 Eastroc Beverage", "2026-02-03", 248.00, "华泰/Morgan Stanley/UBS"),
    ("02768", "青岛冠中 Qingdao Gon", "2026-02-04", 36.00, "招商证券"),
    ("02677", "卓越医疗 Distinct Healthcare", "2026-02-06", 59.90, "海通/浦银"),
    ("02714", "牧原食品 Muyuan Foods", "2026-02-06", 39.00, "Morgan Stanley/中信/Goldman"),
    ("03200", "大族数控 Han's CNC", "2026-02-06", 95.80, "CICC"),
    ("06809", "澜起科技 Montage Tech", "2026-02-09", 106.89, "CICC/Morgan Stanley/UBS"),
    ("00600", "爱芯元智 Axera Semi", "2026-02-10", 28.20, "CICC/国君/交银"),
    ("02720", "岭外国际 Ridge Outdoor", "2026-02-10", 12.25, "CICC"),
    ("00470", "先导智能 Wuxi Lead", "2026-02-11", 45.80, "中信/JP Morgan"),
    ("02706", "海致科技 Haizhi Tech", "2026-02-13", 27.06, "招银/BOCI/申万"),
    ("09981", "沃尔核材 Woer Heat-Shrink", "2026-02-13", 20.09, "中证/招商证券"),
    ("02649", "澳仕科 ALSCO Pooling", "2026-03-09", 11.00, "中证"),
    ("02715", "埃斯顿 Estun Automation", "2026-03-09", 15.36, "华泰"),
    ("02692", "兆威机电 Zhaowei", "2026-03-09", 71.28, "招商/德意志"),
    ("03268", "美格智能 MeiG Smart", "2026-03-10", 28.86, "CICC"),
]


def get_tokens(session):
    r = session.get(BASE_URL, headers=HEADERS, timeout=15)
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    td = re.search(r'id="today"\s+value="([^"]*)"', r.text)
    return {"__VIEWSTATE": vs.group(1) if vs else "",
            "__VIEWSTATEGENERATOR": vg.group(1) if vg else "",
            "today": td.group(1) if td else ""}


def scrape(session, code, date, tokens):
    data = {
        "__EVENTTARGET": "btnSearch", "__EVENTARGUMENT": "",
        "__VIEWSTATE": tokens["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": tokens["__VIEWSTATEGENERATOR"],
        "today": tokens["today"], "sortBy": "shareholding", "sortDirection": "desc",
        "originalShareholdingDate": "", "alertMsg": "",
        "txtShareholdingDate": date, "txtStockCode": code,
        "txtStockName": "", "txtParticipantID": "",
        "txtParticipantName": "", "txtSelPartID": "",
    }
    r = session.post(BASE_URL, data=data, headers=HEADERS, timeout=30)
    # Update tokens
    v = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    g = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    if v: tokens["__VIEWSTATE"] = v.group(1)
    if g: tokens["__VIEWSTATEGENERATOR"] = g.group(1)

    if len(r.text) < 15000:
        return None

    holdings = []
    for m in PARSE_PATTERN.finditer(r.text):
        pid, name = m.group(1).strip(), m.group(2).strip()
        try: shares = int(m.group(4).strip().replace(",", ""))
        except: shares = 0
        try: pct = float(m.group(5).strip().replace("%", ""))
        except: pct = 0.0
        holdings.append({"pid": pid, "name": name, "shares": shares, "pct": pct})
    return sorted(holdings, key=lambda x: x["shares"], reverse=True)


def analyze(holdings):
    if not holdings: return {}
    total = sum(h["shares"] for h in holdings)
    if total == 0: return {}
    n = len(holdings)
    top1 = holdings[0]["shares"] / total * 100
    top5 = sum(h["shares"] for h in holdings[:5]) / total * 100
    top10 = sum(h["shares"] for h in holdings[:10]) / total * 100
    hhi = sum((h["shares"] / total * 100) ** 2 for h in holdings)

    clearing = {"A00001", "A00003", "A00004", "A00005"}
    non_inst = [h for h in holdings if h["pid"] not in clearing
                and not h["pid"].startswith("C")]
    ni = non_inst[0] if non_inst else None

    is_hshare = holdings[0]["pid"] == "A00005"

    flags = []
    if top5 > 95: flags.append("🔴")
    elif top5 > 90: flags.append("🟡")
    if ni and ni["shares"] / total * 100 > 5: flags.append("⚠️")
    if n < 80: flags.append("📌")

    return {
        "num_participants": n, "top1_pct": round(top1, 2),
        "top5_pct": round(top5, 2), "top10_pct": round(top10, 2),
        "hhi": round(hhi, 0),
        "top1_id": holdings[0]["pid"], "top1_name": holdings[0]["name"][:40],
        "ni_top1_id": ni["pid"] if ni else "",
        "ni_top1_name": ni["name"][:40] if ni else "",
        "ni_top1_pct": round(ni["shares"] / total * 100, 2) if ni else 0,
        "is_hshare": is_hshare,
        "flags": "".join(flags) if flags else "✅",
    }


def main():
    p = argparse.ArgumentParser(description="CCASS Sentinel — IPO Scanner")
    p.add_argument("--date", required=True, help="Scan date YYYY/MM/DD")
    p.add_argument("--day1", help="Day 1 protocol: single stock code")
    p.add_argument("--add-stock", action="append", default=[],
                   help="CODE:NAME:DATE:PRICE:SPONSOR")
    p.add_argument("--output", help="Output JSON path")
    p.add_argument("--delay", type=float, default=1.2)
    args = p.parse_args()

    # Build stock list
    stocks = [(c, n, d, pr, sp) for c, n, d, pr, sp in IPOS_2026]
    for spec in args.add_stock:
        parts = spec.split(":")
        if len(parts) >= 5:
            stocks.append((parts[0], parts[1], parts[2],
                          float(parts[3]), parts[4]))

    session = requests.Session()
    print("🔍 CCASS Sentinel — IPO Scanner v1.0")
    tokens = get_tokens(session)
    print(f"   Server date: {tokens['today']}  |  Scan date: {args.date}")

    # Day 1 mode: single stock + benchmark comparison
    if args.day1:
        target = args.day1.zfill(5)
        print(f"\n🆕 DAY 1 PROTOCOL: {target}")
        print(f"{'='*80}")

        holdings = scrape(session, target, args.date, tokens)
        if not holdings:
            print(f"   ❌ No CCASS data for {target} on {args.date}")
            print(f"   CCASS data is T+1. If stock listed today, try tomorrow.")
            sys.exit(1)

        stats = analyze(holdings)
        total = sum(h["shares"] for h in holdings)

        print(f"\n  Participants: {stats['num_participants']}")
        print(f"  Top5: {stats['top5_pct']}%  |  Top10: {stats['top10_pct']}%  |  HHI: {stats['hhi']}")
        print(f"  H-share (A00005 dominant): {'YES' if stats['is_hshare'] else 'NO'}")
        print(f"  Flags: {stats['flags']}")
        print(f"\n  {'ID':10s} {'Name':40s} {'Shares':>14s} {'%':>7s}")
        print(f"  {'-'*75}")
        for h in holdings[:15]:
            pct = h["shares"] / total * 100 if total else 0
            print(f"  {h['pid']:10s} {h['name'][:40]:40s} {h['shares']:>14,d} {pct:>6.2f}%")

        # Benchmark vs 2026 IPOs
        print(f"\n{'='*80}")
        print(f"  BENCHMARK: Where does {target} rank among 2026 IPOs?")
        print(f"{'='*80}")

        # Quick scan a sample for comparison (use cached if available)
        benchmarks = []
        sample_codes = ["02706", "02677", "01641", "01768", "02675",
                        "00100", "02513", "03268", "02649"]
        for code in sample_codes:
            if code == target: continue
            time.sleep(args.delay)
            bh = scrape(session, code, args.date, tokens)
            if bh:
                bs = analyze(bh)
                benchmarks.append({"code": code, **bs})

        benchmarks.append({"code": target, **stats})
        benchmarks.sort(key=lambda x: x["top5_pct"], reverse=True)

        print(f"\n  {'Rank':>4s} {'Code':>5s} {'#Part':>5s} {'Top5%':>7s} {'HHI':>7s} {'NI Top1':>25s} {'%':>6s} {'Flag':>6s}")
        print(f"  {'-'*72}")
        for i, b in enumerate(benchmarks, 1):
            marker = " ◀◀◀" if b["code"] == target else ""
            print(f"  {i:>4d} {b['code']:>5s} {b['num_participants']:>5d} "
                  f"{b['top5_pct']:>6.1f}% {b['hhi']:>7.0f} "
                  f"{b.get('ni_top1_name','')[:25]:>25s} {b.get('ni_top1_pct',0):>5.1f}% "
                  f"{b['flags']:>6s}{marker}")

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump({"target": target, "date": args.date,
                          "stats": stats, "top_holders": holdings[:20],
                          "benchmarks": benchmarks}, f, ensure_ascii=False, indent=2)
            print(f"\n✅ → {args.output}")
        return

    # Full scan mode
    print(f"\n📊 Full scan: {len(stocks)} stocks")
    results = []
    for i, (code, name, ldate, price, sponsor) in enumerate(stocks):
        sys.stdout.write(f"\r   [{i+1}/{len(stocks)}] {code} {name[:20]}...")
        sys.stdout.flush()
        try:
            holdings = scrape(session, code, args.date, tokens)
            if holdings:
                stats = analyze(holdings)
                stats.update({"code": code, "name": name, "listing_date": ldate,
                             "ipo_price": price, "sponsor": sponsor,
                             "top_holders": [{"pid": h["pid"], "name": h["name"],
                                             "shares": h["shares"], "pct": h["pct"]}
                                            for h in holdings[:20]]})
                results.append(stats)
            else:
                print(f" NO DATA")
            time.sleep(args.delay)
        except Exception as e:
            print(f" ERROR: {e}")
            time.sleep(3)

    print(f"\r   ✅ {len(results)}/{len(stocks)} stocks scanned")

    # Leaderboard
    results.sort(key=lambda x: x.get("top5_pct", 0), reverse=True)
    print(f"\n{'='*120}")
    print(f"  2026 IPO CONCENTRATION LEADERBOARD ({args.date})")
    print(f"{'='*120}")
    print(f"  {'#':>3s} {'Code':>5s} {'Name':25s} {'#P':>4s} {'Top5%':>7s} {'HHI':>7s} "
          f"{'H?':>3s} {'Top Non-Inst Broker':>30s} {'%':>6s} {'F':>4s}")
    print(f"  {'-'*100}")

    for i, r in enumerate(results, 1):
        hs = "H" if r.get("is_hshare") else ""
        print(f"  {i:>3d} {r['code']:>5s} {r['name'][:25]:25s} {r['num_participants']:>4d} "
              f"{r['top5_pct']:>6.1f}% {r['hhi']:>7.0f} {hs:>3s} "
              f"{r.get('ni_top1_name','')[:30]:>30s} {r.get('ni_top1_pct',0):>5.1f}% "
              f"{r['flags']:>4s}")

    # Anomaly summary
    print(f"\n{'='*80}")
    print(f"  ANOMALY SUMMARY")
    print(f"{'='*80}")
    for r in results:
        if "🔴" in r["flags"] or "🟡" in r["flags"]:
            hs = "(H-share A00005 structural)" if r.get("is_hshare") else "⚡ PURE CONCENTRATION"
            print(f"  {r['code']} {r['name'][:22]:22s} Top5={r['top5_pct']}% {hs}")

    if args.output:
        clean = [{k: v for k, v in r.items() if k != "top_holders"} for r in results]
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n✅ → {args.output}")


if __name__ == "__main__":
    main()
