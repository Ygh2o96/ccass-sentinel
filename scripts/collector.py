#!/usr/bin/env python3
"""
CCASS Sentinel Collector v5 — Institutional Gold Master

Parallel HKEX CCASS scraper with:
- Frozen ViewState (empirically verified: stateless reuse works)
- Thread-local session pooling (1 persistent TCP socket per worker)
- Rolling queue with wait(FIRST_COMPLETED)
- Circuit breaker with cancel_futures=True hard shutdown
- Adjusted_HHI: excludes A00005 (China Clear) from both num and denom
- Anti-poison checkpoints: errors not persisted, auto-retry on rerun
- Atomic disk writes: .tmp + os.replace

Performance: ~0.50 req/s with 5 workers (limited by HKEX 8-10s TTFB)

Usage:
    python collector_v5.py --manifest ccass_universe.json --output ccass_dataset.json
    python collector_v5.py --manifest m.json --output d.json --workers 8 --time-limit 120

Batch loop:
    while python collector_v5.py --manifest m.json --output d.json; do sleep 2; done
"""

import requests, re, json, time, sys, os, random, threading, argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from requests.adapters import HTTPAdapter

# ── Constants ──────────────────────────────────────────────────────────────

BASE_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": BASE_URL,
}
PAT = re.compile(
    r'<div class="mobile-list-body">([ABC]\d{5})</div>\s*</td>\s*'
    r'<td class="col-participant-name">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-address">.*?</td>\s*'
    r'<td class="col-shareholding text-right">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">([\d,]+)</div>', re.DOTALL)

# ── Thread-local session pool ──────────────────────────────────────────────

_tls = threading.local()

def _init_worker():
    """Create one persistent Session per thread — reuses TCP/TLS connection."""
    s = requests.Session()
    s.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=1))
    s.headers.update(HDR)
    _tls.session = s


# ── Network layer ──────────────────────────────────────────────────────────

def harvest():
    """GET once to extract frozen ASP.NET ViewState tokens.
    Raises RuntimeError on Cloudflare interception."""
    r = requests.get(BASE_URL, headers=HDR, timeout=15)
    r.raise_for_status()
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    td = re.search(r'id="today"\s+value="([^"]*)"', r.text)
    if not vs or not vg or not td:
        raise RuntimeError(f"Token harvest failed (Cloudflare? resp={len(r.text)}B)")
    return vs.group(1), vg.group(1), td.group(1)


def fetch(vs, vg, td, code, date):
    """POST with frozen tokens. Returns sorted holdings list or None."""
    time.sleep(random.uniform(0.1, 0.3))
    r = _tls.session.post(BASE_URL, timeout=(3.05, 20), data={
        "__EVENTTARGET": "btnSearch", "__EVENTARGUMENT": "",
        "__VIEWSTATE": vs, "__VIEWSTATEGENERATOR": vg, "today": td,
        "sortBy": "shareholding", "sortDirection": "desc",
        "originalShareholdingDate": "", "alertMsg": "",
        "txtShareholdingDate": date, "txtStockCode": code,
        "txtStockName": "", "txtParticipantID": "",
        "txtParticipantName": "", "txtSelPartID": "",
    })
    r.raise_for_status()
    if len(r.text) < 15000:
        return None
    return sorted(
        [{"pid": m.group(1), "name": m.group(2).strip(),
          "shares": int(m.group(3).replace(",", ""))}
         for m in PAT.finditer(r.text)],
        key=lambda x: x["shares"], reverse=True)


# ── Analysis ───────────────────────────────────────────────────────────────

def analyze(holdings):
    """Compute raw + adjusted concentration metrics.

    Option A: Strip A00005 only (from both numerator and denominator).
    A00005 = CSDC immobilized domestic shares. NOT tradable on HKEX.
    A00003/A00004 = Stock Connect = REAL tradable liquidity. Stays in both.

    adjusted_float = total - A00005
    adj_top5 = top 5 of everyone except A00005 / adjusted_float
    broker_top5 = top 5 of B-prefix only / adjusted_float (cornering detector)
    """
    if not holdings:
        return {}
    total = sum(h["shares"] for h in holdings)
    if total == 0:
        return {}

    # A00005 only (immobilized, non-tradable)
    a5_shares = sum(h["shares"] for h in holdings if h["pid"] == "A00005")
    adjusted_float = total - a5_shares
    is_h_share = a5_shares / total > 0.3 if total > 0 else False

    # Adjusted = everyone except A00005 (includes A00003/A00004)
    adj = [h for h in holdings if h["pid"] != "A00005"]

    if adjusted_float > 0:
        adj_top5_pct = sum(h["shares"] for h in adj[:5]) / adjusted_float * 100
        adj_top10_pct = sum(h["shares"] for h in adj[:10]) / adjusted_float * 100
        adj_hhi = sum((h["shares"] / adjusted_float * 100) ** 2 for h in adj)
    else:
        adj_top5_pct = adj_top10_pct = adj_hhi = 0

    # Broker-only metrics (B-prefix on adjusted_float — the cornering detector)
    brokers = [h for h in adj if h["pid"].startswith("B")]
    if adjusted_float > 0 and brokers:
        broker_top5_pct = sum(h["shares"] for h in brokers[:5]) / adjusted_float * 100
    else:
        broker_top5_pct = 0

    top_broker = brokers[0] if brokers else None
    futu_shares = next((h["shares"] for h in holdings if h["pid"] == "B01955"), 0)

    return {
        "participant_count": len(holdings),
        "total_shares": total,
        "a00005_pct": round(a5_shares / total * 100, 2),
        "is_h_share": is_h_share,
        "adjusted_float": adjusted_float,
        # Raw metrics (all participants including A00005)
        "raw_top5_pct": round(sum(h["shares"] for h in holdings[:5]) / total * 100, 2),
        "raw_hhi": round(sum((h["shares"] / total * 100) ** 2 for h in holdings), 1),
        # Adjusted metrics (ex-A00005 only — includes Stock Connect)
        "adj_top5_pct": round(adj_top5_pct, 2),
        "adj_top10_pct": round(adj_top10_pct, 2),
        "adj_hhi": round(adj_hhi, 1),
        # Broker-only metrics (B-prefix on adjusted float — cornering detector)
        "broker_top5_pct": round(broker_top5_pct, 2),
        "top_broker_id": top_broker["pid"] if top_broker else "",
        "top_broker_name": top_broker["name"][:40] if top_broker else "",
        "top_broker_pct": round(top_broker["shares"] / adjusted_float * 100, 2) if top_broker and adjusted_float > 0 else 0,
        "futu_pct": round(futu_shares / adjusted_float * 100, 2) if adjusted_float > 0 else 0,
        # ALL holders — never re-scrape for methodology changes
        "holders": [
            {"pid": h["pid"], "name": h["name"], "shares": h["shares"]}
            for h in holdings
        ],
    }


# ── I/O ────────────────────────────────────────────────────────────────────

def atomic_save(dataset, path):
    """Write JSON via .tmp + os.replace for crash safety."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(dataset, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        with open(path, "w") as f:
            json.dump(dataset, f, ensure_ascii=False)


# ── Main ───────────────────────────────────────────────────────────────────

def run(manifest_path, output_path, workers, time_limit):
    manifest = json.load(open(manifest_path))
    dataset = json.load(open(output_path)) if os.path.exists(output_path) else {}

    # Build task queue — anti-poison: re-queue any prior errors
    tasks, meta = [], {}
    for job in manifest:
        code = job["code"]
        meta[code] = (job["name"], job["listing_date"])
        for snap_name, snap_date in job["targets"]:
            key = f"{code}_{snap_name}"
            existing = dataset.get(key, {})
            if not existing or existing.get("error"):
                tasks.append((code, snap_name, snap_date, key))

    if not tasks:
        print("✅ All tasks complete.")
        return

    vs, vg, td = harvest()
    t0 = time.time()
    done = failed = 0
    print(f"📋 {len(tasks)} pending | {workers} workers | {time_limit}s limit")

    def process(task):
        code, snap_name, snap_date, key = task
        try:
            holdings = fetch(vs, vg, td, code, snap_date)
            if holdings:
                metrics = analyze(holdings)
                return key, {
                    "code": code,
                    "name": meta[code][0],
                    "listing_date": meta[code][1],
                    "snapshot_name": snap_name,
                    "snapshot_date": snap_date.replace("/", "-"),
                    **metrics,
                }
            return key, {"code": code, "snapshot_name": snap_name, "no_data": True}
        except Exception as e:
            return key, {"error": str(e)[:80]}

    pool = ThreadPoolExecutor(max_workers=workers, initializer=_init_worker)
    try:
        pending = set()
        task_iter = iter(tasks)

        # Seed exactly WORKERS tasks for tight breaker precision
        for _ in range(workers):
            t = next(task_iter, None)
            if t:
                pending.add(pool.submit(process, t))

        while pending:
            if time.time() - t0 > time_limit:
                print(f"\n⏰ Breaker at {time.time() - t0:.0f}s")
                break

            done_set, pending = wait(pending, timeout=1, return_when=FIRST_COMPLETED)

            for fut in done_set:
                key, result = fut.result()

                # Anti-poison: errors NOT persisted → auto-retry on rerun
                if "error" not in result:
                    dataset[key] = result
                    done += 1
                else:
                    failed += 1

                # Keep pool saturated
                if time.time() - t0 <= time_limit:
                    t = next(task_iter, None)
                    if t:
                        pending.add(pool.submit(process, t))

            # Periodic checkpoint
            if done > 0 and done % 10 == 0:
                atomic_save(dataset, output_path)
                elapsed = time.time() - t0
                rate = done / elapsed
                sys.stdout.write(f"\r  ✅{done} ❌{failed} | {rate:.2f}/s | ⏱️{elapsed:.0f}s")
                sys.stdout.flush()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    atomic_save(dataset, output_path)
    elapsed = time.time() - t0
    valid = sum(1 for v in dataset.values() if not v.get("no_data") and not v.get("error"))
    remaining = len(tasks) - done - failed
    print(f"\n✅ {done} new ({valid} total valid) in {elapsed:.0f}s | "
          f"❌ {failed} errs | {remaining} remaining")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="CCASS Sentinel Collector v5")
    p.add_argument("--manifest", required=True, help="Path to manifest JSON")
    p.add_argument("--output", required=True, help="Path to output dataset JSON")
    p.add_argument("--workers", type=int, default=5, help="Concurrent threads")
    p.add_argument("--time-limit", type=int, default=140, help="Circuit breaker (seconds)")
    args = p.parse_args()
    run(args.manifest, args.output, args.workers, args.time_limit)
