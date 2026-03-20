#!/usr/bin/env python3
"""
CCASS Sentinel — Macro-Topology Analyzer v1.0

Non-parametric anomaly detection on IPO CCASS lifecycle data.
Two radars:
  RADAR 1: Pure secondary cornering (static float + rising concentration)
  RADAR 2: Lock-up expiry omnibus funnel (expanding float + retail absorption)

Requires: pandas (pip install pandas --break-system-packages -q)

Usage:
    python macro_topology.py --dataset ccass_dataset.json --output-dir /mnt/user-data/outputs

    # Custom thresholds
    python macro_topology.py --dataset d.json --output-dir out \
        --float-threshold 5.0 --accum-threshold 2.0 --top5-threshold 65.0
"""

import json, argparse, sys, os
import pandas as pd
import numpy as np


def load_and_clean(path):
    """Load v5 dataset, extract analysis-ready DataFrame."""
    with open(path) as f:
        raw = json.load(f)

    records = []
    for k, v in raw.items():
        if v.get("no_data") or v.get("error"):
            continue
        records.append({
            "Ticker": v["code"],
            "Name": v.get("name", "")[:25],
            "Listing_Date": v.get("listing_date"),
            "Snapshot": v.get("snapshot_name"),
            "Participants": v.get("participant_count"),
            "Total_Shares": v.get("total_shares"),
            "A00005_Pct": v.get("a00005_pct"),
            "Is_H_Share": v.get("is_h_share"),
            "Adjusted_Float": v.get("adjusted_float"),
            "Adj_Top5": v.get("adj_top5_pct"),
            "Adj_Top10": v.get("adj_top10_pct"),
            "Adj_HHI": v.get("adj_hhi"),
            "Top_Broker_ID": v.get("top_broker_id"),
            "Top_Broker_Name": v.get("top_broker_name", "")[:30],
            "Top_Broker_Pct": v.get("top_broker_pct"),
            "Futu_Pct": v.get("futu_pct"),
            "Broker_Top5": v.get("broker_top5_pct"),
        })

    df = pd.DataFrame(records)
    df["Listing_Date"] = pd.to_datetime(df["Listing_Date"])
    return df


def pivot_milestones(df):
    """Pivot to Ticker × Snapshot matrix with ECM physics vectors."""
    metrics = ["Adj_Top5", "Adj_HHI", "Total_Shares", "Futu_Pct",
               "Participants", "Top_Broker_Pct", "Broker_Top5"]
    pv = df.pivot_table(
        index=["Ticker", "Name", "Listing_Date"],
        columns="Snapshot", values=metrics)
    pv.columns = [f"{sn}_{m}" for m, sn in pv.columns]
    pv = pv.reset_index()

    # Merge LATEST broker metadata
    latest = df[df["Snapshot"] == "LATEST"][
        ["Ticker", "Top_Broker_Name", "Top_Broker_ID", "Is_H_Share"]
    ].drop_duplicates("Ticker")
    pv = pv.merge(latest, on="Ticker", how="left")

    # ECM Physics Vectors
    pv["Float_D30"] = pv.get("D30_Total_Shares")
    pv["Float_Latest"] = pv.get("LATEST_Total_Shares")
    pv["Float_Change_Pct"] = (
        (pv["Float_Latest"] - pv["Float_D30"]) / pv["Float_D30"] * 100
    )

    # Greenshoe Delta (D1→D30, structural — informational only)
    pv["Greenshoe_Delta"] = (
        pv.get("D30_Adj_Top5", pd.Series(dtype=float))
        - pv.get("D1_Adj_Top5", pd.Series(dtype=float))
    )

    # Organic Accumulation (D30→LATEST, the signal)
    pv["Organic_Accum"] = (
        pv.get("LATEST_Adj_Top5", pd.Series(dtype=float))
        - pv.get("D30_Adj_Top5", pd.Series(dtype=float))
    )

    # Futu accumulation post-greenshoe
    pv["Futu_Accum"] = (
        pv.get("LATEST_Futu_Pct", pd.Series(dtype=float))
        - pv.get("D30_Futu_Pct", pd.Series(dtype=float))
    )

    # Broker concentration accumulation (the primary cornering signal)
    pv["Broker_Organic_Accum"] = (
        pv.get("LATEST_Broker_Top5", pd.Series(dtype=float))
        - pv.get("D30_Broker_Top5", pd.Series(dtype=float))
    )

    # Participant change
    pv["Part_Change"] = (
        pv.get("LATEST_Participants", pd.Series(dtype=float))
        - pv.get("D30_Participants", pd.Series(dtype=float))
    )

    return pv


def print_radar1(valid, float_thresh, accum_thresh, top5_thresh):
    """RADAR 1: Pure secondary cornering — static float + rising BROKER concentration."""
    static = valid[valid["Float_Change_Pct"].abs() < float_thresh]
    hits = static[
        (static["Broker_Organic_Accum"] > accum_thresh)
        & (static["LATEST_Broker_Top5"] > top5_thresh)
    ].sort_values("Broker_Organic_Accum", ascending=False)

    print("🚨 RADAR 1: PURE SECONDARY CORNERING (Static Float + Rising Broker Concentration)")
    print(f"   Filters: |ΔFloat|<{float_thresh}%, Broker Accum>{accum_thresh}%, "
          f"Broker Top5>{top5_thresh}%\n")
    print(f"  {'Ticker':>6s} {'Name':20s} {'D30 BT5':>8s} {'Now BT5':>8s} {'Δ Brk':>6s} "
          f"{'AdjT5':>6s} {'PartΔ':>6s} {'Top Broker':25s} {'Brk%':>5s}")
    print(f"  {'-'*100}")
    for _, r in hits.iterrows():
        print(f"  {r['Ticker']:>6s} {r['Name']:20s} "
              f"{r.get('D30_Broker_Top5',0):>7.1f}% {r.get('LATEST_Broker_Top5',0):>7.1f}% "
              f"{r['Broker_Organic_Accum']:>+5.1f}% "
              f"{r.get('LATEST_Adj_Top5',0):>5.1f}% "
              f"{r.get('Part_Change',0):>+5.0f} "
              f"{str(r.get('Top_Broker_Name',''))[:25]:25s} "
              f"{r.get('LATEST_Top_Broker_Pct',0):>4.1f}%")
    print(f"\n  Total: {len(hits)} targets\n")
    return hits


def print_radar2(valid, float_expand_thresh=20.0, futu_thresh=10.0):
    """RADAR 2: Lock-up expiry omnibus funnel."""
    hits = valid[
        (valid["Float_Change_Pct"] > float_expand_thresh)
        & (valid["LATEST_Futu_Pct"] > futu_thresh)
    ].sort_values("Float_Change_Pct", ascending=False)

    print("⚠️ RADAR 2: LOCK-UP EXPIRY OMNIBUS FUNNEL")
    print(f"   Float expanded >{float_expand_thresh}% + Futu>{futu_thresh}%\n")
    print(f"  {'Ticker':>6s} {'Name':20s} {'D30 Float':>10s} {'Now Float':>10s} "
          f"{'ΔFloat':>7s} {'Futu%':>6s} {'Top Broker':25s}")
    print(f"  {'-'*90}")
    for _, r in hits.iterrows():
        d30f = r.get("Float_D30", 0) or 0
        nowf = r.get("Float_Latest", 0) or 0
        print(f"  {r['Ticker']:>6s} {r['Name']:20s} "
              f"{d30f/1e6:>9.1f}M {nowf/1e6:>9.1f}M "
              f"{r['Float_Change_Pct']:>+6.1f}% {r.get('LATEST_Futu_Pct',0):>5.1f}% "
              f"{str(r.get('Top_Broker_Name',''))[:25]:25s}")
    print(f"\n  Total: {len(hits)} targets\n")
    return hits


def print_leaderboard(valid, n=15):
    """Top N tightest books by Adj_Top5."""
    top = valid.sort_values("LATEST_Adj_Top5", ascending=False).head(n)

    print(f"🏆 UNREGULATED FLOAT LEADERBOARD — Top {n} Tightest Books")
    print(f"   (A00005 stripped. True free-float concentration.)\n")
    print(f"  {'#':>3s} {'Ticker':>6s} {'Name':20s} {'AdjT5':>6s} {'HHI':>6s} "
          f"{'Futu':>5s} {'H':>2s} {'OrgΔ':>6s} {'Top Broker':25s}")
    print(f"  {'-'*90}")
    for i, (_, r) in enumerate(top.iterrows(), 1):
        hs = "H" if r.get("Is_H_Share") else ""
        oa = r.get("Organic_Accum")
        oa_s = f"{oa:>+5.1f}%" if pd.notna(oa) else "  N/A "
        print(f"  {i:>3d} {r['Ticker']:>6s} {r['Name']:20s} "
              f"{r.get('LATEST_Adj_Top5',0):>5.1f}% {r.get('LATEST_Adj_HHI',0):>5.0f} "
              f"{r.get('LATEST_Futu_Pct',0):>4.1f}% {hs:>2s} {oa_s} "
              f"{str(r.get('Top_Broker_Name',''))[:25]:25s}")


def print_lifecycle(valid):
    """Concentration lifecycle stats by snapshot."""
    print(f"\n📊 CONCENTRATION LIFECYCLE: Median Adj_Top5 by Snapshot\n")
    for sn in ["D1", "D7", "D14", "D30", "D60", "D90", "LATEST"]:
        col = f"{sn}_Adj_Top5"
        if col in valid.columns:
            v = valid[col].dropna()
            if len(v) > 0:
                print(f"  {sn:>7s}: med={v.median():>5.1f}%  "
                      f"mean={v.mean():>5.1f}%  std={v.std():>4.1f}%  "
                      f"n={len(v):>3d}  "
                      f"P25={v.quantile(0.25):>5.1f}%  P75={v.quantile(0.75):>5.1f}%")


def main():
    p = argparse.ArgumentParser(description="CCASS Sentinel Macro-Topology Analyzer")
    p.add_argument("--dataset", required=True, help="Path to ccass_dataset.json")
    p.add_argument("--output-dir", default=".", help="Directory for CSV outputs")
    p.add_argument("--float-threshold", type=float, default=5.0,
                   help="Max |float change| %% for RADAR 1 (default 5)")
    p.add_argument("--accum-threshold", type=float, default=2.0,
                   help="Min organic accumulation %% for RADAR 1 (default 2)")
    p.add_argument("--top5-threshold", type=float, default=30.0,
                   help="Min terminal Broker_Top5 %% for RADAR 1 (default 30)")
    args = p.parse_args()

    print("⚙️ CCASS Sentinel — Macro-Topology Analyzer v1.0\n")

    df = load_and_clean(args.dataset)
    print(f"Dataset: {df['Ticker'].nunique()} stocks, {len(df)} snapshots")

    pv = pivot_milestones(df)
    valid = pv.dropna(subset=["Organic_Accum"]).copy()
    valid["Concentration_Pctile"] = valid["LATEST_Adj_Top5"].rank(pct=True) * 100
    print(f"Valid for analysis (D30 + LATEST): {len(valid)} stocks\n")
    print("=" * 95 + "\n")

    # RADAR 1
    r1 = print_radar1(valid, args.float_threshold,
                       args.accum_threshold, args.top5_threshold)
    print("=" * 95 + "\n")

    # RADAR 2
    r2 = print_radar2(valid)
    print("=" * 95 + "\n")

    # Leaderboard
    print_leaderboard(valid)

    # Lifecycle
    print_lifecycle(valid)

    # Save CSVs
    os.makedirs(args.output_dir, exist_ok=True)
    pv.to_csv(os.path.join(args.output_dir, "Macro_Topology_Panel.csv"), index=False)
    if not r1.empty:
        r1.to_csv(os.path.join(args.output_dir, "Cornering_Radar.csv"), index=False)
    if not r2.empty:
        r2.to_csv(os.path.join(args.output_dir, "Lockup_Funnel_Radar.csv"), index=False)
    print(f"\n✅ Saved to {args.output_dir}/")


if __name__ == "__main__":
    main()
