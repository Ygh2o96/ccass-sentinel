"""CCASS Sentinel — Telegram Push Module

Alert Push Protocol format.
Python urllib only — never curl (destroys & and $ characters).

Credentials loaded from environment variables (set in GitHub Secrets):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import json
import os
import urllib.request

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

def _get_api_url():
    if not BOT_TOKEN:
        return None
    return f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send(text, silent=False):
    """Send a Telegram message. Returns True on success."""
    url = _get_api_url()
    if not url or not CHAT_ID:
        print("  ⚠️ Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return False
    try:
        payload = {
            "chat_id": int(CHAT_ID),
            "text": text,
            "disable_notification": silent,
        }
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"}
        )
        resp = urllib.request.urlopen(req, timeout=10)
        return json.load(resp).get("ok", False)
    except Exception as e:
        print(f"  Telegram send failed: {e}")
        return False


def push_daily_summary(date, collected, errors, total_stocks, total_snaps, highlights):
    """Push daily collection summary."""
    lines = [
        f"🛰️ CCASS Daily — {date}",
        f"Collected: {collected} | Errors: {errors}",
        f"Universe: {total_stocks} stocks | {total_snaps} total snapshots",
    ]
    if highlights:
        lines.append("")
        for h in highlights[:5]:
            lines.append(h)
    return send("\n".join(lines))


def push_alerts(date, alerts):
    """Push anomaly alerts. One message per alert."""
    if not alerts:
        return
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
    sorted_alerts = sorted(alerts, key=lambda a: severity_order.get(a["severity"], 3))
    for alert in sorted_alerts:
        icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡"}.get(alert["severity"], "⚪")
        msg = f"🚨 CCASS ALERT — {alert['type']}\n{icon} {alert['message']}\n⏰ {date}"
        send(msg)


def push_discovery(date, new_codes):
    """Push new listing discovery."""
    if not new_codes:
        return
    msg = (
        f"🆕 CCASS New Listings Discovered — {date}\n"
        f"Found {len(new_codes)} new stocks in CCASS:\n"
        + "\n".join(f"  {code}" for code in new_codes[:10])
        + "\nAuto-added to watchlist [HIGH]"
    )
    return send(msg)


def push_error(date, error_msg):
    """Push system error."""
    msg = f"❌ CCASS Sentinel Error — {date}\n{error_msg}"
    return send(msg)
