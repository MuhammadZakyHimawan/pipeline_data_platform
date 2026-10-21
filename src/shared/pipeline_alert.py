#!/usr/bin/env python3
"""
pipeline_alert.py - Alert notifier untuk pipeline failure.

Priority: Telegram → Discord → Generic Webhook → File Log
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def send_telegram(token: str, chat_id: str, message: str) -> bool:
    """Kirim alert via Telegram Bot API."""
    if not HAS_REQUESTS:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Telegram alert failed: {e}", file=sys.stderr)
        return False


def send_discord(webhook_url: str, message: str) -> bool:
    """Kirim alert via Discord Webhook."""
    if not HAS_REQUESTS:
        return False
    # Discord webhook max 2000 chars, truncate if needed
    payload = {"content": message[:1900]}
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Discord alert failed: {e}", file=sys.stderr)
        return False


def send_generic_webhook(webhook_url: str, payload: dict) -> bool:
    """Kirim alert via generic webhook (custom server)."""
    if not HAS_REQUESTS:
        return False
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Generic webhook alert failed: {e}", file=sys.stderr)
        return False


def log_to_file(message: str) -> None:
    """Fallback: tulis alert ke file log."""
    log_path = Path("logs/pipeline_alerts.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {message}\n")


def build_alert_message(source: str, message: str, exit_code: int) -> tuple[str, dict]:
    """Build formatted message untuk Telegram/Discord dan payload untuk webhook."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Telegram/Discord format (HTML untuk Telegram, plain untuk Discord)
    tg_message = (
        f"🚨 <b>PIPELINE ALERT</b> 🚨\n"
        f"⏰ <b>Waktu:</b> {timestamp}\n"
        f"📦 <b>Schedule:</b> {source}\n"
        f"💬 <b>Pesan:</b> {message}\n"
        f"🔢 <b>Exit Code:</b> {exit_code}"
    )
    
    # Webhook payload (JSON structured)
    webhook_payload = {
        "alert_type": "pipeline_failure",
        "timestamp": timestamp,
        "schedule": source,
        "message": message,
        "exit_code": exit_code,
        "severity": "critical"
    }
    
    return tg_message, webhook_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline failure alert notifier")
    parser.add_argument("--message", required=True, help="Alert message")
    parser.add_argument("--exit-code", type=int, default=1, help="Exit code from pipeline")
    parser.add_argument("--source", default="unknown", help="Pipeline schedule/source name")
    args = parser.parse_args()
    
    # Build messages
    tg_message, webhook_payload = build_alert_message(args.source, args.message, args.exit_code)
    
    sent = False
    
    # ============================================================
    # PRIORITY 1: Telegram
    # ============================================================
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if telegram_token and telegram_chat_id:
        print("📤 Sending Telegram alert...", file=sys.stderr)
        if send_telegram(telegram_token, telegram_chat_id, tg_message):
            print("✅ Telegram alert sent", file=sys.stderr)
            sent = True
    
    # ============================================================
    # PRIORITY 2: Discord
    # ============================================================
    if not sent:
        discord_webhook = os.getenv("DISCORD_WEBHOOK_URL")
        if discord_webhook:
            print("📤 Sending Discord alert...", file=sys.stderr)
            if send_discord(discord_webhook, tg_message):
                print("✅ Discord alert sent", file=sys.stderr)
                sent = True
    
    # ============================================================
    # PRIORITY 3: Generic Webhook
    # ============================================================
    if not sent:
        generic_webhook = os.getenv("ALERT_WEBHOOK_URL")
        if generic_webhook:
            print("📤 Sending Generic Webhook alert...", file=sys.stderr)
            if send_generic_webhook(generic_webhook, webhook_payload):
                print("✅ Generic webhook alert sent", file=sys.stderr)
                sent = True
    
    # ============================================================
    # FALLBACK: File Log (always executed for audit trail)
    # ============================================================
    log_message = f"schedule={args.source} | exit_code={args.exit_code} | {args.message}"
    log_to_file(log_message)
    print(f"📝 Alert logged to logs/pipeline_alerts.log", file=sys.stderr)
    
    if not sent:
        print("⚠️ No alert channel configured (set TELEGRAM_BOT_TOKEN/CHAT_ID, DISCORD_WEBHOOK_URL, or ALERT_WEBHOOK_URL)", file=sys.stderr)
    
    # Always exit 0 so wrapper script doesn't loop
    return 0


if __name__ == "__main__":
    sys.exit(main())