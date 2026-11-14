#!/usr/bin/env python3
"""Preview pesan customer dan agent dari output export_semua_livechat.py."""
import argparse
import csv
import sys
from pathlib import Path

p = argparse.ArgumentParser(description="Lihat isi pesan dari ekspor LiveChat (tanpa API)")
p.add_argument("--folder", default="livechat_FULL_2023-01-01_2026-11-12")
p.add_argument("--chat-id", default=None, help="Opsional: tampilkan hanya chat ID ini")
p.add_argument("--thread-id", default=None, help="Opsional: tampilkan hanya thread ID ini")
p.add_argument("--limit", type=int, default=20, help="Batas pesan yang ditampilkan; default 20")
a = p.parse_args()
if a.limit <= 0:
    p.error("--limit harus positif")
folder = Path(a.folder) / "messages"
files = sorted(folder.glob("part_*.csv"))
if not files:
    sys.exit(f"File CSV belum ditemukan dalam {folder}")
shown = 0
for file in files:
    with file.open("r", encoding="utf-8-sig", newline="") as inp:
        reader = csv.DictReader(inp)
        for row in reader:
            if a.chat_id and row.get("chat_id") != a.chat_id:
                continue
            if a.thread_id and row.get("thread_id") != a.thread_id:
                continue
            who = row.get("sender_name") or row.get("sender_type") or "unknown"
            print(f"[{row.get('event_created_at')}] Chat {row.get('chat_id')} / Thread {row.get('thread_id')}")
            print(f"{who} ({row.get('sender_type')}): {row.get('text')}\n")
            shown += 1
            if shown >= a.limit:
                sys.exit(0)
print(f"Ditemukan {shown} pesan sesuai filter")
