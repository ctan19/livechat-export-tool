#!/usr/bin/env python3
"""Ekspor arsip LiveChat v3.6 per bulan, resumable, data mentah + pesan.

Memakai API list_archives dengan Personal Access Token (Basic Auth).
Python 3.10+, dependency: requests. Tidak pernah mengirim data ke selain endpoint LiveChat.
Tidak mengambil isi lampiran file; hanya metadata/URL yang dikirim API.
"""

import argparse
import csv
from contextlib import contextmanager
import getpass
import io
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, time as date_time, timedelta, timezone
from pathlib import Path

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.exit("Modul requests belum terpasang. Jalankan: py -m pip install requests")

API = "https://api.livechatinc.com/v3.6/agent/action/list_archives"
CSV_COLUMNS = [
    "chat_id", "thread_id", "thread_created_at", "event_id", "event_created_at",
    "sender_type", "sender_name", "author_id", "text", "visibility", "welcome_message",
]
RETRIABLE = {429, 500, 502, 503, 504}
COUNTER_KEYS = (
    "pages", "threads", "chats_unique", "messages", "messages_nonempty",
    "customer_messages", "agent_messages", "other_messages", "other_events",
    "restricted_threads", "duplicate_threads", "invalid_events",
)


def params():
    p = argparse.ArgumentParser(
        description="Ekspor semua arsip LiveChat yang dapat diakses, lengkap dengan isi percakapan"
    )
    p.add_argument("--from-date", default="2023-01-01", help="Tanggal awal YYYY-MM-DD")
    p.add_argument("--to-date", default="2026-11-12", help="Tanggal akhir inklusif YYYY-MM-DD")
    p.add_argument("--tz", type=int, default=7, help="Offset waktu; default +7 WIB")
    p.add_argument("--output", help="Folder hasil terpisah; default livechat_FULL_<awal>_<akhir>")
    p.add_argument("--pages-per-part", type=int, default=50, help="Jumlah halaman per file, default 50")
    p.add_argument("--max-pages", type=int, default=None, help="Maksimum halaman pada eksekusi ini (untuk uji coba)")
    p.add_argument("--sleep", type=float, default=0.2, help="Jeda antarpanggilan API dalam detik")
    p.add_argument("--status", action="store_true", help="Lihat progres tanpa login API")
    p.add_argument("--restart-current-month", action="store_true",
                   help="Mulai ulang pagination bulan aktif jika page_id kedaluwarsa; data lama tidak dobel")
    a = p.parse_args()
    try:
        start, end = date.fromisoformat(a.from_date), date.fromisoformat(a.to_date)
    except ValueError:
        p.error("Format tanggal harus YYYY-MM-DD")
    if start > end:
        p.error("Tanggal awal harus <= tanggal akhir")
    if not (-12 <= a.tz <= 14):
        p.error("--tz harus antara -12 dan +14")
    if not (1 <= a.pages_per_part <= 1000):
        p.error("--pages-per-part harus antara 1 dan 1000")
    if a.max_pages is not None and a.max_pages <= 0:
        p.error("--max-pages harus > 0")
    if a.sleep < 0:
        p.error("--sleep tidak boleh negatif")
    a.output = Path(a.output or f"livechat_FULL_{a.from_date}_{a.to_date}").resolve()
    return a


def windows(start_date, end_date, tz_hours):
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    tz = timezone(timedelta(hours=tz_hours))
    output = []
    current = start
    while current <= end:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        last_day = min(end, next_month - timedelta(days=1))
        lower = datetime.combine(current, date_time.min, tzinfo=tz)
        upper = datetime.combine(last_day, date_time.max, tzinfo=tz)
        output.append((current.strftime("%Y-%m"), lower.isoformat(timespec="microseconds"),
                       upper.isoformat(timespec="microseconds")))
        current = last_day + timedelta(days=1)
    return output


def zero_counts():
    return {k: 0 for k in COUNTER_KEYS}


def config_for(a):
    return {"from": a.from_date, "to": a.to_date, "tz": a.tz,
            "pages_per_part": a.pages_per_part, "api": API}


def open_database(base, config):
    base.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(base / "checkpoint.sqlite3"), timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS seen_threads (chat_id TEXT NOT NULL, thread_id TEXT NOT NULL, "
               "PRIMARY KEY(chat_id, thread_id)) WITHOUT ROWID")
    db.execute("CREATE TABLE IF NOT EXISTS seen_chats (chat_id TEXT PRIMARY KEY) WITHOUT ROWID")
    db.execute("CREATE TABLE IF NOT EXISTS job (id INTEGER PRIMARY KEY CHECK (id=1), "
               "config TEXT NOT NULL, month_index INTEGER NOT NULL, page_id TEXT, "
               "pages INTEGER NOT NULL, part INTEGER NOT NULL, raw_offset INTEGER NOT NULL, "
               "csv_offset INTEGER NOT NULL, complete INTEGER NOT NULL, counters TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS monthly (month TEXT PRIMARY KEY, pages INTEGER NOT NULL DEFAULT 0, "
               "threads INTEGER NOT NULL DEFAULT 0, messages INTEGER NOT NULL DEFAULT 0, "
               "found_estimate INTEGER, completed INTEGER NOT NULL DEFAULT 0)")
    old = db.execute("SELECT config FROM job WHERE id=1").fetchone()
    if old is None:
        db.execute("INSERT INTO job VALUES (1, ?, 0, NULL, 0, 0, 0, 0, 0, ?)",
                   (json.dumps(config, sort_keys=True), json.dumps(zero_counts())))
        db.commit()
    elif json.loads(old[0]) != config:
        db.close()
        raise RuntimeError(
            "Folder output sudah memiliki ekspor dengan tanggal/pengaturan BERBEDA. "
            "Pakai --output folder_baru atau samakan parameter lama."
        )
    return db


def load_job(db):
    rec = db.execute("SELECT month_index,page_id,pages,part,raw_offset,csv_offset,complete,counters "
                     "FROM job WHERE id=1").fetchone()
    return dict(month_index=rec[0], page_id=rec[1], pages=rec[2], part=rec[3],
                raw_offset=rec[4], csv_offset=rec[5], complete=bool(rec[6]),
                counts=json.loads(rec[7]))


def part_path(base, subdir, index, ext):
    return base / subdir / f"part_{index:05d}.{ext}"


def recover_output(base, job):
    """Potong tail yang ditulis setelah checkpoint namun sebelum DB commit."""
    for subdir, ext, bytes_committed in (
        ("raw_threads", "jsonl", job["raw_offset"]),
        ("messages", "csv", job["csv_offset"]),
    ):
        folder = base / subdir
        folder.mkdir(parents=True, exist_ok=True)
        for f in folder.glob(f"part_*.{ext}"):
            found = re.fullmatch(r"part_(\d+)\." + ext, f.name)
            if not found:
                continue
            number = int(found.group(1))
            if number > job["part"] or job["part"] == 0:
                f.unlink()
            elif number == job["part"]:
                if f.stat().st_size < bytes_committed:
                    raise RuntimeError(f"File hasil lebih pendek dari checkpoint: {f}. Jangan lanjutkan.")
                with f.open("r+b") as h:
                    h.truncate(bytes_committed)
        for i in range(1, job["part"] + 1):
            f = part_path(base, subdir, i, ext)
            if not f.is_file():
                raise RuntimeError(f"Bagian output sebelumnya hilang: {f}. Tidak aman melanjutkan.")


def count_status(base, job, month_windows):
    print("\n=== STATUS EKSPOR LIVECHAT ===")
    if job["complete"]:
        print("STATUS: SELESAI (semua halaman API dalam rentang waktu telah diproses)")
    elif job["month_index"] < len(month_windows):
        print("STATUS: BELUM SELESAI; bulan aktif:", month_windows[job["month_index"]][0])
    for label, key in [
        ("Halaman sukses", "pages"), ("Thread unik", "threads"),
        ("Chat ID unik", "chats_unique"), ("Event message", "messages"),
        ("Pesan berisi teks", "messages_nonempty"),
        ("Pesan customer", "customer_messages"), ("Pesan agent", "agent_messages"),
        ("Pesan pengirim lain", "other_messages"),
        ("Event lain (file/form/system)", "other_events"),
        ("Thread dengan event tidak tersedia", "restricted_threads"),
        ("Thread duplikat dilewati", "duplicate_threads"),
    ]:
        print(f"{label}: {job['counts'].get(key, 0):,}")
    print("Output:", base)
    print("Catatan: file lampiran fisik tidak diunduh; hanya metadata dan URL pada raw JSONL.\n")


def fetch_page(session, auth, request_body):
    for attempt in range(1, 13):
        try:
            response = session.post(API, auth=auth, json=request_body, timeout=(20, 120))
        except requests.RequestException as exc:
            if attempt == 12:
                raise RuntimeError(f"Koneksi gagal setelah 12 percobaan: {exc}") from exc
            pause = min(2 ** attempt, 120)
            print(f"Koneksi bermasalah, retry dalam {pause}s ...", flush=True)
            time.sleep(pause)
            continue
        if response.status_code in RETRIABLE:
            if attempt == 12:
                raise RuntimeError(f"API HTTP {response.status_code} terus gagal: {response.text[:200]}")
            header = response.headers.get("Retry-After", "0")
            try:
                retry_after = float(header)
            except ValueError:
                retry_after = 0
            pause = min(max(retry_after, 2 ** attempt), 120)
            print(f"API HTTP {response.status_code}, retry dalam {pause:.0f}s ...", flush=True)
            time.sleep(pause)
            continue
        if not response.ok:
            msg = response.text[:500].replace("\n", " ")
            if response.status_code == 422 and "page_id" in msg:
                msg += " | Jika cursor kedaluwarsa gunakan --restart-current-month"
            raise RuntimeError(f"LiveChat HTTP {response.status_code}: {msg}")
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError("API memberi respons bukan JSON") from exc
        if not isinstance(data, dict) or not isinstance(data.get("chats"), list):
            raise RuntimeError("Respons API tidak mengandung chats[]; ekspor dihentikan agar tidak kehilangan data")
        return data
    raise RuntimeError("Gagal mengambil halaman")


def safe_csv(text):
    """Kurangi risiko formula injection ketika CSV dibuka Excel/LibreOffice."""
    s = "" if text is None else str(text)
    if s.lstrip(" \t\r\n").startswith(("=", "+", "-", "@")) or s.startswith(("\t", "\r", "\n")):
        return "'" + s
    return s


def make_csv_bytes(rows, need_header):
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\r\n")
    if need_header:
        writer.writerow(CSV_COLUMNS)
    for row in rows:
        writer.writerow([safe_csv(v) for v in row])
    payload = buf.getvalue().encode("utf-8")
    return (b"\xef\xbb\xbf" + payload) if need_header else payload


def write_page(db, base, job, month, data, pages_per_part, month_windows):
    index = job["pages"] // pages_per_part + 1
    raw_file = part_path(base, "raw_threads", index, "jsonl")
    csv_file = part_path(base, "messages", index, "csv")
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    # Baru pertama kali di index ini? Sebelumnya checkpoint belum menyertakannya.
    is_new_file = index > job["part"]
    before_raw = 0 if is_new_file else job["raw_offset"]
    before_csv = 0 if is_new_file else job["csv_offset"]
    current_counts = dict(job["counts"])
    current_counts["pages"] += 1
    raw_records = []
    csv_rows = []
    added_threads = 0
    added_messages = 0
    next_page = data.get("next_page_id") or None
    if next_page and next_page == job["page_id"]:
        raise RuntimeError("next_page_id sama dengan page_id; hentikan agar tidak looping")
    # DB dan checkpoint dalam transaksi yang sama: jika gagal, bisa resume aman.
    db.execute("BEGIN IMMEDIATE")
    try:
        for chat in data["chats"]:
            if not isinstance(chat, dict) or not isinstance(chat.get("thread"), dict):
                raise RuntimeError("Data chat/thread tidak sesuai; halaman tidak disimpan")
            thread = chat["thread"]
            cid, tid = chat.get("id"), thread.get("id")
            if not isinstance(cid, str) or not cid or not isinstance(tid, str) or not tid:
                raise RuntimeError("Chat ID / Thread ID kosong; halaman tidak disimpan")
            inserted = db.execute("INSERT OR IGNORE INTO seen_threads VALUES (?, ?)", (cid, tid))
            if not inserted.rowcount:
                current_counts["duplicate_threads"] += 1
                continue
            if db.execute("INSERT OR IGNORE INTO seen_chats VALUES (?)", (cid,)).rowcount:
                current_counts["chats_unique"] += 1
            current_counts["threads"] += 1
            added_threads += 1
            raw_records.append(json.dumps(chat, ensure_ascii=False, separators=(",", ":")))
            events = thread.get("events")
            if events is None:
                current_counts["restricted_threads"] += 1
                continue
            if not isinstance(events, list):
                raise RuntimeError("thread.events bukan array; halaman tidak disimpan")
            user_map = {u.get("id"): u for u in chat.get("users", [])
                        if isinstance(u, dict) and isinstance(u.get("id"), str)}
            for ev in events:
                if not isinstance(ev, dict):
                    current_counts["invalid_events"] += 1
                    continue
                if ev.get("type") != "message":
                    current_counts["other_events"] += 1
                    continue
                current_counts["messages"] += 1
                added_messages += 1
                msg = ev.get("text")
                if isinstance(msg, str) and msg.strip():
                    current_counts["messages_nonempty"] += 1
                author_id = ev.get("author_id")
                user = user_map.get(author_id, {})
                kind = user.get("type", "unknown")
                if kind == "customer":
                    current_counts["customer_messages"] += 1
                elif kind in ("agent", "bot"):
                    current_counts["agent_messages"] += 1
                else:
                    current_counts["other_messages"] += 1
                csv_rows.append((cid, tid, thread.get("created_at"), ev.get("id"),
                                 ev.get("created_at"), kind, user.get("name"),
                                 author_id, msg, ev.get("visibility"),
                                 bool((ev.get("properties") or {}).get("lc2", {}).get("welcome_message"))
                                 if isinstance((ev.get("properties") or {}).get("lc2", {}), dict) else False))

        raw_bytes = ("\n".join(raw_records) + ("\n" if raw_records else "")).encode("utf-8")
        csv_bytes = make_csv_bytes(csv_rows, need_header=is_new_file)
        with raw_file.open("ab") as raw_out, csv_file.open("ab") as csv_out:
            if raw_out.tell() != before_raw or csv_out.tell() != before_csv:
                raise RuntimeError("Ukuran file tidak cocok checkpoint; hentikan agar tidak ada data dobel")
            raw_out.write(raw_bytes)
            csv_out.write(csv_bytes)
            raw_out.flush()
            csv_out.flush()
            os.fsync(raw_out.fileno())
            os.fsync(csv_out.fileno())
            raw_size, csv_size = raw_out.tell(), csv_out.tell()

        next_month_index = job["month_index"] + (not next_page)
        complete = int(next_month_index >= len(month_windows))
        db.execute("UPDATE job SET month_index=?,page_id=?,pages=?,part=?,raw_offset=?,csv_offset=?,"
                   "complete=?,counters=? WHERE id=1", (next_month_index, next_page,
                   current_counts["pages"], index, raw_size, csv_size, complete,
                   json.dumps(current_counts, sort_keys=True)))
        db.execute("INSERT OR IGNORE INTO monthly(month) VALUES (?)", (month,))
        db.execute("UPDATE monthly SET pages=pages+1, threads=threads+?, messages=messages+?,"
                   "found_estimate=COALESCE(found_estimate, ?), completed=? WHERE month=?",
                   (added_threads, added_messages, data.get("found_chats"), int(not next_page), month))
        db.commit()
        return current_counts, bool(not next_page)
    except BaseException:
        db.rollback()
        # Tail yang belum commit akan dipotong saat resume berikutnya.
        raise


def save_summary(base, db, job, a, month_windows):
    result = {
        "status": "SELESAI" if job["complete"] else "BELUM_SELESAI",
        "period": {"from": a.from_date, "to": a.to_date, "timezone": f"UTC{a.tz:+d}"},
        "description": "list_archives adalah per-thread; total dari Reports API tidak otomatis sama",
        "counts": job["counts"],
        "months": [dict(month=r[0], pages=r[1], threads=r[2], messages=r[3],
                        estimated_archives=r[4], completed=bool(r[5]))
                   for r in db.execute("SELECT month,pages,threads,messages,found_estimate,completed "
                                       "FROM monthly ORDER BY month")],
        "limits": [
            "Hanya arsip/thread yang dapat diakses token; chat yang masih aktif tidak dicakup",
            "Event pada thread restricted_access mungkin tidak tersedia",
            "Konten file/gambar tidak diunduh; hanya URL/metadata di raw_threads JSONL",
            "Pesan CSV termasuk welcome message otomatis dengan type=message",
            "Satu chat_id bisa memiliki beberapa thread_id; laporan total_chats berbeda definisi",
        ],
    }
    target = base / "export_info.json"
    temp = base / "export_info.json.tmp"
    with temp.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, target)


@contextmanager
def exclusive_export_lock(folder):
    """Prevent two writers from corrupting the same output/checkpoint."""
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / ".export.lock").open("a+b") as lock_file:
        if lock_file.tell() == 0:
            lock_file.write(b"0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("Ekspor lain sedang berjalan pada folder ini. Jangan jalankan bersamaan.") from exc
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise RuntimeError("Ekspor lain sedang berjalan pada folder ini. Jangan jalankan bersamaan.") from exc
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def run_export(a):
    month_windows = windows(a.from_date, a.to_date, a.tz)
    config = config_for(a)
    db = open_database(a.output, config)
    try:
        job = load_job(db)
        if a.status:
            count_status(a.output, job, month_windows)
            return
        recover_output(a.output, job)
        if job["complete"]:
            print("Sudah selesai; tidak ada request baru.")
            count_status(a.output, job, month_windows)
            return
        if a.restart_current_month:
            if not job["page_id"]:
                print("Bulan aktif sudah berada di halaman pertama; tidak perlu reset cursor")
            else:
                db.execute("UPDATE job SET page_id=NULL WHERE id=1")
                db.commit()
                job["page_id"] = None
                print("Cursor pagination bulan aktif di-reset. Thread lama akan dilewati (deduplikasi).")

        account = os.environ.get("LIVECHAT_ACCOUNT_ID") or input("Account ID LiveChat: ").strip()
        token = os.environ.get("LIVECHAT_PAT") or getpass.getpass("Personal Access Token (tidak terlihat): ").strip()
        if not account or not token:
            raise RuntimeError("Account ID / token kosong")
        print(f"\nMemulai / melanjutkan: {a.from_date} s.d. {a.to_date} (UTC{a.tz:+d})")
        print("Folder:", a.output)
        print("Jangan jalankan 2 proses ekspor sekaligus ke folder output yang sama.")
        per_run = 0
        with requests.Session() as session:
            session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
            auth = HTTPBasicAuth(account, token)
            while True:
                job = load_job(db)
                if job["complete"] or (a.max_pages is not None and per_run >= a.max_pages):
                    break
                month, start_ts, end_ts = month_windows[job["month_index"]]
                payload = {"page_id": job["page_id"]} if job["page_id"] else {
                    "filters": {"from": start_ts, "to": end_ts},
                    "limit": 100, "sort_order": "asc",
                }
                data = fetch_page(session, auth, payload)
                new_counts, end_of_month = write_page(
                    db, a.output, job, month, data, a.pages_per_part, month_windows
                )
                per_run += 1
                if per_run == 1 or per_run % 10 == 0 or end_of_month:
                    print(f"{month} | halaman {new_counts['pages']:,} | thread {new_counts['threads']:,} | "
                          f"chat unik {new_counts['chats_unique']:,} | "
                          f"event pesan {new_counts['messages']:,} | "
                          f"berisi teks {new_counts['messages_nonempty']:,}", flush=True)
                if end_of_month:
                    print(f"Bulan {month} selesai. Beralih ke bulan berikutnya.\n", flush=True)
                if per_run % 10 == 0 or end_of_month:
                    save_summary(a.output, db, load_job(db), a, month_windows)
                if a.sleep:
                    time.sleep(a.sleep)
        job = load_job(db)
        save_summary(a.output, db, job, a, month_windows)
        count_status(a.output, job, month_windows)
        if not job["complete"]:
            print("DIJEDA (bukan selesai). Lanjutkan dengan perintah sama tanpa --max-pages.")
        else:
            print("Selesai. File raw ada di raw_threads/; CSV pesan ada di messages/.")
    finally:
        db.close()


def main():
    a = params()
    if a.status:
        run_export(a)
    else:
        with exclusive_export_lock(a.output):
            run_export(a)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDihentikan pengguna. Progres terakhir sudah disimpan; jalankan kembali untuk melanjutkan.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        print("File yang sudah tersimpan tidak dibuang. Perbaiki kendala, lalu jalankan ulang.", file=sys.stderr)
        sys.exit(1)
