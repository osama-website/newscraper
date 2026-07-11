"""
import_articles.py — bulk-import scraped article JSONL directly into the
news-scraper coordinator's DB + storage, bypassing the HTTP claim/submit API.

Safe to run standalone alongside the live uvicorn process: the DB is WAL-mode
SQLite, which supports a second short-lived writer safely.

Each input line must be a JSON object with at least: {"source", "url"} and
ideally the same shape /batch/submit expects: headline, pub_date, author,
body, html.

Effect per record:
  - Appended to server/data/<source>/<source>_articles.jsonl (dedup by URL
    within this run only, matching the coordinator's own JsonlArticleStore).
  - urls row for that (source, url) is set to status='done', done_at=now.
    If no row exists yet (URL wasn't in the ingested manifest), one is
    inserted directly as 'done'.

Run (from repo root, e.g. /home/fast-data/hussam/news-scraper):
    python3 import_articles.py --source bbcurdu < articles.jsonl
    cat chunk1.jsonl chunk2.jsonl | python3 import_articles.py --source nawaiwaqt
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

SERVER_DIR = Path(__file__).resolve().parent
DB_PATH = SERVER_DIR / "state.sqlite"
DATA_DIR = SERVER_DIR / "data"


def main() -> None:
    p = argparse.ArgumentParser(description="Bulk-import scraped articles into the coordinator DB")
    p.add_argument("--source", required=True, help="Coordinator source id, e.g. bbcurdu, nawaiwaqt")
    p.add_argument("--input", default="-", help="JSONL path, or '-' for stdin (default)")
    args = p.parse_args()

    out_path = DATA_DIR / args.source / f"{args.source}_articles.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    fh_in = sys.stdin if args.input == "-" else open(args.input, "r", encoding="utf-8")

    # Unlike the coordinator's own JsonlArticleStore (which only dedupes within
    # one process lifetime, since it's written once per weeks-long uptime),
    # this tool is meant to be re-run repeatedly over the same output file —
    # so it must scan for already-written URLs on every invocation.
    seen: set[str] = set()
    if out_path.exists():
        with out_path.open("r", encoding="utf-8") as existing:
            for line in existing:
                try:
                    seen.add(json.loads(line)["url"])
                except (json.JSONDecodeError, KeyError):
                    continue

    written = 0
    marked_done = 0
    inserted_new = 0
    skipped_dup = 0
    bad_lines = 0
    now = int(time.time())

    with open(out_path, "a", encoding="utf-8") as out_fh:
        for line in fh_in:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue

            url = rec.get("url")
            if not url:
                bad_lines += 1
                continue
            if url in seen:
                skipped_dup += 1
                continue
            seen.add(url)

            rec.setdefault("source", args.source)
            out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1

            cur = conn.execute(
                "UPDATE urls SET status='done', done_at=?, error=NULL, "
                "lease_token=NULL, lease_expires=NULL WHERE url=? AND source=?",
                (now, url, args.source),
            )
            if cur.rowcount == 0:
                conn.execute(
                    "INSERT OR IGNORE INTO urls(source, url, status, done_at) VALUES (?,?,'done',?)",
                    (args.source, url, now),
                )
                inserted_new += 1
            else:
                marked_done += 1

            if written % 500 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    if fh_in is not sys.stdin:
        fh_in.close()

    print(
        f"[{args.source}] written={written} marked_done={marked_done} "
        f"inserted_new_done_rows={inserted_new} skipped_dup_in_batch={skipped_dup} "
        f"bad_lines={bad_lines} -> {out_path}"
    )


if __name__ == "__main__":
    main()
