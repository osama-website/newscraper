#!/usr/bin/env python3
"""
scrape_direct.py — Scrape a source straight from a URL list, no coordinator
claim/submit needed.

Use this for sources the coordinator doesn't (yet) have registered in its
in-memory /sources map, or when you just want to scrape from any machine
with network access to the site (no obelix reachability required at all).

Output is article JSONL, one record per line, in the exact shape
server/import_articles.py expects: {source, url, html, headline, pub_date,
author, body}.

Scrape to a local, resumable file:
    python3 client/scrape_direct.py --source bbcurdu \
        --urls-file New_Downloader/bbcurdu_urls.txt \
        --output bbcurdu_articles.jsonl

Or stream straight into the coordinator's DB over SSH (no intermediate file,
requires SSH access to the DB host):
    python3 client/scrape_direct.py --source nawaiwaqt \
        --urls-file New_Downloader/nawaiwaqt_urls.txt \
        | ssh sarmad@obelix.cs.uiowa.edu \
            "cd /home/fast-data/hussam/news-scraper && python3 import_articles.py --source nawaiwaqt"
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
from pathlib import Path
from typing import Any, Optional

import aiohttp

CLIENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CLIENT_DIR.parent
SCRAPERS_DIR = REPO_ROOT / "New_Downloader" / "scrapers"

# Source id -> "module:ClassName" under New_Downloader/scrapers/.
# Kept in sync with server/sources.py's parser column.
PARSER_MAP = {
    "dawn": "dawn:DawnScraper",
    "express": "expressnews:ExpressNewsScraper",
    "geo": "geo:GeoScraper",
    "jang": "jang:JangScraper",
    "thenews": "thenews:TheNewsScraper",
    "tribune": "tribune:TribuneScraper",
    "bbcurdu": "bbcurdu:BbcUrduScraper",
    "nawaiwaqt": "nawaiwaqt:NawaiWaqtScraper",
}


def load_parser_class(parser_spec: str) -> type:
    mod_name, _, cls_name = parser_spec.partition(":")
    if not mod_name or not cls_name:
        raise ValueError(f"bad parser spec: {parser_spec!r}")
    sys.path.insert(0, str(SCRAPERS_DIR))
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)


def load_done_urls(output: Optional[str]) -> set[str]:
    if not output:
        return set()
    path = Path(output)
    if not path.exists():
        return set()
    done: set[str] = set()
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(json.loads(line)["url"])
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def failed_log_path(output: Optional[str]) -> Optional[Path]:
    return Path(f"{output}.failed") if output else None


def load_failed_urls(output: Optional[str]) -> set[str]:
    path = failed_log_path(output)
    if not path or not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as fh:
        return {line.strip() for line in fh if line.strip()}


async def scrape_one(scraper: Any, url: str, sem: asyncio.Semaphore, source: str) -> Optional[dict]:
    async with sem:
        try:
            html = await scraper._raw_fetch(url)
            if html is None:
                return None
            fields = scraper.parse_article(html, url)
            if not fields:
                return None
            body = fields.get("body")
            if not body or not body.strip():
                # Some pages match the article URL pattern but carry no text
                # body (photo/video-only pages, galleries, etc.) — a headline
                # alone isn't an article; don't let these masquerade as done.
                return None
            return {
                "source": source,
                "url": url,
                "html": html,
                "headline": fields.get("headline"),
                "pub_date": fields.get("pub_date"),
                "author": fields.get("author"),
                "body": body,
            }
        except Exception as exc:
            print(f"skip {url}: {exc}", file=sys.stderr)
            return None


async def run(args: argparse.Namespace) -> None:
    parser_spec = PARSER_MAP.get(args.source)
    if not parser_spec:
        print(f"unknown source: {args.source} (known: {sorted(PARSER_MAP)})", file=sys.stderr)
        sys.exit(1)
    ParserCls = load_parser_class(parser_spec)

    done = load_done_urls(args.output)
    failed = load_failed_urls(args.output)
    skip = done | failed
    if done or failed:
        print(f"resuming: {len(done)} done, {len(failed)} previously failed (skipped)", file=sys.stderr)

    with open(args.urls_file, "r", encoding="utf-8") as fh:
        urls = [line.strip() for line in fh if line.strip() and line.strip() not in skip]
    print(f"{len(urls)} URLs to scrape", file=sys.stderr)

    out_fh = open(args.output, "a", encoding="utf-8") if args.output else None
    fail_path = failed_log_path(args.output)
    fail_fh = fail_path.open("a", encoding="utf-8") if fail_path else None

    def emit(record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False)
        if out_fh:
            out_fh.write(line + "\n")
            out_fh.flush()
        else:
            print(line)
            sys.stdout.flush()

    def emit_failed(url: str) -> None:
        # _raw_fetch already retries transient errors internally before
        # giving up (see base.py), so a None result here means the URL is
        # effectively permanently unfetchable (404, parse-empty, etc.) —
        # record it so a restarted run doesn't loop on it forever.
        if fail_fh:
            fail_fh.write(url + "\n")
            fail_fh.flush()

    dummy_out = REPO_ROOT / "client" / ".scrape_direct_dummy.jsonl"
    dummy_out.parent.mkdir(parents=True, exist_ok=True)
    scraper = ParserCls(output_file=str(dummy_out), max_concurrent=args.concurrency)

    timeout = aiohttp.ClientTimeout(total=35)
    connector = aiohttp.TCPConnector(limit=args.concurrency + 10, ttl_dns_cache=300)
    headers = {
        "User-Agent": "news-scraper-client/1.0",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7,ur;q=0.5",
        # No "br": aiohttp's Brotli decode fails against some sites (e.g.
        # thenews.com.pk returns HTTP 400 "Can not decode content-encoding: br").
        "Accept-Encoding": "gzip, deflate",
    }
    sem = asyncio.Semaphore(args.concurrency)

    ok = 0
    failed_n = 0
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        scraper._session = session
        for i in range(0, len(urls), args.chunk):
            chunk = urls[i : i + args.chunk]
            results = await asyncio.gather(*[scrape_one(scraper, u, sem, args.source) for u in chunk])
            for url, rec in zip(chunk, results):
                if rec:
                    emit(rec)
                    ok += 1
                else:
                    emit_failed(url)
                    failed_n += 1
            print(
                f"progress: {min(i + args.chunk, len(urls))}/{len(urls)} attempted, {ok} ok, {failed_n} failed",
                file=sys.stderr,
            )
        scraper._session = None

    if out_fh:
        out_fh.close()
    if fail_fh:
        fail_fh.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Scrape a source directly from a URL list, no coordinator claim/submit needed")
    p.add_argument("--source", required=True, choices=sorted(PARSER_MAP))
    p.add_argument("--urls-file", required=True, help="Plain text, one URL per line")
    p.add_argument("--output", default=None, help="Resumable local JSONL file; omit to stream JSONL to stdout")
    p.add_argument("--concurrency", type=int, default=20)
    p.add_argument("--chunk", type=int, default=500, help="URLs per progress-reporting batch")
    args = p.parse_args()

    if not SCRAPERS_DIR.is_dir():
        print(f"Scrapers dir not found: {SCRAPERS_DIR} (clone full repo)", file=sys.stderr)
        sys.exit(1)

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
