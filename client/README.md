# News scraper client

Python script that talks to the coordinator: claim a batch, download HTML with the same retry behaviour as the scrapers, run `parse_article`, upload JSONL-shaped records.

## Setup

From **repository root** (you need the full clone so `New_Downloader/scrapers/` exists):

```bash
pip install -r client/requirements.txt
```

## Usage

```bash
# One batch then exit
python client/run_client.py --server http://127.0.0.1:8000

# Continuous worker
python client/run_client.py --server http://127.0.0.1:8000 --loop

# Options
python client/run_client.py --server http://HOST:8000 --source tribune --batch-size 50 \
  --concurrency 25 --client-id my-laptop --verbose

# BBC Urdu / Nawa-i-Waqt (once the coordinator has these sources registered — see note below)
python client/run_client.py --server http://HOST:8000 --source bbcurdu --loop --client-id my-laptop
python client/run_client.py --server http://HOST:8000 --source nawaiwaqt --loop --client-id my-laptop
```

- **`--source`**: optional; if omitted the server assigns the source with the largest pending queue.
- **`record["source"]`** is set to the server’s canonical source id (e.g. `express`) so it matches the coordinator even when `scraper.SOURCE_NAME` differs (e.g. `express_news`).
- `--source <name>` only works for sources the coordinator's running process currently has loaded (check `GET /sources`). A source added to `server/sources.py` doesn't take effect until the coordinator process is restarted, since `SOURCES` is imported once at startup.

## Scraping a source the coordinator hasn't loaded yet

If a source (e.g. a freshly added `bbcurdu`/`nawaiwaqt`) isn't in `GET /sources` yet because the coordinator hasn't been restarted, `scrape_direct.py` scrapes straight from a URL list — no coordinator HTTP calls at all — and emits the same record shape `/batch/submit` would have stored:

```bash
# Scrape to a local, resumable file
python client/scrape_direct.py --source bbcurdu \
  --urls-file New_Downloader/bbcurdu_urls.txt \
  --output bbcurdu_articles.jsonl

# Or stream straight into the DB over SSH (needs SSH access to the DB host —
# writes directly to state.sqlite + server/data/, bypassing the HTTP API
# entirely; safe to run alongside the live coordinator since the DB is WAL-mode
# SQLite):
python client/scrape_direct.py --source nawaiwaqt --urls-file New_Downloader/nawaiwaqt_urls.txt \
  | ssh sarmad@obelix.cs.uiowa.edu \
      "cd /home/fast-data/hussam/news-scraper && python3 server/import_articles.py --source nawaiwaqt"
```

`server/import_articles.py` runs on the machine holding `state.sqlite`. It marks each URL `status='done'` (inserting the row if the coordinator's manifest didn't have it yet) and appends to `server/data/<source>/<source>_articles.jsonl` — resumable across repeated runs (it scans the existing output file for already-imported URLs first).

## Requirements

- Python 3.10+
- Network access to the coordinator and to the news sites you scrape.
