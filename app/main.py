"""
Scraper Service — FastAPI Entry Point
-------------------------------------
Internal service only. Not exposed via Traefik.
Reachable by the backend at http://job-search-agent-scraper:9000
Endpoints:
  POST /run          — scrape all active sources
  POST /run/{id}     — scrape a single source by ID
  GET  /logs/raw     — return lines from the most recent log file
  GET  /health       — health check
"""
import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from app.runner import run_all

LOG_FILE = "/app/scraper/logs/scraper.log"

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Job Search Agent — Scraper Service",
    version="0.1.0",
    docs_url="/docs",
)


def _reset_log_file() -> None:
    """Remove any existing file handler and attach a fresh one (overwrite mode)."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            root.removeHandler(h)
            h.close()
    fh = logging.FileHandler(LOG_FILE, mode="w")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    root.addHandler(fh)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "scraper"}


@app.post("/run")
async def run_scrape_all():
    """Scrape all active sources."""
    _reset_log_file()
    results = await run_all()
    return {
        "sources_scraped": len(results),
        "results": [
            {
                "source_id":     r.source_id,
                "company":       r.company,
                "status":        r.status,
                "jobs_found":    r.jobs_found,
                "jobs_added":    r.jobs_added,
                "jobs_filtered": r.jobs_filtered,
                "jobs_skipped":  r.jobs_skipped,
                "error":         r.error,
            }
            for r in results
        ],
    }


@app.post("/run/{source_id}")
async def run_scrape_source(source_id: int):
    """Scrape a single source by ID."""
    _reset_log_file()
    results = await run_all(source_id=source_id)
    if not results:
        raise HTTPException(status_code=404, detail="Source not found or inactive")
    r = results[0]
    return {
        "source_id":     r.source_id,
        "company":       r.company,
        "status":        r.status,
        "jobs_found":    r.jobs_found,
        "jobs_added":    r.jobs_added,
        "jobs_filtered": r.jobs_filtered,
        "jobs_skipped":  r.jobs_skipped,
        "error":         r.error,
    }


@app.get("/logs/raw")
async def get_raw_logs(
    source: Optional[str] = Query(None, description="Filter by company name (case-insensitive)"),
    level: Optional[str] = Query(None, description="Filter by log level: INFO, ERROR, WARNING"),
    filter_type: Optional[str] = Query(None, description="Pass 'filtered' to show only FILTERED title lines"),
    limit: int = Query(2000, description="Max number of lines to return"),
):
    """
    Return lines from the most recent scraper run log file.
    The log file is overwritten at the start of each run, so this always
    reflects the most recent run only.
    """
    if not os.path.exists(LOG_FILE):
        return {"lines": [], "total": 0, "note": "No log file found — scraper has not run yet."}

    with open(LOG_FILE, "r") as f:
        lines = [l.rstrip("\n") for l in f.readlines()]

    if source:
        lines = [l for l in lines if f"[{source}]" in l or source.lower() in l.lower()]
    if level:
        lines = [l for l in lines if f"[{level.upper()}]" in l]
    if filter_type == "filtered":
        lines = [l for l in lines if "FILTERED" in l]

    lines = lines[-limit:]

    return {
        "lines": lines,
        "total": len(lines),
        "note": "Log reflects most recent scraper run only (file is overwritten each run).",
    }