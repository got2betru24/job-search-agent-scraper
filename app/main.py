"""
Scraper Service — FastAPI Entry Point
-------------------------------------
Internal service only. Not exposed via Traefik.
Reachable by the backend at http://job-search-agent-scraper:9000

Endpoints:
  POST /run          — scrape all active sources
  POST /run/{id}     — scrape a single source by ID
  GET  /health       — health check
"""

import logging
from fastapi import FastAPI, HTTPException
from app.runner import run_all

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Job Search Agent — Scraper Service",
    version="0.1.0",
    docs_url="/docs",
)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "scraper"}


@app.post("/run")
async def run_scrape_all():
    """Scrape all active sources."""
    results = await run_all()
    return {
        "sources_scraped": len(results),
        "results": [
            {
                "source_id":    r.source_id,
                "company":      r.company,
                "status":       r.status,
                "jobs_found":   r.jobs_found,
                "jobs_added":   r.jobs_added,
                "jobs_filtered": r.jobs_filtered,
                "jobs_skipped": r.jobs_skipped,
                "error":        r.error,
            }
            for r in results
        ],
    }


@app.post("/run/{source_id}")
async def run_scrape_source(source_id: int):
    """Scrape a single source by ID."""
    results = await run_all(source_id=source_id)
    if not results:
        raise HTTPException(status_code=404, detail="Source not found or inactive")
    r = results[0]
    return {
        "source_id":    r.source_id,
        "company":      r.company,
        "status":       r.status,
        "jobs_found":   r.jobs_found,
        "jobs_added":   r.jobs_added,
        "jobs_filtered": r.jobs_filtered,
        "jobs_skipped": r.jobs_skipped,
        "error":        r.error,
    }
