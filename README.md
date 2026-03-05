# Job Search Agent — Scraper Service

An independent Python scraping service that monitors curated company career pages, extracts job listings, and writes structured data to MySQL. Designed as a plugin-based system where each ATS platform has its own extractor.

> **This is the scraper service repo.** It is one of three services in the job search agent system:
> - **job-search-agent-frontend** — React/TypeScript UI
> - **job-search-agent-backend** — FastAPI REST API
> - **job-search-agent-scraper** — this repo

---

## What It Does

- Receives scrape trigger requests from the backend via an internal HTTP API
- Routes each source URL to the appropriate extractor based on the ATS platform detected
- Runs a two-pass scrape: Pass 1 discovers job listings, Pass 2 fetches full detail
- Applies title keyword/regex filters before writing to the database
- Deduplicates by URL hash — archived jobs are never re-inserted
- Writes structured job data and scrape run logs directly to MySQL
- Not exposed publicly — only reachable by the backend on `traefik-network`

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Framework | FastAPI (internal API only) |
| HTTP client | httpx (async) |
| HTML parsing | BeautifulSoup4 |
| Database | MySQL 8.0 (shared container) |
| Container | Docker + Compose v2 |

---

## Project Structure

```
scraper/
├── .devcontainer/
│   └── devcontainer.json       # VS Code Dev Container config
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI entry point — /run, /run/{id}, /health
│   ├── runner.py               # Core orchestration — two-pass scrape loop, DB writes
│   ├── registry.py             # Maps source URLs to the right extractor class
│   ├── base.py                 # Abstract BaseExtractor all extractors inherit from
│   ├── database.py             # MySQL connection pool and cursor context managers
│   ├── models.py               # JobListing, JobDetail, ScrapeResult dataclasses
│   ├── utils.py                # URL hashing, title filter matching, default headers
│   └── extractors/
│       ├── __init__.py
│       ├── greenhouse.py       # Greenhouse public API
│       ├── lever.py            # Lever public API
│       ├── ashby.py            # Ashby public API
│       ├── bamboohr.py         # BambooHR JSON API
│       ├── workday.py          # Workday JSON-LD + fallback HTML extraction
│       ├── phenom.py           # Phenom People — JSON blob in script tag
│       ├── oracle.py           # Oracle public API
│       └── generic.py          # Fallback — httpx + BeautifulSoup anchor extraction
├── .env                        # Local secrets and config for TARGET_ROLES, TARGET_LOCATIONS, and TARGET_DEPARTMENTS
├── Dockerfile                  # scraper-base, scraper-dev, scraper-prod stages
├── compose.yml                 # Scraper service only
└── requirements.txt
```

---

## Extractor Architecture

The scraper uses a plugin-based registry pattern. When a scrape run is triggered, each source URL is matched against the registry to select the right extractor:

```
Source URL → registry.get_extractor(url) → Extractor instance
                                                  ↓
                                        get_listings()  ← Pass 1
                                                  ↓
                                    title filter + URL dedup
                                                  ↓
                                         get_detail()   ← Pass 2
                                                  ↓
                                          write to MySQL
```

### Supported Platforms

| Platform | Strategy | Pass 1 & 2 |
|---|---|---|
| **Greenhouse** | Public JSON API | Merged — all data in one API call |
| **Lever** | Public JSON API | Separate listing + detail calls |
| **Ashby** | Public JSON API | Separate listing + detail calls |
| **BambooHR** | JSON API | Separate listing + detail calls |
| **Workday** | JSON-LD in HTML | HTML fetch + structured data extraction |
| **Phenom People** | JSON blob in `<script>` | Merged — all data pre-loaded in page HTML |
| **Generic** | httpx + BeautifulSoup | Anchor tag extraction + detail page scrape |

### Adding a New Extractor

1. Create `app/extractors/myplatform.py` inheriting from `BaseExtractor`
2. Implement `get_listings(source_url)` and `get_detail(listing)`
3. Add an entry to `EXTRACTORS` in `app/registry.py`

```python
# registry.py
from app.extractors.myplatform import MyPlatformExtractor

EXTRACTORS = [
    ...
    ("myplatform.com", MyPlatformExtractor),
]
```

---

## Internal API

The scraper exposes a minimal FastAPI app on port `9000`. It is **not** reachable from outside the Docker network — only the backend service can call it.

| Method | Path | Description |
|---|---|---|
| `POST` | `/run` | Scrape all active sources |
| `POST` | `/run/{source_id}` | Scrape a single source by ID |
| `GET` | `/health` | Health check |

Swagger docs available at `http://job-search-agent-scraper:9000/docs` from within the network.

---

## Two-Pass Scrape Design

**Pass 1 — listing discovery**
- Fetch the career page listing URL
- Extract `(title, url)` pairs using the platform-appropriate method
- Apply title keyword/regex filters — only matching titles proceed
- Check URL SHA-256 hash against the database — skip if already seen

**Pass 2 — detail extraction**
- For each new URL, fetch or derive the full job detail
- Populate structured fields: description, requirements, location, salary, job_type
- Write to `jobs` table with `scrape_status = scraped`
- Log run stats to `scrape_log`

For API-based extractors (Greenhouse, Lever, Ashby), Pass 1 and Pass 2 are merged into a single API call since full detail is available in the listing response.

---

## Deduplication

Jobs are deduplicated by a SHA-256 hash of their URL stored in the `url_hash` column. Once a job URL is in the database — regardless of status — it will never be re-inserted. This means:

- Archived jobs do not resurface on subsequent scrape runs
- Jobs that failed detail scraping (`scrape_status = pending`) are not re-attempted automatically (future: retry queue)

---

## Getting Started

### Prerequisites

- Docker Engine 20.10.21+
- Docker Compose v2.13.0+
- Shared MySQL container running on `traefik-network`
- Backend service running on `traefik-network` (to trigger scrapes)

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env with your actual values
```

Required variables:

| Variable | Description |
|---|---|
| `DB_HOST` | MySQL container name (default: `mysql`) |
| `DB_PORT` | MySQL port (default: `3306`) |
| `DB_NAME` | Database name |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |

### 2. Start the scraper service

```bash
docker compose up --build
```

The service will be reachable by the backend at `http://job-search-agent-scraper:9000`.

### 3. Verify

From within the backend container or any container on `traefik-network`:

```bash
curl http://job-search-agent-scraper:9000/health
# {"status":"healthy","service":"scraper"}
```

### 4. Trigger a test scrape

Update a source in the database with a real URL, then trigger via the backend:

```bash
# Update source in MySQL
UPDATE sources SET company='Vercel', url='https://boards.greenhouse.io/vercel' WHERE id=1;

# Trigger scrape via backend API
curl -X POST http://job-search-agent.local/api/scraper/run/1
```

---

## Startup Order

```
1. MySQL container       (shared — already running)
2. Scraper service       (docker compose up --build)
3. Backend service       (docker compose up --build)
4. Frontend service      (docker compose up --build)
```

The backend must start after the scraper since it proxies scrape requests to it. Both must be on `traefik-network` to communicate.

---

## Development Notes

- **Hot reload** is enabled in `scraper-dev` — `app/` is mounted as a volume
- **VS Code Dev Container** — open the `scraper/` folder and select "Reopen in Container"
- Workday is the most brittle extractor — it falls back through three strategies (JSON-LD → embedded script data → anchor tags). Sources using Workday that return empty results should have `requires_js = TRUE` set in the database for the future Playwright service
- The **Playwright service** for JS-rendered pages is a planned addition — sources with `requires_js = TRUE` are currently skipped

---

## Roadmap

- [ ] Retry queue for jobs with `scrape_status = pending`
- [ ] APScheduler for automatic scheduled scrape runs
- [ ] Playwright service for JS-rendered career pages (`requires_js = TRUE`)
- [ ] Claude integration for structured field extraction (requirements, role classification)
- [ ] Per-source custom extractor overrides for edge cases
- [ ] Scrape run notifications (email/webhook on completion)