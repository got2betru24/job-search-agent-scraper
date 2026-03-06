"""
Scrape Runner
-------------
Orchestrates scraping across sources using the extractor registry.
Handles deduplication, filter matching, role classification,
target role filtering, DB writes, and scrape logging.

Two-pass flow:
  Pass 1 — get_listings() → filter → deduplicate → write to DB
  Pass 2 — get_detail()   → fetch full description → update DB
            Only fires for jobs written in Pass 1 with scrape_status='pending'.
            Extractors that already populate description in Pass 1
            (Greenhouse, Lever, Ashby) return 'scraped' immediately and
            are skipped in Pass 2.
"""

import json
import logging
from typing import Optional

from app.database import get_cursor, update_job_detail
from app.models import ScrapeResult
from app.registry import get_extractor
from app.utils import (
    hash_url,
    title_matches_filters,
    classify_role,
    get_target_roles,
    role_is_targeted,
    get_target_locations,
    location_is_targeted,
    get_target_departments,
    department_is_targeted,
    extract_salary,
)

logger = logging.getLogger(__name__)


async def run_source(source: dict) -> ScrapeResult:
    """
    Run a full two-pass scrape for a single source.
    Pass 1: get_listings() → filter → deduplicate → write to DB
    Pass 2: get_detail()   → fetch full description → update DB
    """
    filters = source.get("filters")
    if filters and isinstance(filters, str):
        filters = json.loads(filters)

    target_roles       = get_target_roles()
    target_locations   = get_target_locations()
    target_departments = get_target_departments()
    if target_roles:
        logger.info(f"Target roles: {target_roles}")
    if target_locations:
        logger.info(f"Target locations: {target_locations}")
    if target_departments:
        logger.info(f"Target departments: {target_departments}")

    result = ScrapeResult(
        source_id=source["id"],
        company=source["company"],
        status="running",
    )

    log_id = _start_log(source["id"])

    try:
        extractor = get_extractor(source["url"], source.get("extractor_type"))
        logger.info(f"[{source['company']}] Using {extractor.__class__.__name__}")

        # ── Pass 1: get listings ─────────────────────────────
        listings = await extractor.get_listings(source["url"])
        result.jobs_found = len(listings)
        logger.info(f"[{source['company']}] Found {len(listings)} listings")

        # Track jobs written as 'pending' so Pass 2 knows what to fetch
        pending_jobs: list[tuple[str, object]] = []  # (url_hash, listing)

        for listing in listings:

            # Title filter
            if not title_matches_filters(listing.title, filters):
                result.jobs_filtered += 1
                logger.debug(f"[{source['company']}] Filtered (title): {listing.title}")
                continue

            # Role classification
            role = classify_role(listing.title)

            # Target role filter
            if not role_is_targeted(role, target_roles):
                result.jobs_filtered += 1
                logger.debug(f"[{source['company']}] Filtered (role={role}): {listing.title}")
                continue

            # Deduplication
            url_hash = hash_url(listing.url)
            if _job_exists(url_hash):
                result.jobs_skipped += 1
                logger.debug(f"[{source['company']}] Skipped (exists): {listing.title}")
                continue

            # ── Pass 2 prep: get detail ──────────────────────
            try:
                detail = await extractor.get_detail(listing)
            except Exception as e:
                if "406" not in str(e):
                    logger.warning(f"[{source['company']}] Detail fetch failed for {listing.url}: {e}")
                detail = None

            # Location filter — applied after detail fetch
            location = detail.location if detail else None
            if not location_is_targeted(location, target_locations):
                result.jobs_filtered += 1
                logger.debug(f"[{source['company']}] Filtered (location={location}): {listing.title}")
                continue

            # Department filter
            departments = detail.departments if detail else []
            if not department_is_targeted(departments, target_departments):
                result.jobs_filtered += 1
                logger.debug(f"[{source['company']}] Filtered (dept={departments}): {listing.title}")
                continue

            scrape_status = _write_job(source, listing.title, listing.url, url_hash, role, detail)
            result.jobs_added += 1
            logger.info(f"[{source['company']}] Added ({role}, {location}): {listing.title}")

            # Queue for Pass 2 if description not yet populated
            if scrape_status == "pending":
                pending_jobs.append((url_hash, listing))

        # ── Pass 2: fetch full descriptions ──────────────────
        if pending_jobs:
            logger.info(f"[{source['company']}] Pass 2: fetching descriptions for {len(pending_jobs)} jobs")
            for url_hash, listing in pending_jobs:
                try:
                    detail = await extractor.get_detail(listing)
                    update_job_detail(url_hash, detail)
                    status = "scraped" if detail and detail.description else "failed"
                    logger.info(f"[{source['company']}] Pass 2 {status}: {listing.title}")
                except Exception as e:
                    logger.warning(f"[{source['company']}] Pass 2 failed for {listing.url}: {e}")
                    _mark_failed(url_hash, str(e))

        _update_source_timestamp(source["id"])
        result.status = "success"

    except Exception as e:
        logger.error(f"[{source['company']}] Scrape failed: {e}", exc_info=True)
        result.status = "failed"
        result.error  = str(e)

    finally:
        _finish_log(log_id, result)

    return result


async def run_all(source_id: Optional[int] = None) -> list[ScrapeResult]:
    sources = _get_sources(source_id)
    if not sources:
        return []

    results = []
    for source in sources:
        if source.get("requires_js"):
            logger.info(f"[{source['company']}] Skipping — requires_js=TRUE (Playwright not yet built)")
            results.append(ScrapeResult(
                source_id=source["id"],
                company=source["company"],
                status="skipped",
                error="requires_js — awaiting Playwright service",
            ))
            continue
        result = await run_source(source)
        results.append(result)

    return results


# ── DB helpers ────────────────────────────────────────────────

def _get_sources(source_id: Optional[int] = None) -> list:
    with get_cursor() as cursor:
        if source_id:
            cursor.execute(
                "SELECT * FROM sources WHERE id = %s AND active = TRUE",
                (source_id,)
            )
        else:
            cursor.execute("SELECT * FROM sources WHERE active = TRUE")
        return cursor.fetchall()


def _job_exists(url_hash: str) -> bool:
    with get_cursor() as cursor:
        cursor.execute("SELECT id FROM jobs WHERE url_hash = %s", (url_hash,))
        return cursor.fetchone() is not None


def _write_job(source: dict, title: str, url: str, url_hash: str, role: Optional[str], detail) -> str:
    """
    Write a new job to the DB. Returns the scrape_status that was set:
      'scraped'  — description already populated from Pass 1
      'pending'  — no description yet, Pass 2 will fetch it
    """
    from datetime import datetime
    import json

    has_description = bool(detail and detail.description and detail.description.strip())
    scrape_status   = "scraped" if has_description else "pending"

    # Fallback salary extraction from description when no structured field
    salary = detail.salary if detail else None
    if not salary and has_description:
        salary = extract_salary(detail.description)
    requirements    = None
    if detail and detail.requirements:
        requirements = json.dumps(detail.requirements)

    with get_cursor() as cursor:
        cursor.execute(
            """INSERT INTO jobs (
                source_id, title, job_url, url_hash, company,
                location, job_type, salary, description, requirements,
                role, scrape_status, scraped_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                source["id"],
                title,
                url,
                url_hash,
                detail.company if detail and detail.company else source["company"],
                detail.location[:500] if detail and detail.location else None,
                detail.job_type if detail else None,
                salary,
                detail.description if has_description else None,
                requirements,
                role,
                scrape_status,
                datetime.utcnow() if has_description else None,
            )
        )

    return scrape_status


def _mark_failed(url_hash: str, error: str) -> None:
    """Mark a job as failed after a Pass 2 error."""
    from datetime import datetime
    with get_cursor() as cursor:
        cursor.execute(
            """UPDATE jobs SET
                scrape_status = 'failed',
                scrape_error  = %s,
                scraped_at    = %s
               WHERE url_hash = %s""",
            (error[:500], datetime.utcnow(), url_hash),
        )


def _update_source_timestamp(source_id: int) -> None:
    with get_cursor() as cursor:
        cursor.execute(
            "UPDATE sources SET last_scraped_at = NOW() WHERE id = %s",
            (source_id,)
        )


def _start_log(source_id: int) -> int:
    with get_cursor() as cursor:
        cursor.execute(
            "INSERT INTO scrape_log (source_id, status) VALUES (%s, 'running')",
            (source_id,)
        )
        cursor.execute("SELECT LAST_INSERT_ID() as id")
        return cursor.fetchone()["id"]


def _finish_log(log_id: int, result: ScrapeResult) -> None:
    with get_cursor() as cursor:
        cursor.execute(
            """UPDATE scrape_log SET
               status = %s, finished_at = NOW(),
               jobs_found = %s, jobs_added = %s,
               jobs_filtered = %s, jobs_skipped = %s,
               error_message = %s
               WHERE id = %s""",
            (
                result.status,
                result.jobs_found,
                result.jobs_added,
                result.jobs_filtered,
                result.jobs_skipped,
                result.error,
                log_id,
            )
        )