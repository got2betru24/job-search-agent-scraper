#!/usr/bin/env python3
"""
dry_run.py
----------
Runs the full scraper filter pipeline for one or more sources
without writing anything to the database.

Useful for validating filter changes (role classification, blocked
keywords, location/department matching) against live job data before
deploying to production.

Usage:
    # By company name (partial match, case-insensitive)
    python3 dry_run.py airbnb
    python3 dry_run.py adobe pinterest

    # By extractor type
    python3 dry_run.py --type greenhouse
    python3 dry_run.py --type workday

    # All active sources
    python3 dry_run.py --all

    # Custom output file
    python3 dry_run.py airbnb --output /app/scraper/logs/test.log

Output:
    Writes to a timestamped file in /app/scraper/logs/ by default.
    Also prints progress to stdout so you know it's running.
    Per-job decisions with reason — WOULD ADD, FILTERED, or FILTERED blocked_title.
    Summary counts at the end.

Notes:
    - No DB writes at any point — deduplication check always returns False.
    - Pass 2 detail fetches still make real HTTP requests.
    - Respects all env vars: TARGET_ROLES, TARGET_LOCATIONS,
      TARGET_DEPARTMENTS, BLOCKED_TITLE_KEYWORDS.
"""

import sys
import asyncio
import argparse
import os
from datetime import datetime

sys.path.insert(0, "/app/scraper")

from app.registry import get_extractor
from app.database import get_cursor
from app.utils import (
    classify_role,
    get_target_roles,
    role_is_targeted,
    get_target_locations,
    location_is_targeted,
    get_target_departments,
    department_is_targeted,
    get_blocked_title_keywords,
    title_is_blocked,
)


# ── Output ────────────────────────────────────────────────────

_log_file = None

def log(msg: str = ""):
    """Write to both stdout (progress) and the output file."""
    print(msg)
    if _log_file:
        _log_file.write(msg + "\n")
        _log_file.flush()


# ── Counters ──────────────────────────────────────────────────

class RunStats:
    def __init__(self):
        self.found    = 0
        self.added    = 0
        self.filtered = 0
        self.blocked  = 0

    def summary(self) -> str:
        return (
            f"found={self.found}  "
            f"would_add={self.added}  "
            f"filtered={self.filtered}  "
            f"blocked={self.blocked}"
        )


# ── Core dry-run logic ────────────────────────────────────────

async def dry_run_source(source: dict) -> RunStats:
    stats = RunStats()

    target_roles       = get_target_roles()
    target_locations   = get_target_locations()
    target_departments = get_target_departments()
    blocked_keywords   = get_blocked_title_keywords()

    company = source["company"]

    try:
        extractor = get_extractor(source["url"], source.get("extractor_type"))
        log(f"\n[{company}] Using {extractor.__class__.__name__}")

        listings = await extractor.get_listings(source["url"])
        stats.found = len(listings)
        log(f"[{company}] Found {stats.found} listings")

        for listing in listings:

            # Role classification
            role = classify_role(listing.title)
            if not role_is_targeted(role, target_roles):
                stats.filtered += 1
                log(f"[{company}] FILTERED role={role}: {listing.title!r}")
                continue

            # Blocked title keywords
            if title_is_blocked(listing.title, blocked_keywords):
                stats.blocked += 1
                log(f"[{company}] FILTERED blocked_title: {listing.title!r}")
                continue

            # Deduplication — always False in dry run (no DB check)

            # Detail fetch
            try:
                detail = await extractor.get_detail(listing)
            except Exception:
                detail = None

            # Location filter
            location = detail.location if detail else None
            if not location_is_targeted(location, target_locations):
                stats.filtered += 1
                log(f"[{company}] FILTERED location={location!r}: {listing.title!r}")
                continue

            # Department filter
            departments = detail.departments if detail else []
            if not department_is_targeted(departments, target_departments):
                stats.filtered += 1
                log(f"[{company}] FILTERED dept={departments}: {listing.title!r}")
                continue

            # Would be added
            stats.added += 1
            log(f"[{company}] WOULD ADD role={role} location={location!r}: {listing.title!r}")

    except Exception as e:
        log(f"[{company}] ERROR: {e}")

    return stats


# ── Source loading ────────────────────────────────────────────

def get_sources(companies: list[str] = None, extractor_type: str = None, all_sources: bool = False) -> list:
    with get_cursor() as cursor:
        if all_sources:
            cursor.execute(
                "SELECT * FROM sources WHERE active = TRUE AND requires_js = FALSE ORDER BY company"
            )
        elif extractor_type:
            cursor.execute(
                "SELECT * FROM sources WHERE active = TRUE AND requires_js = FALSE AND extractor_type = %s ORDER BY company",
                (extractor_type,)
            )
        elif companies:
            placeholders = " OR ".join(["company LIKE %s"] * len(companies))
            params = [f"%{c}%" for c in companies]
            cursor.execute(
                f"SELECT * FROM sources WHERE active = TRUE AND requires_js = FALSE AND ({placeholders}) ORDER BY company",
                params
            )
        else:
            return []
        return cursor.fetchall()


# ── Main ──────────────────────────────────────────────────────

async def main():
    global _log_file

    parser = argparse.ArgumentParser(description="Dry-run scraper pipeline without DB writes")
    parser.add_argument("companies", nargs="*", help="Company name(s) to test (partial match)")
    parser.add_argument("--type", dest="extractor_type", help="Run all sources of a given extractor type")
    parser.add_argument("--all", dest="all_sources", action="store_true", help="Run all active sources")
    parser.add_argument("--output", dest="output_path", help="Output file path (default: /app/scraper/logs/dry_run_<timestamp>.log)")
    args = parser.parse_args()

    if not args.companies and not args.extractor_type and not args.all_sources:
        parser.print_help()
        sys.exit(1)

    # Resolve output path
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = args.output_path or f"/app/scraper/logs/dry_run_{timestamp}.log"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    sources = get_sources(
        companies=args.companies,
        extractor_type=args.extractor_type,
        all_sources=args.all_sources,
    )

    if not sources:
        print("No matching active sources found.")
        sys.exit(1)

    print(f"Writing output to: {output_path}")

    with open(output_path, "w") as f:
        _log_file = f

        log(f"DRY RUN — {len(sources)} source(s) — no DB writes")
        log(f"Timestamp:               {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log(f"Output:                  {output_path}")
        log(f"TARGET_ROLES:            {os.getenv('TARGET_ROLES', '(all)')}")
        log(f"TARGET_LOCATIONS:        {os.getenv('TARGET_LOCATIONS', '(all)')[:80]}...")
        log(f"TARGET_DEPARTMENTS:      {os.getenv('TARGET_DEPARTMENTS', '(all)')}")
        log(f"BLOCKED_TITLE_KEYWORDS:  {os.getenv('BLOCKED_TITLE_KEYWORDS', '(none)')}")

        total = RunStats()
        for source in sources:
            if source.get("requires_js"):
                log(f"\n[{source['company']}] SKIPPED — requires_js=TRUE")
                continue
            stats = await dry_run_source(source)
            total.found    += stats.found
            total.filtered += stats.filtered
            total.blocked  += stats.blocked
            total.added    += stats.added
            log(f"[{source['company']}] {stats.summary()}")

        log()
        log("=" * 60)
        log(f"  TOTAL  {total.summary()}")
        log("=" * 60)

    print(f"\nDone. Full output written to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())