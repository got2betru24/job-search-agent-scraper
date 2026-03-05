#!/usr/bin/env python3
"""
discover.py
-----------
Fetches all jobs from sources of a given extractor type and reports
every unique location and department string found across all of them.

Use this to tune TARGET_LOCATIONS and TARGET_DEPARTMENTS in your .env
before running a full scrape.

Usage:
    python3 discover.py greenhouse
    python3 discover.py lever
    python3 discover.py ashby
    python3 discover.py phenom
    python3 discover.py bamboohr
    python3 discover.py all

Output:
    A consolidated report of unique locations and departments
    across all matching sources, with per-company breakdown.
"""

import sys
import asyncio
import os

# Make sure app modules are importable
sys.path.insert(0, "/app/scraper")

from app.database import get_cursor
from app.registry import get_extractor


async def discover_source(source: dict) -> dict:
    """Fetch all listings for a source and collect location/department strings."""
    extractor = get_extractor(source["url"], source.get("extractor_type"))
    result = {
        "company":     source["company"],
        "url":         source["url"],
        "extractor":   extractor.__class__.__name__,
        "locations":   set(),
        "departments": set(),
        "total_jobs":  0,
        "error":       None,
    }

    try:
        listings = await extractor.get_listings(source["url"])
        result["total_jobs"] = len(listings)

        for listing in listings:
            try:
                detail = await extractor.get_detail(listing)

                if detail.location:
                    # Split pipe-separated locations back into individual values
                    for loc in detail.location.split(" | "):
                        loc = loc.strip()
                        if loc:
                            result["locations"].add(loc)

                for dept in (detail.departments if hasattr(detail, "departments") else []):
                    if dept:
                        result["departments"].add(dept)

            except Exception:
                continue

    except Exception as e:
        result["error"] = str(e)

    return result


def print_report(results: list[dict], extractor_type: str):
    """Print a consolidated report of locations and departments."""
    all_locations   = set()
    all_departments = set()

    print()
    print("=" * 70)
    print(f"  DISCOVERY REPORT — {extractor_type.upper()}")
    print("=" * 70)

    for r in results:
        print()
        print(f"  {r['company']} ({r['extractor']}) — {r['total_jobs']} jobs")
        print(f"  URL: {r['url'][:70]}")

        if r["error"]:
            print(f"  ERROR: {r['error']}")
            continue

        if r["locations"]:
            print(f"  Locations ({len(r['locations'])}):")
            for loc in sorted(r["locations"]):
                print(f"    {loc}")
            all_locations.update(r["locations"])
        else:
            print("  Locations: none returned")

        if r["departments"]:
            print(f"  Departments ({len(r['departments'])}):")
            for dept in sorted(r["departments"]):
                print(f"    {dept}")
            all_departments.update(r["departments"])
        else:
            print("  Departments: none returned")

    # Consolidated summary
    print()
    print("=" * 70)
    print(f"  CONSOLIDATED — ALL {extractor_type.upper()} SOURCES")
    print("=" * 70)

    print(f"\n  ALL UNIQUE LOCATIONS ({len(all_locations)}):")
    for loc in sorted(all_locations):
        print(f"    {loc}")

    print(f"\n  ALL UNIQUE DEPARTMENTS ({len(all_departments)}):")
    for dept in sorted(all_departments):
        print(f"    {dept}")

    # Suggest env var values
    print()
    print("=" * 70)
    print("  SUGGESTED .env ADDITIONS")
    print("=" * 70)
    print()
    print("  Review the lists above and add relevant keywords to your .env:")
    print()
    print("  TARGET_LOCATIONS=remote,utah,salt lake,lehi,provo,orem,")
    print("                   american fork,draper,south jordan,ut -,us - remote,")
    print("                   <add more from the list above>")
    print()
    print("  TARGET_DEPARTMENTS=engineering,product,data science,analytics,")
    print("                     platform,technology,")
    print("                     <add more from the list above>")
    print()


async def main():
    extractor_type = sys.argv[1].lower() if len(sys.argv) > 1 else "greenhouse"

    with get_cursor() as cursor:
        if extractor_type == "all":
            cursor.execute(
                "SELECT * FROM sources WHERE active = TRUE AND requires_js = FALSE ORDER BY extractor_type, company"
            )
        else:
            cursor.execute(
                "SELECT * FROM sources WHERE active = TRUE AND requires_js = FALSE AND extractor_type = %s ORDER BY company",
                (extractor_type,)
            )
        sources = cursor.fetchall()

    if not sources:
        print(f"No active sources found for extractor_type='{extractor_type}'")
        sys.exit(1)

    print(f"Discovering locations/departments for {len(sources)} {extractor_type} source(s)...")
    print("This may take a minute depending on the number of jobs...")

    # Group by extractor type if running all
    if extractor_type == "all":
        by_type = {}
        for s in sources:
            t = s.get("extractor_type") or "unknown"
            by_type.setdefault(t, []).append(s)

        for etype, esources in sorted(by_type.items()):
            results = []
            for source in esources:
                print(f"  Fetching {source['company']}...", flush=True)
                result = await discover_source(source)
                results.append(result)
            print_report(results, etype)
    else:
        results = []
        for source in sources:
            print(f"  Fetching {source['company']}...", flush=True)
            result = await discover_source(source)
            results.append(result)
        print_report(results, extractor_type)


if __name__ == "__main__":
    asyncio.run(main())