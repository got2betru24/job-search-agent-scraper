"""
Greenhouse Extractor
--------------------
Greenhouse exposes a fully public JSON API — no scraping needed.

Listing API:
  https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true

This returns all jobs with full content in one call, so Pass 1 and Pass 2
are effectively merged. get_listings() fetches everything and caches the
detail data. get_detail() just returns from that cache.

Company slug is extracted from the source URL, which typically looks like:
  https://boards.greenhouse.io/{company}
  https://{company}.greenhouse.io/
  https://careers.{company}.com  (custom domain — requires manual slug config)
"""

import re
from typing import List, Dict
from app.base import BaseExtractor
from app.models import JobListing, JobDetail


class GreenhouseExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    def _extract_slug(self, url: str) -> str:
        """
        Extract the Greenhouse company slug from a URL.
        Handles:
          - https://boards.greenhouse.io/{slug}
          - https://{slug}.greenhouse.io/
        """
        # boards.greenhouse.io/{slug}
        match = re.search(r"boards\.greenhouse\.io/([^/?#]+)", url)
        if match:
            return match.group(1)

        # {slug}.greenhouse.io
        match = re.search(r"([^/.]+)\.greenhouse\.io", url)
        if match:
            return match.group(1)

        raise ValueError(f"Could not extract Greenhouse slug from URL: {url}")

    async def get_listings(self, source_url: str) -> List[JobListing]:
        slug = self._extract_slug(source_url)
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

        data = await self.fetch_json(api_url, params={"content": "true"})

        if not data or "jobs" not in data:
            return []

        listings = []
        for job in data["jobs"]:
            title = job.get("title", "").strip()
            url   = job.get("absolute_url", "").strip()

            if not title or not url:
                continue

            # Cache the full detail now since we already have it
            self._detail_cache[url] = self._parse_detail(job)
            listings.append(JobListing(title=title, url=url))

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        # Return from cache populated during get_listings()
        if listing.url in self._detail_cache:
            return self._detail_cache[listing.url]

        # Fallback: fetch individual job by ID if not cached
        # Greenhouse job URLs look like: .../jobs/{id}
        match = re.search(r"/jobs/(\d+)", listing.url)
        if not match:
            return JobDetail(title=listing.title, url=listing.url)

        job_id = match.group(1)
        slug   = self._extract_slug(listing.url)
        data   = await self.fetch_json(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}",
            params={"questions": "false"}
        )

        if not data:
            return JobDetail(title=listing.title, url=listing.url)

        return self._parse_detail(data)

    def _parse_detail(self, job: dict) -> JobDetail:
        """Parse a Greenhouse job object into a JobDetail."""
        # Extract plain text from HTML description
        description = ""
        raw_content = job.get("content", "") or ""
        if raw_content:
            from bs4 import BeautifulSoup
            description = BeautifulSoup(raw_content, "html.parser").get_text(
                separator="\n", strip=True
            )

        # Location — combine primary + otherLocations into one pipe-separated string
        # so location_is_targeted() can match against any of them
        location_parts = []

        loc = job.get("location", {})
        primary = loc.get("name") if isinstance(loc, dict) else (loc if isinstance(loc, str) else None)
        if primary:
            location_parts.append(primary.strip())

        for other in job.get("otherLocations", []):
            name = other.get("name") if isinstance(other, dict) else str(other)
            if name and name.strip() and name.strip() not in location_parts:
                location_parts.append(name.strip())

        location = " | ".join(location_parts) if location_parts else None

        # Departments
        departments = [
            d.get("name")
            for d in job.get("departments", [])
            if d.get("name")
        ]

        # Requirements — Greenhouse doesn't separate these, they're in the
        # description HTML. Claude will parse them out in a future phase.
        return JobDetail(
            title=job.get("title", "").strip(),
            url=job.get("absolute_url", "").strip(),
            location=location,
            departments=departments,
            description=description[:5000] if description else None,
        )