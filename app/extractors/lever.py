"""
Lever Extractor
---------------
Lever exposes a fully public JSON API — no scraping needed.

Listing + detail API (all jobs with full content):
  https://api.lever.co/v0/postings/{company}?mode=json

Company slug is extracted from the source URL:
  https://jobs.lever.co/{company}

The listing API returns full job data including description, location, team,
and commitment — so Pass 1 and Pass 2 are merged via a detail cache.

Note: Lever uses 'team' for what is effectively department.
"""

import re
from typing import List, Dict
from bs4 import BeautifulSoup
from app.base import BaseExtractor
from app.models import JobListing, JobDetail
from app.utils import clean_html


class LeverExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    def _extract_slug(self, url: str) -> str:
        match = re.search(r"jobs\.lever\.co/([^/?#]+)", url)
        if match:
            return match.group(1)
        raise ValueError(f"Could not extract Lever slug from URL: {url}")

    async def get_listings(self, source_url: str) -> List[JobListing]:
        slug    = self._extract_slug(source_url)
        api_url = f"https://api.lever.co/v0/postings/{slug}"

        data = await self.fetch_json(api_url, params={"mode": "json"})
        if not data or not isinstance(data, list):
            return []

        listings = []
        for job in data:
            title = job.get("text", "").strip()
            url   = job.get("hostedUrl", "").strip()

            if not title or not url:
                continue

            detail = self._parse_detail(job)
            self._detail_cache[url] = detail
            listings.append(JobListing(title=title, url=url))

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """Return cached detail from listing API — no separate fetch needed."""
        if listing.url in self._detail_cache:
            return self._detail_cache[listing.url]
        return JobDetail(title=listing.title, url=listing.url)

    def _parse_detail(self, job: dict) -> JobDetail:
        """Parse a Lever posting object into a JobDetail."""
        description_parts = []
        requirements      = []

        for section in job.get("lists", []):
            name    = section.get("text", "").lower()
            content = section.get("content", "")
            items   = BeautifulSoup(content, "html.parser").get_text(
                separator="\n", strip=True
            )
            if any(kw in name for kw in ["requirement", "qualif", "what you'll need", "what we're looking for"]):
                requirements = [line.strip() for line in items.split("\n") if line.strip()]
            else:
                description_parts.append(f"{section.get('text', '')}\n{items}")

        raw_desc = job.get("descriptionPlain", "") or job.get("description", "") or ""
        raw_desc = clean_html(raw_desc)
        if raw_desc:
            description_parts.insert(0, raw_desc)

        description = "\n\n".join(description_parts).strip() or None

        categories  = job.get("categories", {})
        location    = categories.get("location")
        team        = categories.get("team")
        departments = [team] if team else []
        job_type    = categories.get("commitment")

        return JobDetail(
            title=job.get("text", "").strip(),
            url=job.get("hostedUrl", "").strip(),
            location=location,
            job_type=job_type,
            departments=departments,
            description=description,
            requirements=requirements,
        )