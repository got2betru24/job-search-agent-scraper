"""
Ashby Extractor
---------------
Ashby exposes a public job board API — no scraping needed.

API endpoint:
  https://api.ashbyhq.com/posting-api/job-board/{company}?includeCompensation=true

Company slug is extracted from the source URL:
  https://jobs.ashbyhq.com/{company}

The listing API returns full job data including description, location, department,
and salary — so Pass 1 and Pass 2 are merged. The separate /posting/{id} endpoint
requires authentication on some boards so we avoid it entirely.
"""

import re
from typing import List, Dict
from app.base import BaseExtractor
from app.models import JobListing, JobDetail
from app.utils import clean_html


class AshbyExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    def _extract_slug(self, url: str) -> str:
        match = re.search(r"jobs\.ashbyhq\.com/([^/?#]+)", url)
        if match:
            return match.group(1)
        raise ValueError(f"Could not extract Ashby slug from URL: {url}")

    async def get_listings(self, source_url: str) -> List[JobListing]:
        slug    = self._extract_slug(source_url)
        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

        data = await self.fetch_json(api_url, params={"includeCompensation": "true"})
        if not data:
            return []

        job_list = data.get("jobPostings") or data.get("jobs") or []

        listings = []
        for job in job_list:
            if not job.get("isListed", True):
                continue

            title = job.get("title", "").strip()
            url   = job.get("jobUrl", "").strip()

            if not title or not url:
                continue

            detail = self._parse_detail(job, url)
            self._detail_cache[url] = detail
            listings.append(JobListing(title=title, url=url))

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """Return cached detail from listing API — no separate fetch needed."""
        if listing.url in self._detail_cache:
            return self._detail_cache[listing.url]
        return JobDetail(title=listing.title, url=listing.url)

    def _parse_detail(self, job: dict, url: str) -> JobDetail:
        description = clean_html(job.get("descriptionHtml") or job.get("description") or "")

        # Location — handle both API versions
        location_parts = []

        loc = job.get("locationName") or job.get("location")
        if isinstance(loc, list):
            location_parts.extend([l.strip() for l in loc if l])
        elif isinstance(loc, str) and loc:
            location_parts.append(loc.strip())

        for sec in job.get("secondaryLocations", []):
            name = sec.get("location", "").strip()
            if name and name not in location_parts:
                location_parts.append(name)

        for other in job.get("otherLocations", []):
            name = other.get("name", "").strip() if isinstance(other, dict) else str(other)
            if name and name not in location_parts:
                location_parts.append(name)

        location = " | ".join(location_parts) if location_parts else None

        # Department
        dept        = job.get("department")
        departments = []
        if isinstance(dept, str) and dept:
            departments = [dept]
        elif isinstance(dept, dict):
            name = dept.get("name")
            if name:
                departments = [name]

        # Salary
        salary = None
        comp   = job.get("compensation", {})
        if comp:
            min_v    = comp.get("minValue")
            max_v    = comp.get("maxValue")
            currency = comp.get("currencyCode", "USD")
            interval = comp.get("interval", "")
            if min_v and max_v:
                salary = f"{currency} {min_v:,}–{max_v:,} {interval}".strip()

        return JobDetail(
            title=job.get("title", "").strip(),
            url=url,
            location=location,
            departments=departments,
            job_type=job.get("employmentType"),
            salary=salary,
            description=description,
        )