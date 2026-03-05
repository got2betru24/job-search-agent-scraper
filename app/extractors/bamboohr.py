"""
BambooHR Extractor
------------------
BambooHR job boards have a consistent JSON API endpoint.

API endpoint:
  https://{company}.bamboohr.com/careers/list

Company subdomain is extracted from the source URL:
  https://{company}.bamboohr.com/careers
"""

import re
from typing import List
from bs4 import BeautifulSoup
from app.base import BaseExtractor
from app.models import JobListing, JobDetail


class BambooHRExtractor(BaseExtractor):

    def _extract_subdomain(self, url: str) -> str:
        match = re.search(r"([^/.]+)\.bamboohr\.com", url)
        if match:
            return match.group(1)
        raise ValueError(f"Could not extract BambooHR subdomain from URL: {url}")

    async def get_listings(self, source_url: str) -> List[JobListing]:
        subdomain = self._extract_subdomain(source_url)
        api_url   = f"https://{subdomain}.bamboohr.com/careers/list"

        data = await self.fetch_json(api_url)

        if not data or "result" not in data:
            return []

        listings = []
        for job in data["result"]:
            title  = job.get("jobOpeningName", "").strip()
            job_id = job.get("id")

            if not title or not job_id:
                continue

            url = f"https://{subdomain}.bamboohr.com/careers/{job_id}"
            listings.append(JobListing(title=title, url=url))

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Fetch individual job detail.
        BambooHR job URLs: https://{subdomain}.bamboohr.com/careers/{id}
        """
        match = re.search(r"([^/.]+)\.bamboohr\.com/careers/(\d+)", listing.url)
        if not match:
            return JobDetail(title=listing.title, url=listing.url)

        subdomain = match.group(1)
        job_id    = match.group(2)

        data = await self.fetch_json(
            f"https://{subdomain}.bamboohr.com/careers/{job_id}/detail"
        )

        if not data:
            return JobDetail(title=listing.title, url=listing.url)

        return self._parse_detail(data, listing.url)

    def _parse_detail(self, job: dict, url: str) -> JobDetail:
        description = ""
        raw = job.get("description") or ""
        if raw:
            description = BeautifulSoup(raw, "html.parser").get_text(
                separator="\n", strip=True
            )

        location = job.get("location", {})
        if isinstance(location, dict):
            city  = location.get("city", "")
            state = location.get("state", "")
            location = f"{city}, {state}".strip(", ") or None
        elif isinstance(location, str):
            location = location or None

        return JobDetail(
            title=job.get("jobOpeningName", listing_title := job.get("title", "")).strip(),
            url=url,
            location=location,
            job_type=job.get("employmentType"),
            description=description[:5000] if description else None,
        )
