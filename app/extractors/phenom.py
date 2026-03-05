"""
Phenom People Extractor
-----------------------
Phenom People (used by Adobe, Autodesk, and others) pre-loads job data
as a JSON blob inside a <script> tag in the page source for SEO purposes.

The data lives in window.phApp.ddo.eagerLoadRefineSearch.data.jobs

Since all job data is available in the listing page HTML, Pass 1 and
Pass 2 are merged — get_listings() extracts everything and caches it.
"""

import re
import json
from typing import List, Dict
from bs4 import BeautifulSoup
from app.base import BaseExtractor
from app.models import JobListing, JobDetail


class PhenomExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    async def get_listings(self, source_url: str) -> List[JobListing]:
        listings = []
        page_size = 10
        max_pages = 20  # safety cap — 200 jobs max per source
        seen_urls = set()

        for page in range(max_pages):
            # Append pagination param — preserve existing query params
            from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
            parsed = urlparse(source_url)
            params = parse_qs(parsed.query)
            params["from"] = [str(page * page_size)]
            paginated_url = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

            html = await self.fetch(paginated_url)
            if not html:
                break

            jobs = self._extract_jobs_from_script(html)
            if not jobs:
                break

            new_jobs = 0
            for job in jobs:
                title = job.get("title", "").strip()
                url   = (
                    job.get("applyUrl")
                    or job.get("absoluteUrl")
                    or self._build_url(source_url, job)
                )

                if not title or not url or url in seen_urls:
                    continue

                seen_urls.add(url)
                detail = self._parse_detail(job, url)
                self._detail_cache[url] = detail
                listings.append(JobListing(title=title, url=url))
                new_jobs += 1

            # If we got fewer than page_size new jobs, we've hit the last page
            if len(jobs) < page_size or new_jobs == 0:
                break

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        if listing.url in self._detail_cache:
            return self._detail_cache[listing.url]
        return JobDetail(title=listing.title, url=listing.url)

    def _extract_jobs_from_script(self, html: str) -> list:
        """
        Extract jobs array from the phApp.ddo script tag.
        Uses json.JSONDecoder to robustly extract the nested object
        without relying on fragile regex boundaries.
        """
        soup = BeautifulSoup(html, "html.parser")

        for script in soup.find_all("script", {"type": "text/javascript"}):
            text = script.string or ""
            if "eagerLoadRefineSearch" not in text:
                continue

            # Find the position of eagerLoadRefineSearch in the script
            idx = text.find('"eagerLoadRefineSearch"')
            if idx == -1:
                continue

            # Move past the key and colon to the start of the object {
            start = text.find("{", idx)
            if start == -1:
                continue

            # Use JSONDecoder to extract exactly one valid JSON object
            # starting at this position — handles nested braces correctly
            decoder = json.JSONDecoder()
            try:
                obj, _ = decoder.raw_decode(text, start)
                jobs = obj.get("data", {}).get("jobs", [])
                if jobs:
                    return jobs
            except json.JSONDecodeError:
                continue

        return []

    def _build_url(self, source_url: str, job: dict) -> str:
        """Construct a job URL from jobSeqNo if no direct URL available."""
        from urllib.parse import urlparse
        seq = job.get("jobSeqNo") or job.get("jobId")
        if seq:
            base = urlparse(source_url)
            return f"{base.scheme}://{base.netloc}/job/{seq}"
        return ""

    def _parse_detail(self, job: dict, url: str) -> JobDetail:
        description = (
            job.get("descriptionTeaser")
            or job.get("ml_job_parser", {}).get("descriptionTeaser_first200")
            or ""
        )

        location_parts = job.get("multi_location") or []
        location = location_parts[0] if location_parts else job.get("cityState")

        return JobDetail(
            title=job.get("title", "").strip(),
            url=url,
            location=location,
            job_type=job.get("type"),
            description=description[:5000] if description else None,
        )