"""
Phenom People Extractor
-----------------------
Phenom People (used by Adobe, Autodesk, and others) pre-loads job data
as a JSON blob inside a <script> tag in the page source for SEO purposes.

The data lives in window.phApp.ddo.eagerLoadRefineSearch.data.jobs

Pass 1 extracts title, location, and a short description teaser from the
listing page. Pass 2 fetches the individual job page for the full description.

Note: Phenom job URLs often point to the underlying ATS (e.g. Workday).
The detail fetch handles this transparently — it fetches the job URL and
parses either a JSON-LD block (Workday-hosted pages) or falls back to
extracting visible page text.
"""

import re
import json
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from app.base import BaseExtractor
from app.models import JobListing, JobDetail
from app.utils import clean_html


class PhenomExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    async def get_listings(self, source_url: str) -> List[JobListing]:
        listings  = []
        page_size = 10
        max_pages = 20  # safety cap — 200 jobs max per source
        seen_urls = set()

        for page in range(max_pages):
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

                # Strip /apply suffix — we want the job page, not the apply page
                url = re.sub(r"/apply$", "", url.rstrip("/"))

                seen_urls.add(url)
                detail = self._parse_listing(job, url)
                self._detail_cache[url] = detail
                listings.append(JobListing(title=title, url=url))
                new_jobs += 1

            if len(jobs) < page_size or new_jobs == 0:
                break

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Fetch full job description from the individual job page.
        Tries JSON-LD first (works for Workday-hosted pages),
        falls back to extracting visible body text.
        Falls back to cached listing data if fetch fails.
        """
        try:
            html = await self.fetch(listing.url)
        except Exception:
            return self._detail_cache.get(
                listing.url,
                JobDetail(title=listing.title, url=listing.url)
            )

        if not html:
            return self._detail_cache.get(
                listing.url,
                JobDetail(title=listing.title, url=listing.url)
            )

        # Try JSON-LD first (Workday-hosted pages embed this for SEO)
        detail = self._parse_jsonld(html, listing)
        if detail and detail.description:
            return detail

        # Fall back to full page text extraction
        description = clean_html(html)
        # Guard against JS-rendered pages that return only nav/footer text —
        # if the extracted text is suspiciously short it is not a real description
        if not description or len(description) < 500:
            description = None
        cached = self._detail_cache.get(listing.url)
        return JobDetail(
            title=listing.title,
            url=listing.url,
            location=cached.location if cached else None,
            job_type=cached.job_type if cached else None,
            description=description,
        )

    def _parse_jsonld(self, html: str, listing: JobListing) -> Optional[JobDetail]:
        """Parse JSON-LD JobPosting block embedded in the page (Workday pattern)."""
        matches = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if not matches:
            return None

        try:
            data = json.loads(matches[0])
        except (json.JSONDecodeError, IndexError):
            return None

        if data.get("@type") != "JobPosting":
            return None

        description = data.get("description", "") or ""

        # Location from JSON-LD address
        cached   = self._detail_cache.get(listing.url)
        location = None
        loc      = data.get("jobLocation", {})
        if isinstance(loc, dict):
            addr     = loc.get("address", {})
            locality = addr.get("addressLocality", "")
            parts    = [p for p in [locality] if p]
            location = ", ".join(parts) or None

        # Append remote indicator if telecommute
        if data.get("jobLocationType") == "TELECOMMUTE":
            location = f"{location} | Remote, US" if location else "Remote, US"

        if not location and cached:
            location = cached.location

        return JobDetail(
            title=listing.title,
            url=listing.url,
            location=location,
            job_type=data.get("employmentType") or (cached.job_type if cached else None),
            description=description.strip() or None,
        )

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

            idx = text.find('"eagerLoadRefineSearch"')
            if idx == -1:
                continue

            start = text.find("{", idx)
            if start == -1:
                continue

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

    def _parse_listing(self, job: dict, url: str) -> JobDetail:
        """Parse listing data into a JobDetail for Pass 1 cache."""
        description = clean_html(
            job.get("descriptionTeaser")
            or job.get("ml_job_parser", {}).get("descriptionTeaser_first200")
            or ""
        )

        # Multi-location — join all locations pipe-separated
        location_parts = job.get("multi_location") or []
        if location_parts:
            location = " | ".join(location_parts)
        else:
            location = job.get("cityState")

        return JobDetail(
            title=job.get("title", "").strip(),
            url=url,
            location=location,
            job_type=job.get("type"),
            description=description,
        )