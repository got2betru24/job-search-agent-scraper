"""
Netflix extractor — Eightfold AI career platform.

Netflix hosts its job board at explore.jobs.netflix.net, powered by Eightfold AI.
Eightfold serves job listings via a public query API endpoint that returns paginated
JSON. No auth token is required for public job listings.

API endpoint:
  GET https://explore.jobs.netflix.net/api/apply/v2/jobs
  Query params:
    domain=netflix.com     (required — identifies the tenant)
    location=<str>         (optional; e.g. "Remote")
    num=<int>              (page size; max 100)
    start=<int>            (0-based offset for pagination)

Response JSON shape:
  {
    "count": <int>,
    "positions": [
      {
        "id": <int>,
        "name": <str>,                    // job title
        "posting_name": <str>,            // alternate title (usually same)
        "location": <str>,                // primary location string
        "locations": [<str>],             // all location strings
        "department": <str>,
        "t_create": <epoch_int>,          // posted date (Unix seconds)
        "ats_job_id": <str>,              // e.g. "JR38859"
        "work_location_option": <str>,    // "remote" | "onsite" | "hybrid"
        "canonicalPositionUrl": <str>,    // full URL to job detail page
        "job_description": <str>,         // usually empty in listing; full in detail
        ...
      }
    ]
  }

Pass 1 — get_listings():
  Fetches paginated listings from the API. Returns JobListing(title, url) for each.

Pass 2 — get_detail():
  Fetches the canonicalPositionUrl HTML page and extracts the full description via:
    1. Embedded <script type="application/ld+json"> JobPosting block (preferred)
    2. BeautifulSoup visible-text fallback (results <500 chars treated as JS shell)

Source URL format (stored in DB):
  https://explore.jobs.netflix.net/careers?domain=netflix.com&location=Remote

The extractor reads the `location` param from the source URL and passes it to the
API. If absent, all jobs are fetched unfiltered by location.
"""

import json
import re
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

from app.base import BaseExtractor
from app.models import JobDetail, JobListing
from app.utils import clean_html, extract_salary

PAGE_SIZE = 100
BASE_API = "https://explore.jobs.netflix.net/api/apply/v2/jobs"
CAREERS_BASE = "https://explore.jobs.netflix.net"


class NetflixExtractor(BaseExtractor):
    """Extractor for Netflix jobs via the Eightfold AI public listing API."""

    # ------------------------------------------------------------------ #
    # Pass 1 — fetch job listings                                         #
    # ------------------------------------------------------------------ #

    async def get_listings(self, source_url: str) -> List[JobListing]:
        parsed = urlparse(source_url)
        qs = parse_qs(parsed.query)

        location_filter: Optional[str] = qs["location"][0] if "location" in qs else None

        listings: List[JobListing] = []
        start = 0

        while True:
            params: dict = {
                "domain": "netflix.com",
                "num": PAGE_SIZE,
                "start": start,
            }
            if location_filter:
                params["location"] = location_filter

            data = await self.fetch_json(BASE_API, params=params)

            positions: List[dict] = data.get("positions") or []
            if not positions:
                break

            for pos in positions:
                listing = self._parse_listing(pos)
                if listing:
                    listings.append(listing)

            total: int = data.get("count") or 0
            start += len(positions)
            if start >= total or len(positions) < PAGE_SIZE:
                break

        return listings

    def _parse_listing(self, pos: dict) -> Optional[JobListing]:
        """Convert a raw Eightfold position object into a JobListing."""
        title: str = (pos.get("name") or pos.get("posting_name") or "").strip()
        if not title:
            return None

        # Canonical job URL — prefer dedicated field, fall back to id-based URL
        canonical: str = (pos.get("canonicalPositionUrl") or "").strip()
        if not canonical:
            job_id = pos.get("id") or pos.get("ats_job_id")
            if job_id:
                canonical = f"{CAREERS_BASE}/careers/job/{job_id}"
        if not canonical:
            return None

        return JobListing(title=title, url=canonical)

    # ------------------------------------------------------------------ #
    # Pass 2 — fetch full description from canonical job detail page      #
    # ------------------------------------------------------------------ #

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Fetch the Eightfold job detail page and extract the full description.

        Tries JSON-LD first, falls back to BeautifulSoup text extraction.
        Returns a minimal JobDetail (no description) if the page is a
        JS-rendered shell or the fetch fails — the runner will mark it 'failed'.
        """
        try:
            html = await self.fetch(listing.url)
        except Exception:
            return JobDetail(title=listing.title, url=listing.url)

        # --- Attempt 1: JSON-LD JobPosting block ---
        ld_match = re.search(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        if ld_match:
            try:
                ld = json.loads(ld_match.group(1))
                if isinstance(ld, dict) and "@graph" in ld:
                    for item in ld["@graph"]:
                        if isinstance(item, dict) and item.get("@type") == "JobPosting":
                            ld = item
                            break
                if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                    return self._detail_from_ld(listing, ld)
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # --- Attempt 2: BeautifulSoup visible-text fallback ---
        description = self._extract_page_description(html)
        if not description or len(description) < 500:
            # JS-rendered shell — no usable content
            return JobDetail(title=listing.title, url=listing.url)

        description = description.strip()
        return JobDetail(
            title=listing.title,
            url=listing.url,
            description=description,
            salary=extract_salary(description),
        )

    def _detail_from_ld(self, listing: JobListing, ld: dict) -> JobDetail:
        """Build a JobDetail from a JSON-LD JobPosting block."""
        raw_desc = ld.get("description") or ""
        description = (clean_html(raw_desc).strip() or None) if raw_desc else None

        # Salary — structured baseSalary first, regex fallback on description
        salary: Optional[str] = None
        base_salary = ld.get("baseSalary")
        if isinstance(base_salary, dict):
            value = base_salary.get("value") or {}
            if isinstance(value, dict):
                mn = value.get("minValue")
                mx = value.get("maxValue")
                currency = base_salary.get("currency", "USD")
                unit = value.get("unitText", "")
                if mn and mx:
                    salary = f"{currency} {int(mn):,}–{int(mx):,}"
                    if unit:
                        salary += f" / {unit.lower()}"
                elif mn:
                    salary = f"{currency} {int(mn):,}+"
        if salary is None and description:
            salary = extract_salary(description)

        # Location — jobLocation address + TELECOMMUTE flag
        location: Optional[str] = None
        job_location = ld.get("jobLocation")
        if isinstance(job_location, dict):
            addr = job_location.get("address") or {}
            locality = (addr.get("addressLocality") or "").strip()
            region = (addr.get("addressRegion") or "").strip()
            parts = [p for p in [locality, region] if p]
            location = ", ".join(parts) or None
        if (ld.get("jobLocationType") or "").upper() == "TELECOMMUTE":
            location = f"{location} | Remote, US" if location else "Remote, US"

        return JobDetail(
            title=listing.title,
            url=listing.url,
            location=location,
            job_type=ld.get("employmentType"),
            salary=salary,
            description=description,
        )

    def _extract_page_description(self, html: str) -> Optional[str]:
        """
        Fallback: extract visible description content from the Eightfold page HTML.
        Strips boilerplate elements and returns clean Markdown via clean_html().
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()

        content = (
            soup.find("div", class_=re.compile(r"position[-_]detail|job[-_]description|content[-_]area", re.I))
            or soup.find("main")
            or soup.find("article")
            or soup.body
        )

        return clean_html(str(content)) if content else None