"""
Workday Extractor
-----------------
Workday does not expose a public API, but uses an internal CXS API
that can be called directly without JS rendering.

API endpoint pattern:
  POST https://{company}.wd{N}.myworkdayjobs.com/wday/cxs/{company}/{board}/jobs

URL facet parameters (locations, timeType, jobFamilyGroup, etc.) are passed
directly as appliedFacets in the POST body — enabling pre-filtered results
that match what the user sees in their bookmarked URL.

Pagination uses limit/offset with a default page size of 20.
"""

import json
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs
from app.base import BaseExtractor
from app.models import JobListing, JobDetail


# Workday facet param names that map directly to appliedFacets
KNOWN_FACETS = {
    "locations",
    "timeType",
    "workerSubType",
    "jobFamilyGroup",
    "locationHierarchy1",
    "locationHierarchy2",
    "locationRegionStateProvince",
    "locationCountry",
    "jobFamily",
    "departments",
    "category",
}

PAGE_SIZE = 20


class WorkdayExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    def _parse_url(self, source_url: str) -> tuple[str, str, str, dict]:
        """
        Parse a Workday URL into its components.

        Returns:
            base_url  — https://{company}.wd{N}.myworkdayjobs.com
            company   — e.g. 'zillow'
            board     — e.g. 'Zillow_Group_External'
            facets    — dict of appliedFacets extracted from query params
        """
        parsed  = urlparse(source_url)
        host    = parsed.netloc  # zillow.wd5.myworkdayjobs.com

        # Extract company and board
        # Pattern 1: {company}.wd{N}.myworkdayjobs.com (standard)
        match = re.match(r"([^.]+)\.(wd\d+)\.myworkday(?:jobs|site)\.com", host)

        if match:
            company  = match.group(1)
            base_url = f"https://{host}"
            # Board name — strip known prefixes (/en-US/, /recruiting/{company}/)
            path_parts = [p for p in parsed.path.strip("/").split("/") if p and p not in ("en-US", "recruiting")]
            # Skip the company slug if it appears as the first path segment after /recruiting/
            if path_parts and path_parts[0].lower() == company.lower():
                path_parts = path_parts[1:]
            board = path_parts[0] if path_parts else company
        else:
            # Pattern 2: wd{N}.myworkdaysite.com/recruiting/{company}/{board} (Fidelity-style)
            match2 = re.match(r"(wd\d+)\.myworkday(?:jobs|site)\.com", host)
            if not match2:
                raise ValueError(f"Could not parse Workday URL: {source_url}")
            base_url   = f"https://{host}"
            path_parts = [p for p in parsed.path.strip("/").split("/") if p and p not in ("en-US", "recruiting")]
            # path_parts[0] = company slug, path_parts[1] = board
            if len(path_parts) >= 2:
                company = path_parts[0]
                board   = path_parts[1]
            elif len(path_parts) == 1:
                company = path_parts[0]
                board   = path_parts[0]
            else:
                raise ValueError(f"Could not parse Workday URL: {source_url}")

        # Extract facets from query params
        params = parse_qs(parsed.query)
        facets = {}
        for key, values in params.items():
            if key in KNOWN_FACETS:
                facets[key] = values  # keep as list — Workday expects arrays

        return base_url, company, board, facets

    async def get_listings(self, source_url: str) -> List[JobListing]:
        try:
            base_url, company, board, facets = self._parse_url(source_url)
        except ValueError as e:
            raise ValueError(str(e))

        api_url = f"{base_url}/wday/cxs/{company}/{board}/jobs"
        headers = {
            "Accept":       "application/json",
            "Content-Type": "application/json",
        }

        listings = []
        offset   = 0

        while True:
            payload = {
                "appliedFacets": facets,
                "limit":         PAGE_SIZE,
                "offset":        offset,
                "searchText":    "",
            }

            data = await self.fetch_json(api_url, method="POST", json=payload, headers=headers)
            if not data:
                break

            postings = data.get("jobPostings", [])
            total    = data.get("total", 0)

            for job in postings:
                title         = job.get("title", "").strip()
                external_path = job.get("externalPath", "").strip()

                if not title or not external_path:
                    continue

                url = f"{base_url}{external_path}"

                # Build detail from listing data
                # "2 Locations" / "3 Locations" means multiple locations —
                # treat as None so location_is_targeted() lets it through
                loc_text = job.get("locationsText", "")
                location = None if re.match(r"^\d+ Locations?$", loc_text, re.I) else loc_text or None

                detail = JobDetail(
                    title=title,
                    url=url,
                    location=location,
                )
                self._detail_cache[url] = detail
                listings.append(JobListing(title=title, url=url))

            offset += PAGE_SIZE
            if offset >= total or not postings:
                break

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Fetch full job detail from the Workday job page JSON-LD.
        Falls back to cached listing data if detail fetch fails.
        """
        if listing.url in self._detail_cache:
            cached = self._detail_cache[listing.url]
            # If we already have a full description, return it
            if cached.description:
                return cached

        # Try fetching the detail page for full description
        try:
            detail = await self._fetch_detail_page(listing)
            if detail:
                return detail
        except Exception:
            pass

        # Fall back to cached listing data
        return self._detail_cache.get(
            listing.url,
            JobDetail(title=listing.title, url=listing.url)
        )

    async def _fetch_detail_page(self, listing: JobListing) -> Optional[JobDetail]:
        """Try to extract job detail from the Workday job detail API."""
        # Workday detail API pattern:
        # POST https://{company}.wd{N}.myworkdayjobs.com/wday/cxs/{company}/{board}/jobs/{path}
        parsed  = urlparse(listing.url)
        host    = parsed.netloc
        match   = re.match(r"([^.]+)\.(wd\d+)\.myworkday(?:jobs|site)\.com", host)
        if not match:
            return None

        company    = match.group(1)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]

        # Find board — it's the segment before /job/
        try:
            job_idx = path_parts.index("job")
            board   = path_parts[job_idx - 1] if job_idx > 0 else company
            job_path = "/" + "/".join(path_parts[job_idx:])
        except ValueError:
            return None

        api_url = f"https://{host}/wday/cxs/{company}/{board}/jobs{job_path}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        data = await self.fetch_json(api_url, method="GET", headers=headers)
        if not data:
            return None

        # Workday detail response wraps job in jobPostingInfo
        job = data.get("jobPostingInfo", data)
        if not job:
            return None

        description = job.get("jobDescription", "") or job.get("description", "")
        location    = job.get("location") or self._detail_cache.get(listing.url, JobDetail(title="", url="")).location

        return JobDetail(
            title=listing.title,
            url=listing.url,
            location=location,
            job_type=job.get("timeType") or job.get("employmentType"),
            description=description[:5000] if description else None,
        )

    def _parse_jsonld(self, data: dict, url: str) -> JobDetail:
        """Parse schema.org JobPosting JSON-LD (fallback)."""
        from bs4 import BeautifulSoup

        description = ""
        raw = data.get("description", "")
        if raw:
            description = BeautifulSoup(raw, "html.parser").get_text(
                separator="\n", strip=True
            )

        location = None
        loc = data.get("jobLocation", {})
        if isinstance(loc, dict):
            addr    = loc.get("address", {})
            city    = addr.get("addressLocality", "")
            state   = addr.get("addressRegion", "")
            country = addr.get("addressCountry", "")
            parts   = [p for p in [city, state, country] if p]
            location = ", ".join(parts) or None

        salary = None
        comp = data.get("baseSalary", {})
        if comp:
            val      = comp.get("value", {})
            min_v    = val.get("minValue")
            max_v    = val.get("maxValue")
            currency = comp.get("currency", "USD")
            if min_v and max_v:
                salary = f"{currency} {int(min_v):,}–{int(max_v):,}"

        return JobDetail(
            title=data.get("title", "").strip(),
            url=url,
            location=location,
            job_type=data.get("employmentType"),
            salary=salary,
            description=description[:5000] if description else None,
        )