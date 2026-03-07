"""
Workday Extractor
-----------------
Workday does not expose a public API, but uses an internal CXS API
that can be called directly without JS rendering.

Listing API endpoint pattern:
  POST https://{company}.wd{N}.myworkdayjobs.com/wday/cxs/{company}/{board}/jobs

URL facet parameters (locations, timeType, jobFamilyGroup, etc.) are passed
directly as appliedFacets in the POST body — enabling pre-filtered results
that match what the user sees in their bookmarked URL.

Detail: human-facing HTML page — {base_url}/en-US/{board}/job/{slug}_{id}
  Pass 1 returns title + location from the CXS listing API (no description).
  Pass 2 fetches the HTML job page and parses the embedded JSON-LD for full description.

Pagination uses limit/offset with a default page size of 20.

Multi-location jobs:
  When locationsText is "N Locations", Workday bakes one city into the externalPath
  but the job actually exists at multiple locations. We resolve this by making a
  secondary CXS call (searchText=jobId, no facets) and reading the locations facet
  values. Any location matching TARGET_LOCATIONS gets its own JobListing with a
  reconstructed URL using the matched city slug.
"""

import json
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs
from app.base import BaseExtractor
from app.models import JobListing, JobDetail
from app.utils import clean_html, get_target_locations, location_is_targeted


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
    "remoteType"
}

PAGE_SIZE = 20


class WorkdayExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}
        self._board_cache: Dict[str, str] = {}  # url -> board name
        self._api_url_cache: Dict[str, str] = {}  # url -> CXS api_url (for multi-loc lookups)

    def _parse_url(self, source_url: str) -> tuple[str, str, str, dict]:
        """
        Parse a Workday URL into its components.

        Returns:
            base_url  — https://{company}.wd{N}.myworkdayjobs.com
            company   — e.g. 'zillow'
            board     — e.g. 'Zillow_Group_External'
            facets    — dict of appliedFacets extracted from query params
        """
        parsed = urlparse(source_url)
        host   = parsed.netloc

        # Pattern 1: {company}.wd{N}.myworkdayjobs.com (standard)
        match = re.match(r"([^.]+)\.(wd\d+)\.myworkday(?:jobs|site)\.com", host)

        if match:
            company  = match.group(1)
            base_url = f"https://{host}"
            path_parts = [p for p in parsed.path.strip("/").split("/") if p and p not in ("en-US", "recruiting")]
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

    def _parse_job_url(self, job_url: str) -> Optional[tuple[str, str, str, str]]:
        """
        Parse a Workday job detail URL into API components.

        Handles both URL patterns:
          Standard:     {company}.wd{N}.myworkdayjobs.com/.../{board}/job/{jobId}
          Fidelity:     wd{N}.myworkdaysite.com/recruiting/{company}/{board}/job/{jobId}

        Returns:
            (base_url, company, board, job_path) or None if unparseable
        """
        parsed     = urlparse(job_url)
        host       = parsed.netloc
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]

        # Find the 'job' segment index — board is immediately before it
        try:
            job_idx  = path_parts.index("job")
            job_path = "/" + "/".join(path_parts[job_idx:])
        except ValueError:
            return None

        # Pattern 1: standard
        match = re.match(r"([^.]+)\.(wd\d+)\.myworkday(?:jobs|site)\.com", host)
        if match:
            company = match.group(1)
            board   = path_parts[job_idx - 1] if job_idx > 0 else company
            return f"https://{host}", company, board, job_path

        # Pattern 2: Fidelity-style
        match2 = re.match(r"(wd\d+)\.myworkday(?:jobs|site)\.com", host)
        if match2:
            # path: recruiting/{company}/{board}/job/{jobId}
            clean_parts = [p for p in path_parts if p not in ("recruiting", "en-US")]
            if len(clean_parts) >= 2:
                company = clean_parts[0]
                board   = clean_parts[1]
                return f"https://{host}", company, board, job_path

        return None

    def _extract_job_id(self, external_path: str) -> Optional[str]:
        """
        Extract the Workday job ID from an externalPath.
        e.g. /job/San-Jose/Principal-Product-Manager_R164308 -> R164308
        """
        match = re.search(r"_([A-Z0-9]+)$", external_path.rstrip("/"))
        return match.group(1) if match else None

    def _slugify_location(self, descriptor: str) -> str:
        """
        Convert a Workday location descriptor to a URL slug.
        e.g. "San Jose" -> "San-Jose", "Lehi" -> "Lehi"
        """
        return re.sub(r"\s+", "-", descriptor.strip())

    def _reconstruct_url(self, original_path: str, job_url_prefix: str, new_city_slug: str) -> str:
        """
        Replace the city slug in a Workday externalPath with a new one.
        e.g. /job/San-Jose/Principal-Product-Manager_R164308
          -> {prefix}/job/Lehi/Principal-Product-Manager_R164308
        """
        new_path = re.sub(
            r"^(/job/)[^/]+(/)",
            rf"\g<1>{new_city_slug}\2",
            original_path
        )
        return f"{job_url_prefix}{new_path}"

    async def _resolve_multi_location_urls(
        self,
        api_url: str,
        job_id: str,
        external_path: str,
        job_url_prefix: str,
        target_locations: Optional[List[str]],
    ) -> List[tuple[str, str]]:
        """
        For a multi-location job, fetch all location variants via a secondary
        CXS call (searchText=jobId, no facets) and return (url, location_descriptor)
        tuples for any location that matches target_locations.

        Falls back to an empty list on any error — caller will use original URL.
        """
        try:
            payload = {
                "appliedFacets": {},
                "limit": 20,
                "offset": 0,
                "searchText": job_id,
            }
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            data = await self.fetch_json(api_url, method="POST", json=payload, headers=headers)
            if not data:
                return []

            # Walk the nested facets to find the locations facet
            locations_values = []
            for facet in data.get("facets", []):
                # Top-level locations facet
                if facet.get("facetParameter") == "locations":
                    locations_values = facet.get("values", [])
                    break
                # Nested under locationMainGroup
                for nested in facet.get("values", []):
                    if isinstance(nested, dict) and nested.get("facetParameter") == "locations":
                        locations_values = nested.get("values", [])
                        break
                if locations_values:
                    break

            if not locations_values:
                return []

            results = []
            for loc in locations_values:
                descriptor = loc.get("descriptor", "")
                if not descriptor:
                    continue
                if location_is_targeted(descriptor, target_locations):
                    slug = self._slugify_location(descriptor)
                    url  = self._reconstruct_url(external_path, job_url_prefix, slug)
                    results.append((url, descriptor))

            return results

        except Exception:
            return []

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

        parsed_source  = urlparse(source_url)
        is_site_domain = "myworkdaysite.com" in parsed_source.netloc and not re.match(r"[^.]+\.wd\d+\.myworkdaysite", parsed_source.netloc)
        if is_site_domain:
            job_url_prefix = f"{base_url}/recruiting/{company}/{board}"
        else:
            job_url_prefix = f"{base_url}/en-US/{board}"

        target_locations = get_target_locations()

        listings = []
        offset   = 0
        resolved_job_ids: set[str] = set()  # job IDs already resolved via secondary CXS call

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

                loc_text = job.get("locationsText", "")
                is_multi = bool(re.match(r"^\d+ Locations?$", loc_text, re.I))

                if is_multi:
                    # Resolve all targeted location variants via secondary CXS call.
                    # Deduplicate — same job ID may appear on multiple pages.
                    job_id = self._extract_job_id(external_path)
                    if job_id and job_id not in resolved_job_ids:
                        variants = await self._resolve_multi_location_urls(
                            api_url, job_id, external_path, job_url_prefix, target_locations
                        )
                        resolved_job_ids.add(job_id)
                        for url, descriptor in variants:
                            detail = JobDetail(title=title, url=url, location=descriptor)
                            self._detail_cache[url] = detail
                            self._board_cache[url]  = board
                            self._api_url_cache[url] = api_url
                            listings.append(JobListing(title=title, url=url))
                        if variants:
                            continue
                    elif job_id in resolved_job_ids:
                        # Already resolved this job on a previous page — skip entirely
                        continue
                    # Fall through if resolution failed — use original URL with None location
                    location = None
                else:
                    location = loc_text or None

                url = f"{job_url_prefix}{external_path}"
                detail = JobDetail(title=title, url=url, location=location)
                self._detail_cache[url] = detail
                self._board_cache[url]  = board
                self._api_url_cache[url] = api_url
                listings.append(JobListing(title=title, url=url))

            offset += PAGE_SIZE
            if offset >= total or not postings:
                break

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Fetch full job detail from the Workday CXS detail API.
        Falls back to cached listing data (title + location) if fetch fails.
        """
        detail = await self._fetch_detail_page(listing)
        if detail:
            return detail

        # Fall back to cached listing data (no description, but location preserved)
        return self._detail_cache.get(
            listing.url,
            JobDetail(title=listing.title, url=listing.url)
        )

    async def _fetch_detail_page(self, listing: JobListing) -> Optional[JobDetail]:
        """
        Fetch job detail by scraping the human-facing Workday job page and
        parsing the JSON-LD block embedded for SEO.

        URL pattern: {base_url}/en-US/{board}/job/{slug}_{id}
        Constructed from base_url + board (stored during get_listings) +
        the /job/... path extracted from the listing URL.
        """
        parsed = self._parse_job_url(listing.url)
        if not parsed:
            return None

        base_url, company, board_from_url, job_path = parsed

        # Prefer board stored during get_listings — more reliable than URL parse
        board = self._board_cache.get(listing.url, board_from_url)

        html_url = f"{base_url}/en-US/{board}{job_path}"

        try:
            html = await self.fetch(html_url)
        except Exception:
            return None

        if not html:
            return None

        # Parse JSON-LD block embedded by Workday for SEO
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

        description = clean_html(data.get("description", "") or "")

        # Location: prefer the cached descriptor from Pass 1 (already resolved
        # to a targeted city for multi-location jobs). Fall back to JSON-LD.
        cached   = self._detail_cache.get(listing.url)
        location = cached.location if cached and cached.location else None

        if not location:
            loc = data.get("jobLocation", {})
            if isinstance(loc, dict):
                addr     = loc.get("address", {})
                locality = addr.get("addressLocality", "")
                location = locality or None
            elif isinstance(loc, list):
                # Multiple jobLocation entries — concatenate all localities
                localities = []
                for entry in loc:
                    if isinstance(entry, dict):
                        locality = entry.get("address", {}).get("addressLocality", "")
                        if locality:
                            localities.append(locality)
                location = " | ".join(localities) or None

        if data.get("jobLocationType") == "TELECOMMUTE":
            location = f"{location} | Remote, US" if location else "Remote, US"

        # Salary from JSON-LD baseSalary (present when company chooses to disclose)
        salary = None
        comp = data.get("baseSalary", {})
        if isinstance(comp, dict):
            val      = comp.get("value", {})
            min_v    = val.get("minValue")
            max_v    = val.get("maxValue")
            currency = comp.get("currency", "USD")
            unit     = val.get("unitText", "")
            if min_v and max_v:
                salary = f"{currency} {int(min_v):,}–{int(max_v):,}"
                if unit:
                    salary += f" / {unit.lower()}"

        return JobDetail(
            title=listing.title,
            url=listing.url,
            location=location,
            job_type=data.get("employmentType"),
            salary=salary,
            description=description.strip() or None,
        )