"""
Oracle HCM Cloud Extractor
--------------------------
Oracle uses their own HCM Cloud platform for job listings, served via
careers.oracle.com (tenant: eeho.fa.us2.oraclecloud.com).

API endpoint:
  GET https://{tenant}/hcmRestApi/resources/latest/recruitingCEJobRequisitions

Key findings from JS bundle reverse-engineering (main-minimal.js):
  - siteNumber is CX_45001 (not CX_1)
  - expand param is "requisitionList" (flexFieldsFacet.values expands facets but not jobs)
  - finder format: findReqs;siteNumber=X,facetsList=A;B;C,key=val,...
  - pagination uses lowercase "limit" and "offset" as finder params
  - response total is item["TotalJobsCount"] (capital T)
  - job list is item["requisitionList"]

Encoding:
  The finder value is passed via httpx params= which handles encoding
  correctly. The finder string uses literal semicolons throughout —
  Oracle decodes once before parsing. No double-encoding needed.

Job URL pattern:
  https://careers.oracle.com/en/sites/jobsearch/jobs/preview/{Id}

Pass 1 returns title + location + ShortDescriptionStr from the listing API.
Full descriptions require a JS-rendered browser (Playwright) — not yet built.
Jobs are marked 'scraped' with the short description rather than 'pending',
since the detail page is inaccessible without Playwright.
"""

import re
from typing import List, Dict
from urllib.parse import urlparse, parse_qs, urlencode, quote

from datetime import datetime, timezone, timedelta

import httpx

from app.base import BaseExtractor
from app.models import JobListing, JobDetail
from app.utils import DEFAULT_HEADERS, clean_html


PAGE_SIZE = 24  # matches the hardcoded limit in Oracle's JS bundle
SITE_NUMBER = "CX_45001"
JOB_URL_BASE = "https://careers.oracle.com/en/sites/jobsearch/jobs/preview"


class OracleExtractor(BaseExtractor):

    def __init__(self):
        self._detail_cache: Dict[str, JobDetail] = {}

    def _parse_url(self, source_url: str) -> tuple[str, str, dict]:
        """
        Parse an Oracle careers URL into API components.

        Returns:
            api_url  — HCM REST API endpoint
            tenant   — e.g. 'eeho.fa.us2.oraclecloud.com'
            facets   — dict of facet params from query string
        """
        parsed = urlparse(source_url)
        host = parsed.netloc

        # Support both careers.oracle.com and direct tenant URLs
        if host == "careers.oracle.com":
            tenant = "eeho.fa.us2.oraclecloud.com"
        else:
            match = re.match(r"([^.]+\.fa\.[^.]+\.oraclecloud\.com)", host)
            if not match:
                raise ValueError(f"Could not parse Oracle HCM URL: {source_url}")
            tenant = match.group(1)

        api_url = f"https://{tenant}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        # parse_qs decodes percent-encoding including %22→" %7C→| +→space
        query_params = parse_qs(parsed.query)
        facets = {k: v[0] for k, v in query_params.items()}

        return api_url, tenant, facets

    def _build_finder(self, facets: dict, offset: int = 0) -> str:
        """
        Build the Oracle HCM finder param value.

        Uses literal semicolons throughout — Oracle decodes once before
        parsing, so single-encoding via httpx params= is correct.

        selectedFlexFieldsFacets must keep its surrounding double-quotes
        exactly as they appear in the source URL (parse_qs decodes %22→").
        """
        parts = [f"siteNumber={SITE_NUMBER}"]
        parts.append("facetsList=LOCATIONS;CATEGORIES;FLEX_FIELDS")

        if "selectedCategoriesFacet" in facets:
            parts.append(f"selectedCategoriesFacet={facets['selectedCategoriesFacet']}")

        if "selectedFlexFieldsFacets" in facets:
            # Keep surrounding quotes — Oracle requires them, parse_qs preserved them
            parts.append(f"selectedFlexFieldsFacets={facets['selectedFlexFieldsFacets']}")

        if "locationId" in facets:
            parts.append(f"locationId={facets['locationId']}")

        if "selectedPostingDatesFacet" in facets:
            parts.append(f"selectedPostingDatesFacet={facets['selectedPostingDatesFacet']}")
        else:
            parts.append("selectedPostingDatesFacet=14")

        parts.append(f"limit={PAGE_SIZE}")
        parts.append(f"offset={offset}")

        return f"findReqs;{','.join(parts)}"

    async def _fetch_oracle(self, api_url: str, finder: str) -> dict | None:
        """
        Fetch one page from the Oracle HCM API.

        The finder string must reach Oracle with its structural characters
        intact (literal ; , = | and quoted flex field values). httpx's
        params= encoding would encode ; and , which breaks Oracle's parser.

        Instead we URL-encode only the genuinely unsafe characters in the
        finder (spaces → %20, keeping ; , = | " as literals) and append it
        directly to the URL string. The fixed params are safe to urlencode
        normally. The request is sent via httpx.Request + client.send() to
        prevent AsyncClient from re-normalizing the URL.
        """
        fixed  = urlencode({"onlyData": "true", "expand": "requisitionList"})
        finder_encoded = quote(finder, safe=";,=|\"")
        full_url = f"{api_url}?{fixed}&finder={finder_encoded}"

        try:
            merged_headers = {
                **DEFAULT_HEADERS,
                "Accept":   "application/json",
                "Referer":  "https://careers.oracle.com/",
            }
            request = httpx.Request("GET", full_url, headers=merged_headers)
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                response = await client.send(request)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            body = e.response.text[:500]
            raise RuntimeError(
                f"Oracle API HTTP {e.response.status_code}: {body}"
            )
        except httpx.HTTPError as e:
            raise RuntimeError(f"HTTP error fetching Oracle API: {e}")

    async def get_listings(self, source_url: str) -> List[JobListing]:
        try:
            api_url, tenant, facets = self._parse_url(source_url)
        except ValueError as e:
            raise ValueError(str(e))

        cutoff   = datetime.now(timezone.utc) - timedelta(days=30)
        listings = []
        offset   = 0

        while True:
            finder = self._build_finder(facets, offset)
            data   = await self._fetch_oracle(api_url, finder)

            if not data:
                break

            items = data.get("items", [])
            if not items:
                break

            item     = items[0]
            total    = item.get("TotalJobsCount", 0)
            req_list = item.get("requisitionList", [])

            for job in req_list:
                job_id = str(job.get("Id", "")).strip()
                title  = job.get("Title", "").strip()

                if not job_id or not title:
                    continue

                url = f"{JOB_URL_BASE}/{job_id}"

                # Oracle's selectedPostingDatesFacet param is ignored by the API —
                # filter by date client-side instead.
                posted = job.get("PostedDate")
                if posted:
                    try:
                        posted_dt = datetime.fromisoformat(posted.replace("Z", "+00:00"))
                        if posted_dt.tzinfo is None:
                            posted_dt = posted_dt.replace(tzinfo=timezone.utc)
                        if posted_dt < cutoff:
                            continue
                    except ValueError:
                        pass

                location = job.get("PrimaryLocation")
                # Oracle returns bare "United States" for remote/unanchored US roles.
                # Normalize to match TARGET_LOCATIONS "remote - united states" filter.
                if location and location.strip() == "United States":
                    location = "Remote - United States"

                detail = JobDetail(
                    title=title,
                    url=url,
                    location=location,
                    description=clean_html(job.get("ShortDescriptionStr") or ""),
                )
                self._detail_cache[url] = detail
                listings.append(JobListing(title=title, url=url))

            offset += PAGE_SIZE
            if offset >= total or not req_list:
                break

        return listings

    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Return cached detail from listing API.
        Full descriptions require Playwright (JS-rendered pages) — not yet built.
        ShortDescriptionStr from Pass 1 is used as the description for now.
        """
        if listing.url in self._detail_cache:
            return self._detail_cache[listing.url]
        return JobDetail(title=listing.title, url=listing.url)