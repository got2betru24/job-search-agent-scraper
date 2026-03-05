from abc import ABC, abstractmethod
from typing import List, Optional
from app.models import JobListing, JobDetail
import httpx
from app.utils import DEFAULT_HEADERS


class BaseExtractor(ABC):
    """
    Abstract base class for all job extractors.

    Each extractor implements two methods:
      - get_listings(): Pass 1 — return (title, url) pairs from a listing page
      - get_detail():   Pass 2 — return full job detail from a job page URL

    Extractors that use a public API (Greenhouse, Lever, Ashby) override
    both methods and skip HTTP scraping entirely.

    Extractors that scrape HTML (Phenom, Generic) use the shared
    fetch() helper and implement their own parsing logic.
    """

    async def fetch(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        """Fetch a URL and return the response body as text."""
        try:
            async with httpx.AsyncClient(
                headers=DEFAULT_HEADERS,
                timeout=20.0,
                follow_redirects=True,
            ) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.text
        except httpx.HTTPError as e:
            raise RuntimeError(f"HTTP error fetching {url}: {e}")

    async def fetch_json(
        self,
        url: str,
        params: Optional[dict] = None,
        method: str = "GET",
        json: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> Optional[dict]:
        """
        Fetch a URL and return the response as parsed JSON.
        Supports both GET and POST with optional JSON body and custom headers.
        """
        try:
            merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
            async with httpx.AsyncClient(
                headers=merged_headers,
                timeout=20.0,
                follow_redirects=True,
            ) as client:
                if method.upper() == "POST":
                    response = await client.post(url, params=params, json=json)
                else:
                    response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPError as e:
            raise RuntimeError(f"HTTP error fetching {url}: {e}")

    @abstractmethod
    async def get_listings(self, source_url: str) -> List[JobListing]:
        """
        Pass 1: Fetch the career listing page and return a list of JobListings.
        Each JobListing contains at minimum a title and a URL.
        """
        ...

    @abstractmethod
    async def get_detail(self, listing: JobListing) -> JobDetail:
        """
        Pass 2: Fetch the full job detail for a given listing.
        Returns a JobDetail with all available structured fields populated.
        """
        ...