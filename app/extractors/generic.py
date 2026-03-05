"""
Generic Extractor
-----------------
Fallback extractor for plain static HTML career pages.
Uses httpx + BeautifulSoup anchor tag extraction.

Works well for simple pages with direct <a href="/jobs/123">Title</a>
patterns. Will miss JS-rendered content — those sources should have
requires_js = TRUE in the database and use the Playwright service instead.
"""

from typing import List
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from app.base import BaseExtractor
from app.models import JobListing, JobDetail


# URL path fragments that suggest a link is a job posting
JOB_PATH_PATTERNS = [
    "/job/", "/jobs/", "/careers/", "/position/", "/positions/",
    "/opening/", "/openings/", "/role/", "/roles/", "/posting/",
    "/postings/", "/vacancy/", "/vacancies/", "/apply/",
]


class GenericExtractor(BaseExtractor):

    async def get_listings(self, source_url: str) -> List[JobListing]:
        html = await self.fetch(source_url)
        if not html:
            return []

        return self._extract_from_html(html, source_url)

    async def get_detail(self, listing: JobListing) -> JobDetail:
        html = await self.fetch(listing.url)
        if not html:
            return JobDetail(title=listing.title, url=listing.url)

        soup = BeautifulSoup(html, "html.parser")

        # Remove noise
        for tag in soup(["nav", "footer", "header", "script", "style", "aside"]):
            tag.decompose()

        # Prefer semantic content areas
        main = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", {"id": lambda x: x and "job" in x.lower()})
            or soup.find("div", {"class": lambda x: x and "job" in " ".join(x).lower()})
            or soup.body
        )

        description = ""
        if main:
            description = main.get_text(separator="\n", strip=True)

        return JobDetail(
            title=listing.title,
            url=listing.url,
            description=description[:5000] if description else None,
        )

    def _extract_from_html(self, html: str, source_url: str) -> List[JobListing]:
        soup   = BeautifulSoup(html, "html.parser")
        parsed = urlparse(source_url)
        seen   = set()
        listings = []

        for a in soup.find_all("a", href=True):
            href  = a["href"].strip()
            title = a.get_text(strip=True)

            if not title or len(title) < 4:
                continue

            # Skip generic navigation links
            if len(title) > 120:
                continue

            # Resolve relative URLs
            if href.startswith("/"):
                href = f"{parsed.scheme}://{parsed.netloc}{href}"
            elif href.startswith("http"):
                pass
            else:
                continue

            # Must look like a job URL
            href_lower = href.lower()
            if not any(p in href_lower for p in JOB_PATH_PATTERNS):
                continue

            # Deduplicate
            if href in seen:
                continue

            seen.add(href)
            listings.append(JobListing(title=title, url=href))

        return listings
