"""
Extractor Registry
------------------
Maps source URLs to the appropriate extractor class.

Resolution order:
  1. Explicit extractor_type from the sources table (always wins)
  2. URL pattern matching against known ATS domains
  3. GenericExtractor fallback

To add a new extractor:
  1. Create app/extractors/myextractor.py
  2. Add an entry to URL_EXTRACTORS and TYPE_EXTRACTORS below
"""

from app.base import BaseExtractor
from app.extractors.greenhouse import GreenhouseExtractor
from app.extractors.lever import LeverExtractor
from app.extractors.ashby import AshbyExtractor
from app.extractors.bamboohr import BambooHRExtractor
from app.extractors.workday import WorkdayExtractor
from app.extractors.phenom import PhenomExtractor
from app.extractors.oracle import OracleExtractor
from app.extractors.generic import GenericExtractor

# Explicit extractor_type string → extractor class
TYPE_EXTRACTORS: dict[str, type[BaseExtractor]] = {
    "greenhouse":   GreenhouseExtractor,
    "lever":        LeverExtractor,
    "ashby":        AshbyExtractor,
    "bamboohr":     BambooHRExtractor,
    "workday":      WorkdayExtractor,
    "phenom":       PhenomExtractor,
    "oracle":       OracleExtractor,
    "generic":      GenericExtractor,
}

# URL domain pattern → extractor class (order matters — first match wins)
URL_EXTRACTORS: list[tuple[str, type[BaseExtractor]]] = [
    ("greenhouse.io",       GreenhouseExtractor),
    ("lever.co",            LeverExtractor),
    ("ashbyhq.com",         AshbyExtractor),
    ("bamboohr.com",        BambooHRExtractor),
    ("myworkdayjobs.com",   WorkdayExtractor),
    ("workday.com",         WorkdayExtractor),
    ("phenompeople.com",    PhenomExtractor),
    ("oraclecloud.com",     OracleExtractor),
]


def get_extractor(url: str, extractor_type: str = None) -> BaseExtractor:
    """
    Return the appropriate extractor for a given source.

    Args:
        url:            The source career page URL
        extractor_type: Optional explicit type from sources.extractor_type
                        If provided, takes priority over URL matching.
    """
    # 1. Explicit type override
    if extractor_type:
        cls = TYPE_EXTRACTORS.get(extractor_type.lower())
        if cls:
            return cls()

    # 2. URL pattern matching
    url_lower = url.lower()
    for domain, extractor_cls in URL_EXTRACTORS:
        if domain in url_lower:
            return extractor_cls()

    # 3. Fallback
    return GenericExtractor()