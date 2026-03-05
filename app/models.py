from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class JobListing:
    """
    Represents a job discovered during Pass 1 (listing page).
    Only title and url are required at this stage.
    """
    title:      str
    url:        str


@dataclass
class JobDetail:
    """
    Represents a fully scraped job after Pass 2 (detail page).
    All fields beyond title/url are optional until parsed.
    """
    title:          str
    url:            str
    company:        Optional[str]       = None
    location:       Optional[str]       = None
    job_type:       Optional[str]       = None
    salary:         Optional[str]       = None
    description:    Optional[str]       = None
    requirements:   List[str]           = field(default_factory=list)
    departments:    List[str]           = field(default_factory=list)


@dataclass
class ScrapeResult:
    """Summary of a single source scrape run."""
    source_id:      int
    company:        str
    status:         str                 # success | failed | partial | skipped
    jobs_found:     int                 = 0
    jobs_added:     int                 = 0
    jobs_filtered:  int                 = 0
    jobs_skipped:   int                 = 0
    error:          Optional[str]       = None