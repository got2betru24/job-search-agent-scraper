import hashlib
import os
import re
from typing import List, Optional


def hash_url(url: str) -> str:
    """SHA-256 hash of a URL for deduplication."""
    return hashlib.sha256(url.strip().encode()).hexdigest()


def title_matches_filters(title: str, filters: Optional[List[str]]) -> bool:
    """
    Check if a job title matches any filter in the list.
    Filters are case-insensitive substring matches unless
    the value starts with ^ in which case it is treated as regex.
    Returns True if filters is None or empty (accept all).
    """
    if not filters:
        return True

    title_lower = title.lower()

    for f in filters:
        if f.startswith("^"):
            if re.search(f, title_lower):
                return True
        else:
            if f.lower() in title_lower:
                return True

    return False


# Role classification rules — ordered from most specific to least.
# First match wins. Claude will refine this in a future phase.
_ROLE_RULES: List[tuple[str, str]] = [
    # Engineering Manager / Director — check before IC rules
    (r"engineering manager",                                                                          "engineering_manager"),
    (r"(manager|mgr)[,\s].*(engineer|tech|cloud|platform|infrastructure|data|ml|ai|analytics|software|intelligence|devinfra|ci.cd|system)",      "engineering_manager"),
    (r"(sr\.?\s*|senior\s*)(manager|mgr).*(engineer|tech|cloud|platform|infrastructure|data|ml|ai|analytics|software|intelligence|devinfra|ci.cd|system)", "engineering_manager"),
    (r"(engineer|tech|cloud|platform|infrastructure|data|ml|ai|analytics|software|intelligence|devinfra|system).*(manager|mgr)",           "engineering_manager"),
    (r"director.*(engineer|tech|product|data|analytics|platform|cloud|software|architecture|infrastructure|ml|ai)",                             "engineering_manager"),
    (r"vp.*(engineer|tech)",                                                                         "engineering_manager"),
    (r"head of engineer",                                                                            "engineering_manager"),

    # Product Manager — check before IC rules
    (r"product manager",                    "product_manager"),
    (r"(manager|mgr)[,\s].*product",        "product_manager"),
    (r"product.*(manager|mgr)",             "product_manager"),
    (r"director.*product",                  "product_manager"),
    (r"vp.*product",                        "product_manager"),
    (r"head of product",                    "product_manager"),

    # Engineer (IC) — everything else engineering/data/analytics
    (r"engineer",                           "engineer"),
    (r"developer",                          "engineer"),
    (r"architect",                          "engineer"),
    (r"data scientist",                     "engineer"),
    (r"data analyst",                       "engineer"),
    (r"analytics",                          "engineer"),
]


def classify_role(title: str) -> Optional[str]:
    """
    Classify a job title into one of three role buckets:
      engineering_manager | product_manager | engineer

    Returns None if no rule matches.
    Claude will refine this classification in a future phase.
    """
    title_lower = title.lower()
    for pattern, role in _ROLE_RULES:
        if re.search(pattern, title_lower):
            return role
    return None


def get_target_roles() -> Optional[List[str]]:
    """
    Read TARGET_ROLES from environment.
    Returns None if not set (accept all roles).
    Returns a list of role strings if set.

    Example .env:
      TARGET_ROLES=engineering_manager,product_manager
    """
    raw = os.getenv("TARGET_ROLES", "").strip()
    if not raw:
        return None
    return [r.strip() for r in raw.split(",") if r.strip()]


def role_is_targeted(role: Optional[str], target_roles: Optional[List[str]]) -> bool:
    """
    Returns True if the role should be accepted given target_roles config.
    - If target_roles is None, accept everything.
    - If role is None (unclassified), accept it to avoid silent drops.
    - Otherwise only accept if role is in target_roles.
    """
    if not target_roles:
        return True
    if role is None:
        return True
    return role in target_roles


# Standard headers to avoid bot detection on simple sites
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def get_target_locations() -> Optional[List[str]]:
    """
    Read TARGET_LOCATIONS from environment.
    Returns None if not set (accept all locations).
    Returns a list of lowercase keyword strings if set.

    Example .env:
      TARGET_LOCATIONS=remote,utah,salt lake,lehi,provo
    """
    raw = os.getenv("TARGET_LOCATIONS", "").strip()
    if not raw:
        return None
    # Use | as delimiter to allow commas in location strings (e.g. "remote, us")
    return [loc.strip().lower() for loc in raw.split("|") if loc.strip()]


def location_is_targeted(location: Optional[str], target_locations: Optional[List[str]]) -> bool:
    """
    Returns True if the location should be accepted given target_locations config.
    Uses fuzzy keyword matching — any target keyword appearing in the
    location string is a match.

    - If target_locations is None, accept everything.
    - If location is None (unknown), accept it to avoid silent drops.
    - Otherwise check if any target keyword appears in the location string.
    """
    if not target_locations:
        return True
    if not location:
        return True  # unknown location — let it through, review manually
    location_lower = location.lower()
    return any(keyword in location_lower for keyword in target_locations)


def get_target_departments() -> Optional[List[str]]:
    """
    Read TARGET_DEPARTMENTS from environment.
    Returns None if not set (accept all departments).
    Returns a list of lowercase keyword strings if set.

    Example .env:
      TARGET_DEPARTMENTS=engineering,product,data science,analytics,platform,technology
    """
    raw = os.getenv("TARGET_DEPARTMENTS", "").strip()
    if not raw:
        return None
    return [d.strip().lower() for d in raw.split(",") if d.strip()]


def department_is_targeted(departments: Optional[List[str]], target_departments: Optional[List[str]]) -> bool:
    """
    Returns True if any of the job's departments match target_departments.
    Uses fuzzy keyword matching — any target keyword appearing in any
    department name is a match.

    - If target_departments is None, accept everything.
    - If departments is None or empty, accept to avoid silent drops.
    - Otherwise check if any target keyword appears in any department name.
    """
    if not target_departments:
        return True
    if not departments:
        return True  # unknown department — let it through, review manually
    departments_lower = [d.lower() for d in departments]
    return any(
        keyword in dept
        for keyword in target_departments
        for dept in departments_lower
    )