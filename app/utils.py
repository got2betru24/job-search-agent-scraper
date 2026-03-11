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


# ── Role classification ───────────────────────────────────────────────────
#
# Two-stage design:
#
#   Stage 1 — Role TYPE (what this function returns)
#     Determined purely by management structure words in the title.
#     Domain keywords (ml, ai, data, platform, etc.) are intentionally
#     absent from PM rules — they caused PM titles like "Sr. Product
#     Manager, AI/ML" to misclassify as engineering_manager.
#
#   Stage 2 — Domain / fit (handled by BLOCKED_TITLE_KEYWORDS in .env)
#     Negative keyword matching only — no allowlist needed.
#
# Priority order (first match wins):
#   product_manager → tpm → engineering_manager → engineer → None
#
# TPM is classified but filtered out by default (not in TARGET_ROLES).
# Add 'tpm' to TARGET_ROLES in .env to enable.

_ROLE_RULES: List[tuple[str, str]] = [

    # ── Product Manager ───────────────────────────────────────────────────
    # Checked FIRST — "product manager" is unambiguous and must win over
    # any domain keywords (ai, ml, data) that appear later in the title.
    (r"product manager",                                            "product_manager"),
    (r"(manager|mgr)[,\s]+product(?!\s+engineer)",                  "product_manager"),
    (r"product\s+(manager|mgr|owner|lead)",                         "product_manager"),
    (r"(director|vp|head)[,\s]+(?:of\s+)?product(?!\s+engineer)",   "product_manager"),

    # ── TPM — Technical/Engineering Program Manager ───────────────────────
    # Checked before EM rules — "program manager" titles must not fall
    # through to EM rules via domain keywords like ml/ai/infrastructure.
    (r"technical program manager",                                  "tpm"),
    (r"(engineering|technical)\s+program\s+manager",                "tpm"),
    (r"\btpm\b",                                                    "tpm"),
    (r"program\s+(manager|mgr|mgmt|management)",                   "tpm"),

    # ── Engineering Manager ───────────────────────────────────────────────
    (r"engineering manager",                                        "engineering_manager"),
    (r"(manager|mgr)[,\s]+engineering",                             "engineering_manager"),

    # "Manager <level?>, <domain> Engineering" e.g. "Manager II, Machine Learning Engineering"
    (r"(manager|mgr)\s*(?:[ivx]+|\d+)?\s*[,\s]+\w[\w\s]*engineering", "engineering_manager"),

    # Machine learning manager regardless of what follows (e.g. "Manager II, Machine Learning-Search")
    (r"(manager|mgr).*machine learning",                                "engineering_manager"),

    # BI/data/analytics managers — domain before manager is an unambiguous technical signal
    (r"(business intelligence|data|analytics|software).*(manager|mgr)", "engineering_manager"),

    # Senior/Sr Manager — broad, blocklist handles non-engineering domains
    (r"(sr\.?\s*|senior\s*)(manager|mgr)",                        "engineering_manager"),

    # Director/VP/Head — broad, blocklist handles sales/ops/marketing/etc.
    (r"(director|vp|head)[,\s]+(?:of\s+)?\w",                    "engineering_manager"),

    # "Product Engineering Manager" / "Manager, Product Engineering" → EM
    (r"product\s+engineering\s+(manager|mgr)",                      "engineering_manager"),
    (r"(manager|mgr)[,\s]+product\s+engineering",                   "engineering_manager"),

    # ── Engineer (IC) ─────────────────────────────────────────────────────
    (r"engineer",                                                   "engineer"),
    (r"developer",                                                   "engineer"),
    (r"architect",                                                   "engineer"),
    (r"scientist",                                                   "engineer"),
    (r"analyst",                                                     "engineer"),
    (r"analytics",                                                   "engineer"),
]


def classify_role(title: str) -> Optional[str]:
    """
    Classify a job title into one of five role buckets:
      product_manager | tpm | engineering_manager | engineer | None

    Stage 1 classification — role TYPE only, based on management structure
    words. Domain keywords (ml, ai, data, platform) are intentionally absent
    from PM rules to prevent titles like "Sr. Product Manager, AI/ML" from
    miscategorizing as engineering_manager.

    Stage 2 (domain/fit) is handled downstream by title_is_blocked() and
    the BLOCKED_TITLE_KEYWORDS env var.

    TPM roles are classified but filtered out by default — add 'tpm' to
    TARGET_ROLES in .env to enable them.

    Returns None if no rule matches.
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
    return [r.strip() for r in raw.split("|") if r.strip()]


def role_is_targeted(role: Optional[str], target_roles: Optional[List[str]]) -> bool:
    """
    Returns True if the role should be accepted given target_roles config.
    - If target_roles is None, accept everything.
    - If role is None (unclassified), reject it — rules are comprehensive
      enough that None means genuinely unrecognized, not a gap in coverage.
    - Otherwise only accept if role is in target_roles.
    """
    if not target_roles:
        return True
    if role is None:
        return False
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
    return [loc.lower() for loc in raw.split("|") if loc]


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
    return [d.strip().lower() for d in raw.split("|") if d.strip()]


def get_blocked_title_keywords() -> Optional[List[str]]:
    """
    Read BLOCKED_TITLE_KEYWORDS from environment.
    Returns None if not set (block nothing).
    Returns a list of lowercase keyword strings if set.

    Global blocklist applied to all roles — replaces the old per-source
    JSON_FILTERS allowlist. Only add when a false positive slips through.

    Example .env:
      BLOCKED_TITLE_KEYWORDS=security|identity|quality|compliance|legal|hr|human resources|facilities|procurement
    """
    raw = os.getenv("BLOCKED_TITLE_KEYWORDS", "").strip()
    if not raw:
        return None
    return [kw.strip().lower() for kw in raw.split("|") if kw.strip()]


def title_is_blocked(title: str, blocked_keywords: Optional[List[str]]) -> bool:
    """
    Returns True if the title contains any blocked keyword.
    - If blocked_keywords is None or empty, nothing is blocked.
    - Case-insensitive substring match.
    """
    if not blocked_keywords:
        return False
    title_lower = title.lower()
    return any(kw in title_lower for kw in blocked_keywords)


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


def clean_html(raw: Optional[str]) -> Optional[str]:
    """
    Convert HTML to clean Markdown, or normalize plain text.
    Returns None if the input is empty or None.

    HTML conversion rules:
      - h1/h2/h3/h4  -> ## Heading
      - h5/h6        -> ### Heading
      - li           -> - item
      - strong/b     -> **text**
      - em/i         -> *text*
      - p/div/br     -> paragraph breaks
      - All other tags stripped

    Plain text (no HTML tags) is normalized only.
    """
    if not raw or not raw.strip():
        return None

    from bs4 import BeautifulSoup, NavigableString
    from html import unescape

    # Normalize encoding — replace Windows-1252 curly quotes/dashes that
    # sneak in from some Workday sources as mojibake (e.g. worlds -> world's)
    # Try decoding as Windows-1252 first to recover curly quotes/apostrophes,
    # then re-encode to clean UTF-8
    try:
        raw = raw.encode("latin-1").decode("windows-1252")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # Decode HTML entities before parsing (e.g. Greenhouse returns &lt;div&gt; encoded)
    raw = unescape(raw)

    if not re.search(r"<[a-zA-Z]", raw):
        text = re.sub(r"[ \t]{2,}", " ", raw)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Detect implicit section headers in plain text —
        # short phrases ending in : that follow a sentence boundary or newline.
        # e.g. "What you'll be doing:" -> "\n\n## What you'll be doing\n"
        # Also catch a title-case phrase on its own line (e.g. "About the Role")
        text = re.sub(
            r"(?:^|(?<=[.?!])\s{1,2})((?:[A-Z][^.?!:\n]{3,55}?)):\s+(?=[A-Z])",
            lambda m: "\n\n## " + m.group(1).strip() + "\n\n",
            text
        )
        # Standalone title-case line with no colon (e.g. "About the Role\n")
        text = re.sub(
            r"(?:^|\n)([A-Z][A-Za-z ,']{3,50})\n(?=[A-Z])",
            lambda m: "\n\n## " + m.group(1).strip() + "\n\n",
            text
        )
        # ALL-CAPS section headers inline in text (e.g. "POSITION SUMMARY Blah...")
        # Must be 2+ words, all caps, followed by a capital letter starting body text
        text = re.sub(
            r"(?<![A-Z])([A-Z]{2,}(?:\s+[A-Z&]{2,})+)\s+(?=[A-Z][a-z])",
            lambda m: "\n\n## " + m.group(1).strip().title() + "\n\n",
            text
        )
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip() or None

    soup = BeautifulSoup(raw, "html.parser")

    def node_to_md(node) -> str:
        if isinstance(node, NavigableString):
            return str(node)
        tag = node.name
        children = "".join(node_to_md(c) for c in node.children)
        cs = children.strip()
        if tag in ("h1", "h2", "h3", "h4"):
            return "\n\n## " + cs + "\n\n"
        if tag in ("h5", "h6"):
            return "\n\n### " + cs + "\n\n"
        if tag in ("strong", "b"):
            return "**" + cs + "**" if cs else ""
        if tag in ("em", "i"):
            return "*" + cs + "*" if cs else ""
        if tag == "li":
            return "\n- " + cs
        if tag in ("ul", "ol"):
            return "\n" + children + "\n"
        if tag in ("p", "div"):
            return "\n\n" + cs + "\n\n" if cs else ""
        if tag == "br":
            return "\n"
        if tag in ("script", "style"):
            return ""
        return children

    md = node_to_md(soup)
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r"[ \t]{2,}", " ", md)
    return md.strip() or None


def extract_salary(description: Optional[str]) -> Optional[str]:
    """
    Best-effort salary extraction from job description text.
    Only called when no structured salary field is available.

    Matches common patterns:
      $120,000 - $150,000
      $120K-$150K
      $120k to $150k
      $120,000+
      $150,000/year
    Skips hourly rates ($/hour, $/hr).
    """
    if not description:
        return None

    # Normalize — remove markdown bold markers for cleaner matching
    text = re.sub(r"\*{1,2}", "", description)

    # Skip hourly — match $/hr or $/hour anywhere near a dollar amount
    hourly = re.compile(r"\$[\d,K k]+\s*(?:/\s*h(?:ou?r|r)?)", re.I)

    # Main salary pattern — dollar sign + digits/commas/K suffix
    # Handles ranges with - / to / – and single values with optional +
    pattern = re.compile(
        r"(\$[\d,]+(?:\.\d+)?[Kk]?)"  # lower bound e.g. $120,000 or $173,600.00
        r"(?:"                          # optional range
        r"\s*(?:--|—|–|-|to)\s*"      # separator (--, em-dash, en-dash, hyphen)
        r"(\$[\d,]+(?:\.\d+)?[Kk]?)"           # upper bound
        r"|\+)?",                      # or just a + for open-ended
        re.I
    )

    for match in pattern.finditer(text):
        full = match.group(0).strip()
        # Skip if this looks like an hourly rate
        surrounding = text[match.start():match.end() + 20]
        if hourly.search(surrounding):
            continue
        # Must have at least 4 digits to avoid matching e.g. "$5"
        digits = re.sub(r"[^\d]", "", full)
        if len(digits) < 4:
            continue
        return full

    # Secondary pattern — no dollar sign, but followed by USD
    # e.g. "224,000 USD - 356,500 USD" or "224,000 - 356,500 USD"
    usd_pattern = re.compile(
        r"([\d,]+)"                         # lower bound
        r"(?:\s*USD)?"                      # optional USD
        r"\s*(?:--|\u2014|\u2013|-|to)\s*"  # separator
        r"([\d,]+)"                         # upper bound
        r"(?:\s*USD)?",                     # optional USD
        re.I
    )
    for match in usd_pattern.finditer(text):
        # Confirm USD appears near the match
        surrounding = text[match.start():match.end() + 10]
        if "usd" not in surrounding.lower():
            continue
        digits = re.sub(r"[^\d]", "", match.group(0))
        if len(digits) < 4:
            continue
        return match.group(0).strip()

    return None