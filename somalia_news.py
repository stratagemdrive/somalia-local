"""
somalia_news.py
Fetches Somalia-focused news from English-language RSS feeds, categorizes
stories, and writes them to docs/somalia_news.json — capped at 20 per
category, max age 7 days, oldest entries replaced first.
No external APIs are used. All sources publish in English.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser
import feedparser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "somalia_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# RSS feeds — all free, English-language, Somalia-focused, no APIs
FEEDS = [
    # Somali Guardian — leading independent English-language Somalia news site
    {"source": "Somali Guardian", "url": "https://www.somaliguardian.com/feed/"},
    {"source": "Somali Guardian", "url": "https://www.somaliguardian.com/category/somalia/feed/"},
    # Somali Dispatch — English-language community news portal, active 2026
    {"source": "Somali Dispatch", "url": "https://www.somalidispatch.com/feed/"},
    # Puntland Post — independent English/Somali newspaper, Garowe (est. 2001)
    {"source": "Puntland Post", "url": "https://www.puntlandpost.net/feed/"},
    # SONNA — Somalia's official state news agency, English service
    {"source": "SONNA", "url": "https://sonna.so/en/feed/"},
    # Al Jazeera — dedicated Somalia section (English)
    {"source": "Al Jazeera", "url": "https://www.aljazeera.com/where/somalia/feed"},
    {"source": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    # Shabelle Media Network — major independent Somali broadcaster, English content
    {"source": "Shabelle Media", "url": "https://shabellemedia.com/feed/"},
]

# ---------------------------------------------------------------------------
# Category keyword mapping (Somalia-contextualised)
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacy", "diplomatic", "foreign policy", "embassy", "ambassador",
        "treaty", "bilateral", "multilateral", "united nations", "un",
        "foreign minister", "foreign affairs", "summit", "sanctions",
        "international relations", "geopolitical", "arab league", "igad",
        "trade deal", "accord", "alliance", "envoy", "consul",
        "african union", "au mission", "amisom", "atmis",
        "hassan sheikh", "president mohamud", "prime minister",
        "mogadishu talks", "somali government", "federal government",
        "turkey", "qatar", "ethiopia", "kenya", "djibouti",
        "somalia and ethiopia", "somalia and kenya", "us envoy",
        "recognition", "somaliland recognition", "somalia relations",
        "un security council", "un resolution", "peace agreement",
    ],
    "Military": [
        "military", "army", "navy", "air force", "defence", "defense",
        "troops", "soldier", "weapons", "armed forces", "war", "combat",
        "conflict", "bomb", "explosion", "airstrike", "strike",
        "al-shabaab", "al shabaab", "shabaab", "amisom", "atmis",
        "sna", "somali national army", "police", "security forces",
        "terrorism", "terrorist", "suicide bomb", "ied", "ambush",
        "killed", "casualties", "wounded", "attack", "operation",
        "raid", "offensive", "counter terrorism", "us africa command",
        "africom", "drone strike", "mogadishu attack", "checkpoint",
        "clan militia", "warlord", "jubaland", "puntland forces",
    ],
    "Energy": [
        "energy", "oil", "gas", "petroleum", "offshore", "drilling",
        "renewable", "solar", "wind", "electricity", "power grid",
        "blackout", "power cut", "power outage", "fuel", "diesel",
        "climate", "emissions", "environment", "drought",
        "flooding", "deforestation", "charcoal", "biomass",
        "energy crisis", "generator", "power plant", "grid",
        "turkish drillship", "exploration", "oil exploration",
        "gas exploration", "energy deal", "water supply",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "unemployment",
        "jobs", "budget", "finance", "tax", "investment", "business",
        "trade", "shilling", "somali shilling", "remittance",
        "hawala", "imf", "world bank", "donor", "aid",
        "humanitarian", "unrwa", "un agencies", "funding",
        "reconstruction", "development", "fisheries", "fishing",
        "livestock", "agriculture", "exports", "imports",
        "port", "berbera port", "kismayo", "mogadishu port",
        "free trade", "economic growth", "poverty", "famine",
        "food security", "world food programme", "wfp",
        "debt relief", "privatization", "banking", "mobile money",
        "hormuud", "dahabshiil", "telesom",
    ],
    "Local Events": [
        "local", "region", "state", "clan", "community",
        "hospital", "school", "university", "crime", "court",
        "flood", "drought", "fire", "transport", "strike",
        "protest", "housing", "mogadishu", "kismayo", "garowe",
        "hargeisa", "bosaso", "baidoa", "beledweyne", "dhusamareb",
        "jubaland", "puntland", "somaliland", "hirshabelle",
        "southwest state", "galmudug", "south west",
        "federal member state", "election", "parliament",
        "federal parliament", "house of the people", "senate",
        "corruption", "governance", "humanitarian", "idp",
        "displaced", "refugee", "camp", "drought relief",
        "food aid", "cholera", "disease", "health crisis",
        "clan conflict", "intra-clan", "reconciliation",
    ],
}


def classify(title: str, description: str):
    """Return the best-matching category for a story, or None if no match."""
    text = (title + " " + (description or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if re.search(r'\b' + re.escape(kw) + r'\b', text):
                scores[cat] += 1
    best_cat = max(scores, key=scores.get)
    return best_cat if scores[best_cat] > 0 else None


def strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_date(entry):
    """Parse a feed entry's published date into a UTC-aware datetime."""
    raw = entry.get("published") or entry.get("updated") or entry.get("created")
    if not raw:
        struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if struct:
            return datetime(*struct[:6], tzinfo=timezone.utc)
        return None
    try:
        dt = dateparser.parse(raw)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) if dt else None
    except Exception:
        return None


def fetch_feed(feed_cfg: dict) -> list:
    """Fetch a single RSS feed and return a list of story dicts."""
    source = feed_cfg["source"]
    url = feed_cfg["url"]
    stories = []
    try:
        parsed = feedparser.parse(url)
        if parsed.bozo and not parsed.entries:
            log.warning("Bozo feed (%s): %s", source, url)
            return stories
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        for entry in parsed.entries:
            pub_date = parse_date(entry)
            if pub_date and pub_date < cutoff:
                continue
            title = strip_html(entry.get("title", "")).strip()
            desc = strip_html(entry.get("summary", "")).strip()
            if not title:
                continue
            category = classify(title, desc)
            if not category:
                continue
            story = {
                "title": title,
                "source": source,
                "url": entry.get("link", ""),
                "published_date": pub_date.isoformat() if pub_date else None,
                "category": category,
            }
            stories.append(story)
    except Exception as exc:
        log.error("Failed to fetch %s (%s): %s", source, url, exc)
    return stories


def load_existing() -> dict:
    """Load the current JSON file, grouped by category."""
    if not os.path.exists(OUTPUT_FILE):
        return {cat: [] for cat in CATEGORIES}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {cat: [] for cat in CATEGORIES}

    grouped = {cat: [] for cat in CATEGORIES}
    stories = data.get("stories", data) if isinstance(data, dict) else data
    if isinstance(stories, list):
        for story in stories:
            cat = story.get("category")
            if cat in grouped:
                grouped[cat].append(story)
    return grouped


def merge(existing: dict, fresh: list) -> dict:
    """
    Merge fresh stories into the existing pool.
    - De-duplicate by URL.
    - Discard stories older than MAX_AGE_DAYS.
    - Replace oldest entries first when over MAX_PER_CATEGORY.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    existing_urls = set()
    for stories in existing.values():
        for s in stories:
            if s.get("url"):
                existing_urls.add(s["url"])

    for story in fresh:
        cat = story.get("category")
        if cat not in existing:
            continue
        if story["url"] in existing_urls:
            continue
        existing[cat].append(story)
        existing_urls.add(story["url"])

    for cat in CATEGORIES:
        pool = existing[cat]
        # Drop expired stories
        pool = [
            s for s in pool
            if s.get("published_date") and
               dateparser.parse(s["published_date"]).astimezone(timezone.utc) >= cutoff
        ]
        # Sort newest-first, cap at limit (oldest replaced first)
        pool.sort(key=lambda s: s.get("published_date") or "", reverse=True)
        existing[cat] = pool[:MAX_PER_CATEGORY]

    return existing


def write_output(grouped: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    flat = []
    for stories in grouped.values():
        flat.extend(stories)
    output = {
        "country": "Somalia",
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "story_count": len(flat),
        "stories": flat,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)
    log.info("Wrote %d stories to %s", len(flat), OUTPUT_FILE)


def main():
    log.info("Loading existing data ...")
    existing = load_existing()

    log.info("Fetching %d RSS feeds ...", len(FEEDS))
    fresh = []
    for cfg in FEEDS:
        results = fetch_feed(cfg)
        log.info("  %s — %d stories from %s", cfg["source"], len(results), cfg["url"])
        fresh.extend(results)
        time.sleep(0.5)  # polite crawl delay

    log.info("Merging %d fresh stories ...", len(fresh))
    merged = merge(existing, fresh)

    counts = {cat: len(merged[cat]) for cat in CATEGORIES}
    log.info("Category totals: %s", counts)

    write_output(merged)


if __name__ == "__main__":
    main()
