#!/usr/bin/env python3
"""
Nintendo Switch 2 game list scraper.
Fetches the Wikipedia game list, merges with local format/region overrides,
and writes data/games.json.

Run manually:  python scripts/scrape.py
Run by:        GitHub Actions (.github/workflows/update-games.yml)
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run: pip install requests beautifulsoup4")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
OVERRIDES_FILE = ROOT / "data" / "overrides.json"
OUT_FILE       = ROOT / "data" / "games.json"

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"
HEADERS = {"User-Agent": "Switch2Tracker/1.0 (github.com/yourname/switch2-tracker; educational)"}

# ── Nintendo first-party publishers (→ NS2 Exclusive if title isn't NS2 Edition) ──
NINTENDO_PUBLISHERS = {
    "nintendo", "nintendo epd", "hal laboratory", "retro studios",
    "intelligent systems", "game freak", "the pokémon company",
    "camelot software planning", "monolith soft", "nd cube",
    "bandai namco studios, sora ltd.",  # Kirby Air Riders
}

# ── Genre inference from title/publisher keywords ────────────────────────────
GENRE_HINTS = [
    (r"kart|racing|formula|speed|drift|rally|moto", "Racing"),
    (r"mario party|party|jamboree", "Party"),
    (r"zelda|breath of the wild|tears of the kingdom|hyrule warriors|age of imprisonment", "Action-Adventure"),
    (r"pokemon|pokémon", "RPG"),
    (r"kirby|platformer|wonder|bananza|yoshi", "Platformer"),
    (r"fire emblem|disgaea|tactics|strategy|nobunaga|dynasty warriors|duskbloods", "Strategy RPG"),
    (r"splatoon|drag x drive", "Sports"),
    (r"tennis|madden|nba|nfl|pga|efl|fc 2|fifa|football|soccer|basketball|baseball|hockey|golf", "Sports"),
    (r"metroid|resident evil|fatal frame|project zero|horror|nightmare|bloober|phasmophobia|cronos", "Horror"),
    (r"final fantasy|dragon quest|octopath|bravely|persona|atelier|tales of|ys |xenoblade|elden ring|rpg|granblue|suikoden|rune factory|story of seasons|fantasy life|cyberpunk|yakuza|yakuza|raidou", "Action RPG"),
    (r"animal crossing|stardew|tomodachi|time at|my time|evershine|life sim|farming|dave the diver|no man", "Life Sim"),
    (r"hollow knight|silksong|hades|enter the gungeon|balatro|roguelike|rogue", "Action Roguelike"),
    (r"street fighter|mortal kombat|tekken|dragon ball|fighting", "Fighting"),
    (r"hitman|assassin|star wars|indiana jones|hogwarts|batman|lego", "Action-Adventure"),
    (r"overwatch|apex|fortnite|battle royale", "Battle Royale"),
    (r"a-train|train|simulator|farming|powerwash|goat sim", "Simulation"),
    (r"factorio|overcooked|cook|civilization|civ 7", "Strategy"),
    (r"layton|professor layton|puzzle|puyo|tetris", "Puzzle"),
    (r"sonic|pac-man|mega man|yooka|hollow knight|kirby|platformer|wonder|bananza|bubsy|bubsy", "Platformer"),
]

def infer_genre(title: str, publisher: str) -> str:
    combined = (title + " " + publisher).lower()
    for pattern, genre in GENRE_HINTS:
        if re.search(pattern, combined):
            return genre
    return "Action"  # fallback


def classify_type(title: str, publisher: str, developer: str) -> str:
    """Return 'e' (NS2 exclusive), 'n' (NS2 Edition), or 't' (third-party)."""
    title_lower = title.lower()
    if "nintendo switch 2 edition" in title_lower or "– ns2 ed" in title_lower:
        return "n"
    pub_lower = publisher.lower()
    # If any Nintendo studio is listed as publisher it's first-party
    if any(n in pub_lower for n in NINTENDO_PUBLISHERS):
        return "e"
    return "t"


def parse_date(raw: str) -> tuple[str, str]:
    """Return (display_date, status) where status is 'r' or 'u'."""
    raw = raw.strip()
    now = datetime.now(timezone.utc)

    # Remove citation markers like [1], [a]
    raw = re.sub(r'\[.*?\]', '', raw).strip()

    if not raw or raw.upper() in ("TBA", "N/A", ""):
        return "TBA", "u"

    # Loose year-only: "2025" or "2026"
    m = re.fullmatch(r'(\d{4})', raw)
    if m:
        yr = int(m.group(1))
        return raw, "r" if yr <= now.year else "u"

    # Quarter: "Q1 2026"
    m = re.match(r'Q(\d)\s+(\d{4})', raw, re.I)
    if m:
        q, yr = int(m.group(1)), int(m.group(2))
        q_month = {1: 3, 2: 6, 3: 9, 4: 12}[q]
        d = datetime(yr, q_month, 30, tzinfo=timezone.utc)
        return raw, "r" if d <= now else "u"

    # Season
    seasons = {"spring": (3, 21), "summer": (6, 21), "fall": (9, 22), "autumn": (9, 22), "winter": (12, 21)}
    for season, (mo, dy) in seasons.items():
        m = re.search(rf'{season}\s+(\d{{4}})', raw, re.I)
        if m:
            yr = int(m.group(1))
            d = datetime(yr, mo, dy, tzinfo=timezone.utc)
            return raw, "r" if d <= now else "u"

    # Full date: "June 5, 2025" or "5 June 2025"
    fmts = ["%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y",
            "%B %d %Y", "%d/%m/%Y", "%Y-%m-%d"]
    clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', raw)
    for fmt in fmts:
        try:
            d = datetime.strptime(clean.strip(), fmt).replace(tzinfo=timezone.utc)
            return raw, "r" if d <= now else "u"
        except ValueError:
            continue

    return raw, "u"


def scrape_wikipedia() -> list[dict]:
    print(f"Fetching {WIKIPEDIA_URL} …")
    r = requests.get(WIKIPEDIA_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # The game list is in a wikitable — there may be multiple; grab the main one
    table = soup.find("table", class_="wikitable")
    if not table:
        raise ValueError("Could not find wikitable on the page")

    games = []
    rows = table.find_all("tr")[1:]  # skip header

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            continue

        # Wikipedia columns: Title | Developer(s) | Publisher(s) | Release date | Ref
        title_cell = cells[0]
        dev_cell   = cells[1]
        pub_cell   = cells[2]
        date_cell  = cells[3]

        title = title_cell.get_text(" ", strip=True)
        title = re.sub(r'\[.*?\]', '', title).strip()
        developer  = dev_cell.get_text(", ", strip=True)
        publisher  = pub_cell.get_text(", ", strip=True)
        date_raw   = date_cell.get_text(" ", strip=True)

        # Skip empty / stub rows
        if not title or title.lower() in ("title", ""):
            continue

        display_date, status = parse_date(date_raw)
        game_type = classify_type(title, publisher, developer)
        genre     = infer_genre(title, publisher)

        games.append({
            "title":     title,
            "publisher": publisher,
            "developer": developer,
            "type":      game_type,    # e / n / t
            "genre":     genre,
            "date":      display_date,
            "status":    status,       # r / u
            "fmt":       "?",          # overridden by overrides.json
            "region":    "ww",         # overridden by overrides.json
            "note":      "",
        })

    print(f"  Scraped {len(games)} games from Wikipedia")
    return games


def load_overrides() -> dict:
    """
    overrides.json maps normalised title → override fields.
    Example:
    {
      "Daemon X Machina: Titanic Scion": {
        "fmt": "c",
        "region": "var",
        "note": "Full cart in West; GKC in Japan"
      }
    }
    """
    if not OVERRIDES_FILE.exists():
        print("  No overrides.json found — using defaults")
        return {}
    with open(OVERRIDES_FILE, encoding="utf-8") as f:
        data = json.load(f)
    print(f"  Loaded {len(data)} overrides")
    return data


def normalise(title: str) -> str:
    """Lowercase + strip punctuation for fuzzy matching."""
    return re.sub(r'[^a-z0-9 ]', '', title.lower()).strip()


def merge(games: list[dict], overrides: dict) -> list[dict]:
    """Apply overrides to scraped games by fuzzy title match."""
    norm_map = {normalise(k): v for k, v in overrides.items()}
    for game in games:
        key = normalise(game["title"])
        # Exact normalised match
        if key in norm_map:
            game.update(norm_map[key])
            continue
        # Partial match (override key is substring of game title)
        for ok, ov in norm_map.items():
            if ok in key or key in ok:
                game.update(ov)
                break
    return games


def assign_ids(games: list[dict]) -> list[dict]:
    """Stable sequential IDs sorted by title."""
    for i, g in enumerate(sorted(games, key=lambda x: x["title"].lower()), start=1):
        g["id"] = i
    return games


def main():
    games = scrape_wikipedia()
    overrides = load_overrides()
    games = merge(games, overrides)
    games = assign_ids(games)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "source":  WIKIPEDIA_URL,
        "count":   len(games),
        "games":   games,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Wrote {len(games)} games → {OUT_FILE}")
    print(f"  Released: {sum(1 for g in games if g['status']=='r')}")
    print(f"  Upcoming: {sum(1 for g in games if g['status']=='u')}")


if __name__ == "__main__":
    main()
