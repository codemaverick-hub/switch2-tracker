#!/usr/bin/env python3
"""
Multi-source release date scraper for upcoming Switch 2 games.

Sources (tried in priority order per game):
  1. NSCollectors Upcoming Summary tab  — exact per-region dates (handled in main scrape.py)
  2. Nintendo EU search API             — upcoming dates from official store
  3. Nintendo Life confirmed games page — comprehensive community-maintained table
  4. Wikipedia                          — year/quarter hints (handled in main scrape.py)
"""

import re
import sys
from datetime import datetime, timezone

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests beautifulsoup4")
    sys.exit(1)

NINTENDO_EU   = "https://searching.nintendo-europe.com/en/select"
NINTENDOLIFE  = "https://www.nintendolife.com/guides/nintendo-switch-2-all-confirmed-games-and-release-dates"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def norm(t):
    t = re.sub(r"[-\u2013\u2014\u2019]", " ", t.lower())
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return re.sub(r"\s+", " ", t).strip()

def parse_date_str(raw):
    """Parse various date formats → (display_str, status)."""
    raw = raw.strip()
    now = datetime.now(timezone.utc)

    # ISO / YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
            fmt = "%b %-d, %Y" if sys.platform != "win32" else "%b %#d, %Y"
            return d.strftime(fmt), "r" if d <= now else "u"
        except: pass

    # Month Day, Year  e.g. "May 21, 2026"
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %-d, %Y"]:
        try:
            d = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            out_fmt = "%b %-d, %Y" if sys.platform != "win32" else "%b %#d, %Y"
            return d.strftime(out_fmt), "r" if d <= now else "u"
        except: pass

    # Quarter  e.g. "Q2 2026"
    m = re.match(r"Q([1-4])\s*(20\d{2})", raw, re.I)
    if m:
        return f"Q{m.group(1)} {m.group(2)}", "u"

    # Year only  e.g. "2026" or "2027"
    m = re.match(r"^(20[2-9]\d)$", raw)
    if m:
        return m.group(1), "u"

    return raw, "u"


# ── Nintendo EU upcoming dates ────────────────────────────────────────────────
def fetch_eu_upcoming(session, max_rows=200):
    """
    Query Nintendo EU search for upcoming Switch 2 games with release dates.
    Returns {norm_title: (date_str, status)}
    """
    print("Fetching Nintendo EU upcoming dates…")
    results = {}
    try:
        params = {
            "q": "*",
            "fq": "type:GAME AND system_type:nintendoswitch2*",
            "rows": str(max_rows),
            "fl": "title,dates_released_dts,sorting_title",
            "sort": "dates_released_dts asc",
        }
        r = session.get(NINTENDO_EU, params=params, timeout=15)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        for doc in docs:
            title = doc.get("title", "")
            date_raw = doc.get("dates_released_dts", "")
            if not title or not date_raw:
                continue
            # dates_released_dts is ISO format e.g. "2026-05-21T00:00:00Z"
            date_disp, status = parse_date_str(date_raw[:10])
            nk = norm(title)
            if nk and date_disp not in ("TBA", ""):
                results[nk] = (title, date_disp, status)
        print(f"  {len(results)} games with dates from Nintendo EU")
    except Exception as e:
        print(f"  ⚠ Nintendo EU dates failed: {e}")
    return results


# ── Nintendo Life confirmed games table ───────────────────────────────────────
def fetch_nintendolife_dates(session):
    """
    Scrape Nintendo Life's confirmed Switch 2 games guide.
    Returns {norm_title: (date_str, status)}
    """
    print("Fetching Nintendo Life release dates…")
    results = {}
    try:
        r = session.get(NINTENDOLIFE, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Nintendo Life uses a table with Game / Release Date / Platform columns
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            # Find header to identify columns
            if not rows:
                continue
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

            # Look for title and date columns
            title_idx = next((i for i, h in enumerate(headers) if "game" in h or "title" in h), None)
            date_idx  = next((i for i, h in enumerate(headers) if "date" in h or "release" in h), None)

            if title_idx is None or date_idx is None:
                continue

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) <= max(title_idx, date_idx):
                    continue
                title = cells[title_idx].get_text(" ", strip=True)
                date_raw = cells[date_idx].get_text(" ", strip=True)
                title = re.sub(r"\[.*?\]", "", title).strip()
                if not title or not date_raw or date_raw.lower() in ("tba", "tbd", "—", "-", ""):
                    continue
                date_disp, status = parse_date_str(date_raw)
                nk = norm(title)
                if nk:
                    results[nk] = (title, date_disp, status)

        # Also check article body for game mentions with dates (fallback)
        if len(results) < 10:
            # Look for patterns like "Game Name – May 21, 2026"
            body = soup.find("article") or soup.find("main") or soup.body
            if body:
                text = body.get_text(" ")
                # Match "Title – Month Day, Year" or "Title (Month Day, Year)"
                patterns = [
                    r"([A-Z][^–\n]{5,50})\s*[–-]\s*(\w+ \d{1,2},\s*202\d)",
                    r"([A-Z][^–\n]{5,50})\s*[–-]\s*(Q[1-4]\s*202\d)",
                    r"([A-Z][^–\n]{5,50})\s*[–-]\s*(202\d)",
                ]
                for pat in patterns:
                    for m in re.finditer(pat, text):
                        title = m.group(1).strip()
                        date_raw = m.group(2).strip()
                        date_disp, status = parse_date_str(date_raw)
                        nk = norm(title)
                        if nk and nk not in results:
                            results[nk] = (title, date_disp, status)

        print(f"  {len(results)} games with dates from Nintendo Life")
    except Exception as e:
        print(f"  ⚠ Nintendo Life scrape failed: {e}")
    return results


# ── Wikipedia list ─────────────────────────────────────────────────────────────
def fetch_wiki_dates(session, wiki_url="https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"):
    """
    Extract release dates from Wikipedia's Switch 2 game table.
    Returns {norm_title: (date_str, status)}
    """
    print("Fetching Wikipedia release dates…")
    results = {}
    try:
        r = session.get(wiki_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table", class_="wikitable")
        if not table:
            print("  ⚠ wikitable not found")
            return results

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            title = re.sub(r"\[.*?\]", "", cells[0].get_text(" ", strip=True)).strip()
            if not title:
                continue
            # Release date is typically in column 4 or 5
            for cell in cells[3:7]:
                txt = cell.get_text(" ", strip=True)
                # Strip refs
                txt = re.sub(r"\[.*?\]", "", txt).strip()
                if not txt or txt.lower() in ("tba", "tbd", "—", "-"):
                    continue
                date_disp, status = parse_date_str(txt)
                if date_disp and date_disp not in ("TBA", txt):
                    nk = norm(title)
                    if nk:
                        results[nk] = (title, date_disp, status)
                    break

        print(f"  {len(results)} games with dates from Wikipedia")
    except Exception as e:
        print(f"  ⚠ Wikipedia dates failed: {e}")
    return results


# ── Main entry point ───────────────────────────────────────────────────────────
def get_date_map():
    """
    Fetch dates from all sources and merge.
    Priority: Nintendo EU > Nintendo Life > Wikipedia
    Returns {norm_title: (title, date_str, status)}
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # Fetch all sources
    eu_dates   = fetch_eu_upcoming(session)
    nl_dates   = fetch_nintendolife_dates(session)
    wiki_dates = fetch_wiki_dates(session)

    # Merge: EU takes priority, then NL, then Wiki
    merged = {}
    for source in [wiki_dates, nl_dates, eu_dates]:
        for nk, val in source.items():
            if nk not in merged or (
                # Prefer more specific dates
                len(val[1]) > len(merged[nk][1]) and
                not re.match(r"^\d{4}$", val[1])  # year-only is less specific
            ):
                merged[nk] = val

    # Stats
    specific = sum(1 for v in merged.values() if re.search(r"\d{1,2},\s*20\d{2}", v[1]))
    quarter  = sum(1 for v in merged.values() if re.match(r"Q[1-4]", v[1]))
    year     = sum(1 for v in merged.values() if re.match(r"^\d{4}$", v[1]))
    print(f"\n✓ Date map: {len(merged)} games total")
    print(f"  Specific date: {specific}  Quarter: {quarter}  Year only: {year}")

    return merged


if __name__ == "__main__":
    dm = get_date_map()
    print("\nSample (first 15):")
    for nk, (title, date, status) in list(dm.items())[:15]:
        print(f"  [{status}] {title[:45]:45} → {date}")
