#!/usr/bin/env python3
"""
Multi-source release date scraper for upcoming Switch 2 games.

Sources (in priority order):
  1. NSCollectors Upcoming Summary tab  — exact regional dates (scrape.py handles this)
  2. Nintendo EU search API             — official store dates
  3. Nintendo Life confirmed games page — community table
  4. Wikipedia Switch 2 game list       — year/quarter hints
  5. HARDCODED fallbacks                — known dates from research
"""

import re
import sys
from datetime import datetime, timezone

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit(1)

NINTENDO_EU  = "https://searching.nintendo-europe.com/en/select"
NINTENDOLIFE = "https://www.nintendolife.com/guides/nintendo-switch-2-all-confirmed-games-and-release-dates"
WIKI_URL     = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def norm(t):
    import unicodedata
    t = unicodedata.normalize('NFKD', str(t)).encode('ascii', 'ignore').decode('ascii')
    t = re.sub(r'[-]', ' ', t.lower())
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return re.sub(r'\s+', ' ', t).strip()

def parse_date_str(raw):
    raw = raw.strip()
    now = datetime.now(timezone.utc)
    if not raw or raw.upper() in ("TBA", "TBD", "—", "-", ""):
        return "TBA", "u"
    # ISO date from Nintendo EU API
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
            fmt = "%b %-d, %Y" if sys.platform != "win32" else "%b %#d, %Y"
            return d.strftime(fmt), "r" if d <= now else "u"
        except: pass
    # Month Day, Year
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %-d, %Y", "%b %-d, %Y"]:
        try:
            d = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            out = "%b %-d, %Y" if sys.platform != "win32" else "%b %#d, %Y"
            return d.strftime(out), "r" if d <= now else "u"
        except: pass
    # Quarter
    m = re.match(r"Q([1-4])\s*(20\d{2})", raw, re.I)
    if m:
        return f"Q{m.group(1)} {m.group(2)}", "u"
    # Year only
    m = re.match(r"^(20[2-9]\d)$", raw)
    if m:
        return m.group(1), "u"
    return raw, "u"


# ── Source 1: Nintendo EU ────────────────────────────────────────────────────
def fetch_eu_upcoming(session):
    print("Fetching Nintendo EU upcoming dates…")
    results = {}
    try:
        params = {
            "q": "*", "rows": "300",
            "fq": "type:GAME AND system_type:nintendoswitch2*",
            "fl": "title,dates_released_dts",
        }
        r = session.get(NINTENDO_EU, params=params, timeout=10)
        r.raise_for_status()
        for doc in r.json().get("response", {}).get("docs", []):
            title = doc.get("title", "")
            date_raw = doc.get("dates_released_dts", "")
            if title and date_raw:
                date_disp, status = parse_date_str(date_raw[:10])
                if date_disp != "TBA":
                    results[norm(title)] = (title, date_disp, status)
        print(f"  {len(results)} dates from Nintendo EU")
    except Exception as e:
        print(f"  Nintendo EU: {e}")
    return results


# ── Source 2: Nintendo Life ───────────────────────────────────────────────────
def fetch_nintendolife_dates(session):
    print("Fetching Nintendo Life release dates…")
    results = {}
    try:
        r = session.get(NINTENDOLIFE, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2: continue
            hdrs = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
            ti = next((i for i,h in enumerate(hdrs) if "game" in h or "title" in h), None)
            di = next((i for i,h in enumerate(hdrs) if "date" in h or "release" in h), None)
            if ti is None or di is None: continue
            for row in rows[1:]:
                cells = row.find_all(["td","th"])
                if len(cells) <= max(ti,di): continue
                title = re.sub(r"\[.*?\]","",cells[ti].get_text(" ",strip=True)).strip()
                date_raw = cells[di].get_text(" ",strip=True)
                if not title or not date_raw or date_raw.lower() in ("tba","tbd","—","-",""): continue
                date_disp, status = parse_date_str(date_raw)
                if date_disp != "TBA":
                    results[norm(title)] = (title, date_disp, status)
        print(f"  {len(results)} dates from Nintendo Life")
    except Exception as e:
        print(f"  Nintendo Life: {e}")
    return results


# ── Source 3: Wikipedia ───────────────────────────────────────────────────────
def fetch_wiki_dates(session):
    print("Fetching Wikipedia release dates…")
    results = {}
    try:
        r = session.get(WIKI_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table", class_="wikitable"):
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td","th"])
                if len(cells) < 4: continue
                title = re.sub(r"\[.*?\]","",cells[0].get_text(" ",strip=True)).strip()
                if not title: continue
                for cell in cells[3:7]:
                    txt = re.sub(r"\[.*?\]","",cell.get_text(" ",strip=True)).strip()
                    if not txt or txt.lower() in ("tba","tbd","—","-",""): continue
                    date_disp, status = parse_date_str(txt)
                    if date_disp not in ("TBA", txt):
                        nk = norm(title)
                        if nk: results[nk] = (title, date_disp, status)
                        break
        print(f"  {len(results)} dates from Wikipedia")
    except Exception as e:
        print(f"  Wikipedia: {e}")
    return results


# ── Source 4: Hardcoded fallbacks (researched from official sources) ──────────
HARDCODED_DATES = {
    # Year-2027
    "pokemon winds and waves": ("Pokémon Winds and Waves", "2027", "u"),
    "pokemon winds": ("Pokémon Winds", "2027", "u"),
    "pokemon waves": ("Pokémon Waves", "2027", "u"),
    # Specific 2026 dates
    "lollipop chainsaw repop": ("Lollipop Chainsaw RePOP", "May 28, 2026", "u"),
    "lorelei and the laser eyes": ("Lorelei and the Laser Eyes", "Apr 22, 2026", "r"),
    "sayonara wild hearts": ("Sayonara Wild Hearts", "Apr 22, 2026", "r"),
    # 2026 windows (official sources)
    "south of midnight": ("South of Midnight", "2026", "u"),
    "marvel rivals": ("Marvel Rivals", "2026", "u"),
    "elder scrolls iv oblivion remastered": ("The Elder Scrolls IV: Oblivion Remastered", "2026", "u"),
    "stray": ("Stray", "2026", "u"),
    "final fantasy xiv": ("Final Fantasy XIV", "2026", "u"),
    "overwatch 2": ("Overwatch 2", "2026", "u"),
    "borderlands 4": ("Borderlands 4", "2026", "u"),
    "witchbrook": ("Witchbrook", "2026", "u"),
    "phasmophobia": ("Phasmophobia", "2026", "u"),
    "spine": ("Spine", "2026", "u"),
    "duskbloods": ("The Duskbloods", "2026", "u"),
    "fire emblem fortunes weave": ("Fire Emblem: Fortune's Weave", "2026", "u"),
    "splatoon raiders": ("Splatoon Raiders", "2026", "u"),
    "rhythm heaven groove": ("Rhythm Heaven Groove", "2026", "u"),
    "grand theft auto vi": ("Grand Theft Auto VI", "2026", "u"),
    "minecraft dungeons 2": ("Minecraft Dungeons 2", "2026", "u"),
    "dragon quest xii": ("Dragon Quest XII", "2026", "u"),
    "mario tennis fever": ("Mario Tennis Fever", "2026", "u"),
    "metroid prime 4": ("Metroid Prime 4: Beyond", "2026", "u"),
    "donkey kong bananza": ("Donkey Kong Bananza", "Jul 17, 2026", "u"),
}


# ── Main ──────────────────────────────────────────────────────────────────────
def get_date_map():
    """Fetch from all sources and merge. Returns {norm_title: (title, date, status)}."""
    session = requests.Session()
    session.headers.update(HEADERS)

    eu   = fetch_eu_upcoming(session)
    nl   = fetch_nintendolife_dates(session)
    wiki = fetch_wiki_dates(session)

    # Merge: EU > NL > Wiki > hardcoded
    merged = dict(HARDCODED_DATES)  # start with hardcoded as baseline
    for source in [wiki, nl, eu]:
        for nk, val in source.items():
            cur = merged.get(nk)
            if not cur:
                merged[nk] = val
            else:
                # Prefer more specific date (full date > quarter > year)
                cur_specific = bool(re.search(r"\d{1,2},\s*20\d{2}", cur[1]))
                new_specific = bool(re.search(r"\d{1,2},\s*20\d{2}", val[1]))
                if new_specific and not cur_specific:
                    merged[nk] = val

    specific = sum(1 for v in merged.values() if re.search(r"\d{1,2},\s*20\d{2}", v[1]))
    quarter  = sum(1 for v in merged.values() if re.match(r"Q[1-4]", v[1]))
    year     = sum(1 for v in merged.values() if re.match(r"^\d{4}$", v[1]))
    print(f"  Date map: {len(merged)} entries — {specific} specific, {quarter} quarter, {year} year-only")
    return merged


if __name__ == "__main__":
    dm = get_date_map()
    print("\nSample:")
    for nk, (title, date, status) in list(dm.items())[:15]:
        print(f"  [{status}] {title[:45]:45} → {date}")
