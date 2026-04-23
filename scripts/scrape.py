#!/usr/bin/env python3
"""
Nintendo Switch 2 game list scraper.

Data sources (priority order):
  1. r/NSCollectors Google Sheet  — primary: accurate titles + per-region release dates
  2. Wikipedia                    — secondary: publisher / format type
  3. data/overrides.json          — manual corrections that override everything

Run: python scripts/scrape.py
"""

import csv, io, json, re, sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests beautifulsoup4")
    sys.exit(1)

ROOT           = Path(__file__).parent.parent
OVERRIDES_FILE = ROOT / "data" / "overrides.json"
OUT_FILE       = ROOT / "data" / "games.json"

SHEET_ID  = "1LEIJUOanvkKq9kv1fSOnD40GdE1Jt5LzSYsg8yAPmb8"
SHEET_GID = "558942722"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
WIKI_URL  = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

REGION_COLS = ["USA","KOR","JPN","EUR","CHT","AUS","ASI"]
NINTENDO_PUBS = {"nintendo","nintendo epd","hal laboratory","retro studios",
                 "intelligent systems","game freak","the pokemon company",
                 "camelot software planning","monolith soft","nd cube"}

GENRE_HINTS = [
    (r"kart|racing|moto|sonic racing|fast fusion", "Racing"),
    (r"mario party|jamboree", "Party"),
    (r"zelda|hyrule warriors", "Action-Adventure"),
    (r"pokemon|pok.mon", "RPG"),
    (r"kirby|wonder|bananza|yoshi|yooka|pac.man|mega man|hollow knight|silksong|platformer", "Platformer"),
    (r"fire emblem|disgaea|tactics|nobunaga|dynasty warriors|duskbloods|brigandine", "Strategy RPG"),
    (r"drag.x.drive|tennis|madden|nba|nfl|pga|fc 2[56]|football|basketball|hockey|golf", "Sports"),
    (r"metroid|resident evil|fatal frame|project zero|nightmare|phasmophobia|cronos|layers of fear", "Horror"),
    (r"final fantasy|dragon quest|octopath|bravely|persona|atelier|tales of|ys |xenoblade|elden ring|granblue|suikoden|rune factory|story of seasons|fantasy life|cyberpunk|yakuza|raidou|borderlands|fallout|hogwarts", "Action RPG"),
    (r"animal crossing|tomodachi|my time|evershine|farming|dave the diver|no man|pokopia", "Life Sim"),
    (r"hades|enter the gungeon|balatro|roguelike", "Action Roguelike"),
    (r"street fighter|mortal kombat|dragon ball|fighting|virtua fighter", "Fighting"),
    (r"hitman|assassin|star wars|indiana jones|batman|lego|007", "Action-Adventure"),
    (r"overwatch|apex|fortnite|battle royale", "Battle Royale"),
    (r"a.train|simulator|farming|powerwash|goat sim|factorio", "Simulation"),
    (r"overcooked|cook|civilization", "Strategy"),
    (r"layton|puzzle|puyo|tetris|chromagun", "Puzzle"),
]

def infer_genre(title, pub=""):
    s = (title + " " + pub).lower()
    for pat, g in GENRE_HINTS:
        if re.search(pat, s): return g
    return "Action"

def classify_type(title, pub, dev=""):
    if "nintendo switch 2 edition" in title.lower(): return "n"
    if any(n in (pub+" "+dev).lower() for n in NINTENDO_PUBS): return "e"
    return "t"

def norm(title):
    t = re.sub(r'[-\u2013\u2014]', ' ', title.lower())
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return re.sub(r'\s+', ' ', t).strip()

def fuzzy(needle, mapping):
    nn = norm(needle)
    for k in mapping:
        nk = norm(k)
        if nn == nk or (len(nn) > 8 and (nn in nk or nk in nn)):
            return k
    return None

def parse_ymd(raw):
    try:
        d = datetime.strptime(raw.strip(), "%Y/%m/%d").replace(tzinfo=timezone.utc)
        fmt = "%b %-d, %Y" if sys.platform != "win32" else "%b %#d, %Y"
        now = datetime.now(timezone.utc)
        return d.strftime(fmt), "r" if d <= now else "u"
    except: return raw.strip(), "u"

def clean_title(t):
    t = re.sub(r'\s*[\(\[]\s*(JP|JPN|Japan|Japan Only|Asia|KOR|Korea)[^\)\]]*[\)\]]', '', t, flags=re.I)
    t = re.sub(r'^(.+),\s+(The|A|An)$', r'\2 \1', t.strip())
    return t.strip()

# ── 1. Google Sheet ───────────────────────────────────────────────────────────
def fetch_sheet():
    print("Fetching r/NSCollectors sheet …")
    r = requests.get(SHEET_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))

    # Find header row
    hdr_idx = next((i for i,row in enumerate(rows) if "Game Title" in row), None)
    if hdr_idx is None: raise ValueError("Header row not found in sheet")
    headers = rows[hdr_idx]
    print(f"  Headers: {headers}")

    games = []
    for row in rows[hdr_idx+1:]:
        if not row or not row[0].strip().isdigit(): continue
        rec = {headers[i].strip(): row[i].strip() for i in range(min(len(headers),len(row)))}

        title = clean_title(rec.get("Game Title",""))
        if not title: continue

        # Per-region dates
        releases = {}
        filled = {}
        for col in REGION_COLS:
            val = rec.get(col,"").strip()
            if val and re.match(r'\d{4}/\d{2}/\d{2}', val):
                filled[col] = val
                disp, _ = parse_ymd(val)
                releases[col.lower()] = disp

        # Region classification
        if not filled:
            region = "ww"
        elif set(filled.keys()) == {"JPN"}:
            region = "jp"
        elif len(filled) >= 5:
            region = "ww"
        else:
            region = "var"

        # Best display date = Grand Total or earliest filled
        gt = rec.get("Grand Total","").strip()
        if gt and re.match(r'\d{4}/\d{2}/\d{2}', gt):
            date, status = parse_ymd(gt)
        elif filled:
            earliest_raw = min(filled.values())
            date, status = parse_ymd(earliest_raw)
        else:
            date, status = "TBA", "u"

        games.append({
            "title": title, "publisher":"", "developer":"",
            "type":"t", "genre": infer_genre(title),
            "date": date, "status": status,
            "fmt":"?", "region": region,
            "releases": releases, "note":"",
        })

    print(f"  {len(games)} games from sheet")
    return games

# ── 2. Wikipedia ──────────────────────────────────────────────────────────────
def fetch_wiki():
    print("Fetching Wikipedia …")
    try:
        r = requests.get(WIKI_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  ⚠ Wikipedia failed: {e}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="wikitable")
    if not table:
        print("  ⚠ wikitable not found")
        return []
    games = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td","th"])
        if len(cells) < 4: continue
        title = re.sub(r'\[.*?\]','', cells[0].get_text(" ",strip=True)).strip()
        dev   = cells[1].get_text(", ",strip=True)
        pub   = cells[2].get_text(", ",strip=True)
        if not title: continue
        games.append({"title":title,"publisher":pub,"developer":dev,
                      "type":classify_type(title,pub,dev),
                      "genre":infer_genre(title,pub)})
    print(f"  {len(games)} games from Wikipedia")
    return games

# ── 3. Overrides ──────────────────────────────────────────────────────────────
def load_overrides():
    if not OVERRIDES_FILE.exists(): return {}
    with open(OVERRIDES_FILE, encoding="utf-8") as f:
        d = json.load(f)
    d = {k:v for k,v in d.items() if not k.startswith("_")}
    print(f"  {len(d)} overrides loaded")
    return d

# ── 4. Merge ──────────────────────────────────────────────────────────────────
def merge_all(sheet, wiki, overrides):
    wiki_map = {norm(g["title"]): g for g in wiki}
    sheet_norms = {norm(g["title"]) for g in sheet}
    merged = []

    for g in sheet:
        g = dict(g)
        # Merge Wikipedia metadata
        wk = fuzzy(g["title"], wiki_map)
        if wk:
            wg = wiki_map[wk]
            g["publisher"] = wg["publisher"]
            g["developer"] = wg["developer"]
            g["type"]      = wg["type"]
            if g["genre"] == "Action":
                g["genre"] = wg["genre"]
        # Apply overrides (highest priority)
        ok = fuzzy(g["title"], overrides)
        if ok:
            for k,v in overrides[ok].items():
                if not k.startswith("_"): g[k] = v
        merged.append(g)

    # Add Wikipedia-only games (not yet in sheet — future unannounced dates)
    for wg in wiki:
        if norm(wg["title"]) not in sheet_norms:
            g = {"title":wg["title"],"publisher":wg["publisher"],"developer":wg["developer"],
                 "type":wg["type"],"genre":wg["genre"],"date":"TBA","status":"u",
                 "fmt":"?","region":"ww","releases":{},"note":""}
            ok = fuzzy(g["title"], overrides)
            if ok:
                for k,v in overrides[ok].items():
                    if not k.startswith("_"): g[k] = v
            merged.append(g)

    return merged

def assign_ids(games):
    for i, g in enumerate(sorted(games, key=lambda x: x["title"].lower()), start=1):
        g["id"] = i
    return games

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    try:
        sheet = fetch_sheet()
    except Exception as e:
        print(f'  ⚠ Sheet fetch failed ({e}), falling back to Wikipedia only')
        sheet = []
    wiki  = fetch_wiki()
    overrides = load_overrides()
    games = merge_all(sheet, wiki, overrides)
    games = assign_ids(games)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "sources": {"sheet": SHEET_URL, "wikipedia": WIKI_URL},
        "count": len(games),
        "games": games,
    }
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(out,f,ensure_ascii=False,indent=2)

    released = sum(1 for g in games if g["status"]=="r")
    upcoming = sum(1 for g in games if g["status"]=="u")
    print(f"\n✓ {len(games)} games → {OUT_FILE}")
    print(f"  Released: {released}  Upcoming: {upcoming}")
    print(f"  Sheet: {len(sheet)}  Wiki-only additions: {len(games)-len(sheet)}")

if __name__ == "__main__":
    main()
