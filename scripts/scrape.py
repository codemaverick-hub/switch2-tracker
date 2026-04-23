#!/usr/bin/env python3
"""
Nintendo Switch 2 game list scraper.

Data sources (priority order):
  1. r/NSCollectors Release Details tab  — per-region Card Type (Game Card / GKC)
  2. r/NSCollectors Release Summary tab  — per-region release dates
  3. Wikipedia                           — publisher / developer / game type
  4. Nintendo Europe search API          — box art URLs
  5. data/overrides.json                 — manual corrections (highest priority)

Run: python scripts/scrape.py
"""

import csv, io, json, re, sys, time
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

SHEET_ID      = "1LEIJUOanvkKq9kv1fSOnD40GdE1Jt5LzSYsg8yAPmb8"
SUMMARY_GID   = "558942722"
DETAILS_GID   = "764784245"
SUMMARY_URL   = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SUMMARY_GID}"
DETAILS_URL   = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={DETAILS_GID}"
WIKI_URL      = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"
NINTENDO_EU   = "https://searching.nintendo-europe.com/en/select"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

# Region mapping: sheet column name → our key
REGION_COLS = ["USA","KOR","JPN","EUR","CHT","AUS","ASI"]
REGION_KEY  = {r: r.lower() for r in REGION_COLS}

# Card Type → format code
CARD_TYPE_MAP = {
    "game card":      "c",
    "game-key card":  "k",
    "digital only":   "d",
    "digital":        "d",
}

NINTENDO_PUBS = {"nintendo","nintendo epd","hal laboratory","retro studios",
                 "intelligent systems","game freak","the pokemon company",
                 "camelot software planning","monolith soft","nd cube"}

GENRE_HINTS = [
    (r"kart|racing|moto|sonic racing|fast fusion", "Racing"),
    (r"mario party|jamboree", "Party"),
    (r"zelda|hyrule warriors", "Action-Adventure"),
    (r"pokemon|pok.mon", "RPG"),
    (r"kirby|wonder|bananza|yoshi|yokai|pac.man|mega man|hollow knight|silksong|platformer", "Platformer"),
    (r"fire emblem|disgaea|tactics|nobunaga|dynasty warriors|duskbloods|brigandine", "Strategy RPG"),
    (r"drag.x.drive|tennis|madden|nba|nfl|pga|fc 2[56]|football|basketball|hockey|golf", "Sports"),
    (r"metroid|resident evil|fatal frame|project zero|nightmare|phasmophobia|cronos|layers of fear", "Horror"),
    (r"final fantasy|dragon quest|octopath|bravely|persona|atelier|tales of|ys |xenoblade|elden ring|granblu|suikoden|rune factory|story of seasons|fantasy life|cyberpunk|yakuza|raidou|borderlands|fallout|hogwarts", "Action RPG"),
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

def classify_type(title, pub, dev="", ns1_compatible=False):
    if "nintendo switch 2 edition" in title.lower(): return "n"
    if ns1_compatible: return "n"
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


# ── 1. Release Details tab (per-region Card Type) ────────────────────────────
def fetch_details():
    print("Fetching Release Details tab (Card Types)…")
    r = requests.get(DETAILS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))

    hdr_idx = next((i for i, row in enumerate(rows) if 'Game Title' in row and 'Card Type' in row), None)
    if hdr_idx is None:
        raise ValueError("Header row not found in details tab")
    headers = rows[hdr_idx]

    ti = headers.index('Game Title')
    ri = headers.index('Region')
    ci = headers.index('Card Type')
    pi = next((i for i, h in enumerate(headers) if h.strip() == 'Publisher'), None)
    ni = next((i for i, h in enumerate(headers) if 'NS1' in h), None)

    # details[norm_title][region_lower] = {fmt, publisher, ns1}
    details = {}
    for row in rows[hdr_idx+1:]:
        if len(row) <= max(ti, ri, ci): continue
        title = row[ti].strip()
        if not title: continue
        region = row[ri].strip().lower()
        card_raw = row[ci].strip().lower()
        fmt = CARD_TYPE_MAP.get(card_raw, '?')
        publisher = row[pi].strip() if pi and pi < len(row) else ''
        ns1 = (row[ni].strip().lower() == 'yes') if ni and ni < len(row) else False
        nk = norm(title)
        if nk not in details:
            details[nk] = {}
        details[nk][region] = {'fmt': fmt, 'publisher': publisher, 'ns1': ns1}

    print(f"  {len(details)} unique games in details tab")
    return details


# ── 2. Release Summary tab (regional dates) ──────────────────────────────────
def fetch_summary():
    print("Fetching Release Summary tab (dates)…")
    r = requests.get(SUMMARY_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))

    hdr_idx = next((i for i, row in enumerate(rows) if "Game Title" in row), None)
    if hdr_idx is None:
        raise ValueError("Header row not found in summary tab")
    headers = rows[hdr_idx]

    games = []
    for row in rows[hdr_idx+1:]:
        if not row or not row[0].strip().isdigit(): continue
        rec = {headers[i].strip(): row[i].strip() for i in range(min(len(headers), len(row)))}
        title = clean_title(rec.get("Game Title", ""))
        if not title: continue

        releases = {}
        filled = {}
        for col in REGION_COLS:
            val = rec.get(col, "").strip()
            if val and re.match(r'\d{4}/\d{2}/\d{2}', val):
                filled[col] = val
                disp, _ = parse_ymd(val)
                releases[col.lower()] = disp

        if not filled:
            region = "ww"
        elif set(filled.keys()) == {"JPN"}:
            region = "jp"
        elif len(filled) >= 5:
            region = "ww"
        else:
            region = "var"

        gt = rec.get("Grand Total","").strip()
        if gt and re.match(r'\d{4}/\d{2}/\d{2}', gt):
            date, status = parse_ymd(gt)
        elif filled:
            date, status = parse_ymd(min(filled.values()))
        else:
            date, status = "TBA", "u"

        games.append({
            "title": title, "publisher": "", "developer": "",
            "type": "t", "genre": infer_genre(title),
            "date": date, "status": status,
            "fmt": "?", "region": region, "releases": releases, "note": "",
        })

    print(f"  {len(games)} games from summary tab")
    return games


# ── 3. Wikipedia ─────────────────────────────────────────────────────────────
def fetch_wiki():
    print("Fetching Wikipedia…")
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
                      "type":classify_type(title,pub,dev),"genre":infer_genre(title,pub)})
    print(f"  {len(games)} games from Wikipedia")
    return games


# ── 4. Box art from Nintendo EU search ───────────────────────────────────────
def fetch_art_url(title, session, existing_art=None):
    """Fetch art URL from Nintendo EU search API. Returns URL or None."""
    if existing_art is not None:
        return existing_art  # reuse cached URL
    try:
        params = {
            "q": title,
            "fq": "type:GAME AND system_type:nintendoswitch*",
            "rows": "1",
            "fl": "image_url_sq_s,image_url_h2x1_s,title"
        }
        r = session.get(NINTENDO_EU, params=params, timeout=8)
        r.raise_for_status()
        d = r.json()
        docs = d.get("response", {}).get("docs", [])
        if not docs:
            return None
        doc = docs[0]
        # Verify the title is a reasonable match
        found = re.sub(r'[^a-z0-9 ]', '', (doc.get("title") or "").lower())
        query = re.sub(r'[^a-z0-9 ]', '', title.lower())
        fw = set(w for w in found.split() if len(w) > 2)
        qw = [w for w in query.split() if len(w) > 2]
        if qw and fw and sum(1 for w in qw if w in fw) / len(qw) >= 0.5:
            return doc.get("image_url_h2x1_s") or doc.get("image_url_sq_s")
    except:
        pass
    return None


# ── 5. Overrides ─────────────────────────────────────────────────────────────
def load_overrides():
    if not OVERRIDES_FILE.exists(): return {}
    with open(OVERRIDES_FILE, encoding="utf-8") as f:
        d = json.load(f)
    d = {k: v for k, v in d.items() if not k.startswith("_")}
    print(f"  {len(d)} overrides loaded")
    return d


# ── 6. Load existing art cache ────────────────────────────────────────────────
def load_existing_art():
    """Reuse art URLs from previous run to avoid re-fetching."""
    try:
        with open(OUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {norm(g['title']): g.get('art') for g in data.get('games', []) if g.get('art')}
    except:
        return {}


# ── 7. Merge all sources ──────────────────────────────────────────────────────
def merge_all(summary, wiki, details, overrides, art_cache):
    wiki_map = {norm(g["title"]): g for g in wiki}
    summary_norms = {norm(g["title"]) for g in summary}
    merged = []

    session = requests.Session()
    session.headers.update(HEADERS)

    for g in summary:
        g = dict(g)
        nk = norm(g["title"])

        # ── Merge details tab data (per-region card type) ──
        dk = fuzzy(g["title"], {k: k for k in details})
        if dk:
            region_data = details[dk]  # {region_lower: {fmt, publisher, ns1}}
            # Build formats dict
            formats = {r: v['fmt'] for r, v in region_data.items()}
            g['formats'] = formats
            # Determine best overall format
            fmt_vals = list(formats.values())
            if 'c' in fmt_vals:
                g['fmt'] = 'c'
            elif 'k' in fmt_vals:
                g['fmt'] = 'k'
            elif fmt_vals:
                g['fmt'] = fmt_vals[0]
            # Get publisher from details if not set
            if not g['publisher']:
                # Use USA publisher, or first available
                for pref_r in ['usa', 'eur', 'aus', 'jpn', 'kor', 'cht', 'asi']:
                    if pref_r in region_data and region_data[pref_r]['publisher']:
                        g['publisher'] = region_data[pref_r]['publisher']
                        break
            # NS1 compatible → NS2 Edition type
            if any(v['ns1'] for v in region_data.values()):
                g['type'] = 'n'

        # ── Merge Wikipedia ──
        wk = fuzzy(g["title"], wiki_map)
        if wk:
            wg = wiki_map[wk]
            if not g["publisher"]:
                g["publisher"] = wg["publisher"]
            g["developer"] = wg["developer"]
            if g["type"] == "t" and wg["type"] != "t":
                g["type"] = wg["type"]
            if g["genre"] == "Action":
                g["genre"] = wg["genre"]

        # ── Apply overrides ──
        ok = fuzzy(g["title"], overrides)
        if ok:
            for k, v in overrides[ok].items():
                if not k.startswith("_"): g[k] = v

        # ── Art URL ──
        existing = art_cache.get(nk)
        if existing:
            g['art'] = existing
        else:
            time.sleep(0.08)
            url = fetch_art_url(g["title"], session)
            g['art'] = url

        merged.append(g)

    # Add Wikipedia-only games (no date info yet)
    for wg in wiki:
        if norm(wg["title"]) not in summary_norms:
            g = {"title":wg["title"],"publisher":wg["publisher"],"developer":wg["developer"],
                 "type":wg["type"],"genre":wg["genre"],"date":"TBA","status":"u",
                 "fmt":"?","region":"ww","releases":{},"formats":{},"note":"","art":None}
            ok = fuzzy(g["title"], overrides)
            if ok:
                for k, v in overrides[ok].items():
                    if not k.startswith("_"): g[k] = v
            if not g['art']:
                nk = norm(g['title'])
                existing = art_cache.get(nk)
                if existing:
                    g['art'] = existing
                else:
                    time.sleep(0.08)
                    g['art'] = fetch_art_url(g["title"], session)
            merged.append(g)

    return merged

def assign_ids(games):
    for i, g in enumerate(sorted(games, key=lambda x: x["title"].lower()), start=1):
        g["id"] = i
    return games


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    art_cache = load_existing_art()
    print(f"  {len(art_cache)} art URLs cached from previous run")

    try:
        details = fetch_details()
    except Exception as e:
        print(f'  ⚠ Details tab failed ({e}), skipping per-region formats')
        details = {}

    try:
        summary = fetch_summary()
    except Exception as e:
        print(f'  ⚠ Summary tab failed ({e}), falling back to Wikipedia only')
        summary = []

    wiki      = fetch_wiki()
    overrides = load_overrides()
    games     = merge_all(summary, wiki, details, overrides, art_cache)
    games     = assign_ids(games)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "sources": {"summary": SUMMARY_URL, "details": DETAILS_URL, "wikipedia": WIKI_URL},
        "count": len(games),
        "games": games,
    }
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    released = sum(1 for g in games if g["status"]=="r")
    upcoming = sum(1 for g in games if g["status"]=="u")
    with_art  = sum(1 for g in games if g.get("art"))
    print(f"\n✓ {len(games)} games → {OUT_FILE}")
    print(f"  Released: {released}  Upcoming: {upcoming}  With art: {with_art}")
    print(f"  From details: {len(details)}  From summary: {len(summary)}")

if __name__ == "__main__":
    main()
