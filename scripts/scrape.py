#!/usr/bin/env python3
"""
Nintendo Switch 2 game list scraper.

Data sources (priority order):
  1. r/NSCollectors Release Details tab  — per-region Card Type + Editions
  2. r/NSCollectors Release Summary tab  — per-region release dates
  3. Wikipedia                           — publisher / developer / game type
  4. Nintendo Europe search API          — box art URLs (max 40 new per run)
  5. data/overrides.json                 — manual corrections (highest priority)
"""

import csv, io, json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path

# Nintendo Wire format scraper (separate module)
try:
    from scrape_formats import get_format_map, norm as nw_norm
    HAS_FORMAT_SCRAPER = True
except ImportError:
    HAS_FORMAT_SCRAPER = False
    def get_format_map(): return {}

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps. Run: pip install requests beautifulsoup4")
    sys.exit(1)

ROOT           = Path(__file__).parent.parent
OVERRIDES_FILE = ROOT / "data" / "overrides.json"
OUT_FILE       = ROOT / "data" / "games.json"

SHEET_ID    = "1LEIJUOanvkKq9kv1fSOnD40GdE1Jt5LzSYsg8yAPmb8"
SUMMARY_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=558942722"
DETAILS_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=764784245"
# Upcoming tabs — confirmed GIDs (discovered 2026-04-29)
UPCOMING_SUMMARY_GID = "887819792"   # "Upcoming Switch 2 Release Summary" tab
UPCOMING_GID_CANDIDATES = [UPCOMING_SUMMARY_GID]  # kept for auto-discovery fallback
WIKI_URL    = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"
NINTENDO_EU = "https://searching.nintendo-europe.com/en/select"

MAX_NEW_ART_PER_RUN = 200

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

REGION_COLS = ["USA","KOR","JPN","EUR","CHT","AUS","ASI"]

CARD_TYPE_MAP = {
    "code in box":   "b",
    "code-in-box":   "b",
    "code in a box": "b",
    "retail code":   "b",
    "game card":     "c",
    "game-key card": "k",
    "digital only":  "d",
    "digital":       "d",
}

# Normalise edition strings → canonical display names
EDITION_NORM = {
    "standard":          "Standard",
    "deluxe":            "Deluxe",
    "deluxe edition":    "Deluxe",
    "limited":           "Limited",
    "limited edition":   "Limited",
    "collector":         "Collector's",
    "collector's":       "Collector's",
    "collectors":        "Collector's",
    "collector's edition": "Collector's",
    "collectors edition":  "Collector's",
    "special":           "Special",
    "special edition":   "Special",
    "premium":           "Premium",
    "premium edition":   "Premium",
    "day one":           "Day One",
    "day one edition":   "Day One",
    "launch edition":    "Launch",
    "launch":            "Launch",
    "anniversary":       "Anniversary",
}

def parse_editions(raw):
    """'Standard, Deluxe' → ['Standard', 'Deluxe']"""
    if not raw or not raw.strip():
        return ["Standard"]
    parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    result = []
    for p in parts:
        canonical = EDITION_NORM.get(p.lower().strip(), p.strip())
        if canonical and canonical not in result:
            result.append(canonical)
    return result if result else ["Standard"]

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
    (r"final fantasy|dragon quest|octopath|bravely|persona|atelier|tales of|ys |xenoblade|elden ring|granblu|suikoden|rune factory|story of seasons|fantasy life|cyberpunk|yakuza|hogwarts", "Action RPG"),
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

def classify_type(title, pub, dev="", ns1=False):
    if "nintendo switch 2 edition" in title.lower(): return "n"
    if ns1: return "n"
    if any(n in (pub+" "+dev).lower() for n in NINTENDO_PUBS): return "e"
    return "t"

def norm(t):
    t = re.sub(r'[-\u2013\u2014]', ' ', t.lower())
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
        return d.strftime(fmt), "r" if d <= datetime.now(timezone.utc) else "u"
    except: return raw.strip(), "u"

def clean_title(t):
    t = re.sub(r'\s*[\(\[]\s*(JP|JPN|Japan|Japan Only|Asia|KOR|Korea)[^\)\]]*[\)\]]', '', t, flags=re.I)
    t = re.sub(r'^(.+),\s+(The|A|An)$', r'\2 \1', t.strip())
    return t.strip()


# ── 1. Load existing data ────────────────────────────────────────────────────
def load_existing():
    try:
        with open(OUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        games = data.get('games', [])
        art_cache = {norm(g['title']): g['art'] for g in games if g.get('art')}
        return art_cache, games
    except:
        return {}, []


# ── 2. Release Details tab (Card Type + Editions) ─────────────────────────────
def fetch_details():
    print("Fetching Release Details tab…")
    r = requests.get(DETAILS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    rows = list(csv.reader(io.StringIO(r.text)))
    hdr_idx = next((i for i, row in enumerate(rows) if 'Game Title' in row and 'Card Type' in row), None)
    if hdr_idx is None:
        raise ValueError("Header row not found in details tab")
    hdrs = rows[hdr_idx]

    ti  = hdrs.index('Game Title')
    ri  = hdrs.index('Region')
    ci  = hdrs.index('Card Type')
    pi  = next((i for i,h in enumerate(hdrs) if h.strip()=='Publisher'), None)
    ei  = next((i for i,h in enumerate(hdrs) if h.strip()=='Editions'), None)
    ni  = next((i for i,h in enumerate(hdrs) if 'NS1' in h), None)

    # details[norm_title][region_lower] = {fmt, publisher, editions, ns1}
    details = {}
    for row in rows[hdr_idx+1:]:
        if len(row) <= max(ti, ri, ci): continue
        title = row[ti].strip()
        if not title: continue
        region    = row[ri].strip().lower()
        fmt       = CARD_TYPE_MAP.get(row[ci].strip().lower(), '?')
        publisher = row[pi].strip() if pi and pi < len(row) else ''
        editions  = parse_editions(row[ei].strip() if ei and ei < len(row) else '')
        ns1       = (row[ni].strip().lower() == 'yes') if ni and ni < len(row) else False
        nk = norm(title)
        if nk not in details: details[nk] = {}
        details[nk][region] = {'fmt': fmt, 'publisher': publisher, 'editions': editions, 'ns1': ns1}

    # Collect all unique editions across all games for stats
    all_editions = set()
    for rd in details.values():
        for rv in rd.values():
            all_editions.update(rv['editions'])

    print(f"  {len(details)} unique titles · editions found: {sorted(all_editions)}")
    return details




# ── Verify/Discover Upcoming tab GID ─────────────────────────────────────────
def discover_upcoming_gids():
    """Verify the hardcoded GID works, or try to rediscover it."""
    global UPCOMING_SUMMARY_GID
    if UPCOMING_SUMMARY_GID:
        # Quick sanity check — confirm the GID still works
        try:
            url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={UPCOMING_SUMMARY_GID}"
            r = requests.get(url, headers=HEADERS, timeout=8)
            if r.ok:
                r.encoding = "utf-8"
                rows = list(csv.reader(io.StringIO(r.text)))
                hdr_idx = next((i for i, row in enumerate(rows) if "Game Title" in row), None)
                if hdr_idx is not None:
                    print(f"  Upcoming Summary GID {UPCOMING_SUMMARY_GID} verified ✓")
                    return UPCOMING_SUMMARY_GID
        except Exception:
            pass
        print(f"  ⚠ Hardcoded GID {UPCOMING_SUMMARY_GID} no longer valid — trying candidates…")
        UPCOMING_SUMMARY_GID = None

    # Fallback: brute-force discover
    for gid in ["929714088","1534533164","200000000","300000000","500000000",
                "700000000","900000000","1100000000","1300000000","1500000000"]:
        try:
            url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
            r = requests.get(url, headers=HEADERS, timeout=8)
            if not r.ok: continue
            r.encoding = "utf-8"
            rows = list(csv.reader(io.StringIO(r.text)))
            hdr_idx = next((i for i, row in enumerate(rows) if "Game Title" in row), None)
            if hdr_idx is None: continue
            has_future = any(re.search(r"202[6-9]/", cell) for row in rows[hdr_idx+1:hdr_idx+15] for cell in row)
            if has_future:
                UPCOMING_SUMMARY_GID = gid
                print(f"  Rediscovered Upcoming GID: {gid}")
                return gid
        except Exception:
            pass
    print("  Could not find Upcoming GID")
    return None


# ── Fetch Upcoming Release Summary tab ────────────────────────────────────────
def fetch_upcoming_summary():
    if not UPCOMING_SUMMARY_GID:
        return []
    print(f"Fetching Upcoming Release Summary (GID={UPCOMING_SUMMARY_GID})…")
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={UPCOMING_SUMMARY_GID}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    rows = list(csv.reader(io.StringIO(r.text)))
    hdr_idx = next((i for i, row in enumerate(rows) if "Game Title" in row), None)
    if hdr_idx is None:
        print("  Header not found in Upcoming Summary")
        return []
    headers = rows[hdr_idx]
    games = []
    for row in rows[hdr_idx+1:]:
        if not row or not row[0].strip().isdigit():
            continue
        rec = {headers[i].strip(): row[i].strip() for i in range(min(len(headers), len(row)))}
        title = clean_title(rec.get("Game Title", ""))
        if not title:
            continue
        releases, filled = {}, {}
        for col in REGION_COLS:
            val = rec.get(col, "").strip()
            if val and re.match(r"\d{4}/\d{2}/\d{2}", val):
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
        gt = rec.get("Grand Total", "").strip()
        if gt and re.match(r"\d{4}/\d{2}/\d{2}", gt):
            date, status = parse_ymd(gt)
        elif filled:
            date, status = parse_ymd(min(filled.values()))
        else:
            date, status = "TBA", "u"
        games.append({
            "title": title, "publisher": "", "developer": "", "type": "t",
            "genre": infer_genre(title), "date": date, "status": status,
            "fmt": "?", "region": region, "releases": releases,
            "formats": {}, "editions": {}, "note": "",
        })
    print(f"  {len(games)} upcoming games from Upcoming Summary tab")
    return games

# ── 3. Release Summary tab ────────────────────────────────────────────────────
def fetch_summary():
    print("Fetching Release Summary tab…")
    r = requests.get(SUMMARY_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    rows = list(csv.reader(io.StringIO(r.text)))
    hdr_idx = next((i for i, row in enumerate(rows) if "Game Title" in row), None)
    if hdr_idx is None:
        raise ValueError("Header row not found")
    headers = rows[hdr_idx]

    games = []
    for row in rows[hdr_idx+1:]:
        if not row or not row[0].strip().isdigit(): continue
        rec = {headers[i].strip(): row[i].strip() for i in range(min(len(headers),len(row)))}
        title = clean_title(rec.get("Game Title",""))
        if not title: continue

        releases, filled = {}, {}
        for col in REGION_COLS:
            val = rec.get(col,"").strip()
            if val and re.match(r'\d{4}/\d{2}/\d{2}', val):
                filled[col] = val
                disp, _ = parse_ymd(val)
                releases[col.lower()] = disp

        if not filled:                          region = "ww"
        elif set(filled.keys()) == {"JPN"}:     region = "jp"
        elif len(filled) >= 5:                  region = "ww"
        else:                                   region = "var"

        gt = rec.get("Grand Total","").strip()
        if gt and re.match(r'\d{4}/\d{2}/\d{2}', gt):
            date, status = parse_ymd(gt)
        elif filled:
            date, status = parse_ymd(min(filled.values()))
        else:
            date, status = "TBA", "u"

        games.append({"title":title,"publisher":"","developer":"","type":"t",
                      "genre":infer_genre(title),"date":date,"status":status,
                      "fmt":"?","region":region,"releases":releases,
                      "formats":{},"editions":{},"note":""})

    print(f"  {len(games)} games from summary tab")
    return games


# ── 4. Wikipedia ─────────────────────────────────────────────────────────────
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
    if not table: return []
    games = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td","th"])
        if len(cells) < 4: continue
        title = re.sub(r'\[.*?\]','', cells[0].get_text(" ",strip=True)).strip()
        dev   = cells[1].get_text(", ",strip=True)
        pub   = cells[2].get_text(", ",strip=True)
        if title:
            date_hint = ""
        for cell in cells[3:7]:
            txt = cell.get_text(" ", strip=True)
            mq = re.search(r"Q([1-4])\s*(20[2-9]\d)", txt)
            my = re.search(r"\b(20[2-9]\d)\b", txt)
            if mq:
                date_hint = f"Q{mq.group(1)} {mq.group(2)}"
                break
            elif my:
                date_hint = my.group(1)
                break
        games.append({"title":title,"publisher":pub,"developer":dev,
                      "type":classify_type(title,pub,dev),"genre":infer_genre(title,pub),
                      "date_hint": date_hint})
    print(f"  {len(games)} games from Wikipedia")
    return games


# ── 5. Box art ────────────────────────────────────────────────────────────────
NINTENDO_EU_S2 = "https://searching.nintendo-europe.com/en/select"
NINTENDO_JP    = "https://search.nintendo.jp/nintendo_soft/search.json"

def clean_search_title(title):
    """Strip platform/edition tags to improve search matching."""
    t = re.sub(r'\s*[-–]?\s*nintendo switch 2 edition[-–]?\s*', ' ', title, flags=re.I)
    t = re.sub(r'\s*[-–]?\s*ns2 edition[-–]?\s*', ' ', t, flags=re.I)
    t = re.sub(r'\s*[\-\(\[].*?[\-\)\]]\s*', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()

def title_match(found, query, threshold=0.45):
    """Check if found title is a reasonable match for query."""
    fn = re.sub(r'[^a-z0-9 ]', '', found.lower())
    qn = re.sub(r'[^a-z0-9 ]', '', query.lower())
    fw = set(w for w in fn.split() if len(w) > 2)
    qw = [w for w in qn.split() if len(w) > 2]
    if not qw or not fw: return False
    return sum(1 for w in qw if w in fw) / len(qw) >= threshold

def fetch_art_nintendo_eu(title, session, switch2_only=True):
    """Fetch art from Nintendo Europe search API."""
    try:
        # Try Switch 2 specific first, then broaden
        fq = "type:GAME AND system_type:nintendoswitch2*" if switch2_only else "type:GAME AND system_type:nintendoswitch*"
        params = {"q": title, "fq": fq, "rows": "3",
                  "fl": "image_url_h2x1_s,image_url_sq_s,title,system_type"}
        r = session.get(NINTENDO_EU_S2, params=params, timeout=5)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        for doc in docs:
            if title_match(doc.get("title", ""), title):
                url = doc.get("image_url_h2x1_s") or doc.get("image_url_sq_s")
                if url: return url
    except: pass
    return None

def fetch_art_nintendo_jp(title, session):
    """Fetch art from Nintendo Japan search API (for JP-exclusive titles)."""
    try:
        # Search by English title — JP store indexes in romaji too
        params = {"q": title, "hard": "05_BEE", "limit": "3"}
        r = session.get(NINTENDO_JP, params=params, timeout=5)
        r.raise_for_status()
        items = r.json().get("result", {}).get("items", [])
        for item in items:
            item_title = item.get("title", "")
            if title_match(item_title, title, threshold=0.35):
                iurl = item.get("iurl")
                if iurl:
                    # Nintendo JP CDN image URL format
                    return f"https://img-eshop.cdn.nintendo.net/i/{iurl}.jpg"
    except: pass
    return None

def fetch_art_wikipedia(title, session):
    """Fetch thumbnail from Wikipedia as last resort."""
    try:
        r = session.get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(title)}",
            timeout=4
        )
        if r.ok:
            thumb = r.json().get("thumbnail", {}).get("source")
            if thumb and "logo" not in thumb.lower(): return thumb
    except: pass
    return None

def fetch_art_batch(titles_needing_art, session):
    results = {}
    batch = list(titles_needing_art)[:MAX_NEW_ART_PER_RUN]
    print(f"  Fetching art for {len(batch)} new titles (of {len(titles_needing_art)} needed)…")
    found_count = 0

    for title in batch:
        clean = clean_search_title(title)
        url = None

        # 1. Nintendo EU — Switch 2 specific
        url = fetch_art_nintendo_eu(clean, session, switch2_only=True)

        # 2. Nintendo EU — all Switch (gets the Switch 1 art as fallback, still looks good)
        if not url:
            url = fetch_art_nintendo_eu(clean, session, switch2_only=False)

        # 3. Nintendo EU — with original (unclean) title
        if not url and clean != title:
            url = fetch_art_nintendo_eu(title, session, switch2_only=False)

        # 4. Nintendo JP (for Japan-exclusive titles)
        if not url:
            url = fetch_art_nintendo_jp(clean, session)

        # 5. Wikipedia thumbnail (last resort)
        if not url:
            url = fetch_art_wikipedia(clean, session)

        results[title] = url
        if url: found_count += 1
        time.sleep(0.12)

    print(f"  Found art for {found_count}/{len(batch)} titles")
    return results


# ── 6. Overrides ─────────────────────────────────────────────────────────────
def load_overrides():
    if not OVERRIDES_FILE.exists(): return {}
    with open(OVERRIDES_FILE, encoding="utf-8") as f:
        d = json.load(f)
    d = {k:v for k,v in d.items() if not k.startswith("_")}
    print(f"  {len(d)} overrides loaded")
    return d


# ── 7. Merge ──────────────────────────────────────────────────────────────────
def merge_all(summary, wiki, details, overrides, art_cache, nw_formats=None):
    wiki_map = {norm(g["title"]): g for g in wiki}
    summary_norms = {norm(g["title"]) for g in summary}
    session = requests.Session()
    session.headers.update(HEADERS)
    titles_needing_art = []
    merged = []

    for g in summary:
        g = dict(g)
        nk = norm(g["title"])

        # Details tab: formats + editions per region
        dk = fuzzy(g["title"], {k:k for k in details})
        if dk:
            rd = details[dk]
            g['formats']  = {r: v['fmt']      for r,v in rd.items()}
            g['editions'] = {r: v['editions'] for r,v in rd.items()}
            fmts = list(g['formats'].values())
            g['fmt'] = 'c' if 'c' in fmts else ('k' if 'k' in fmts else (fmts[0] if fmts else '?'))
            if not g['publisher']:
                for pref in ['usa','eur','aus','jpn','kor','cht','asi']:
                    if pref in rd and rd[pref]['publisher']:
                        g['publisher'] = rd[pref]['publisher']; break
            if any(v['ns1'] for v in rd.values()):
                g['type'] = 'n'

        # Wikipedia
        wk = fuzzy(g["title"], wiki_map)
        if wk:
            wg = wiki_map[wk]
            if not g["publisher"]: g["publisher"] = wg["publisher"]
            g["developer"] = wg["developer"]
            if g["type"] == "t" and wg["type"] != "t": g["type"] = wg["type"]
            if g["genre"] == "Action": g["genre"] = wg["genre"]
            if g.get("date") == "TBA" and wg.get("date_hint"):
                g["date"] = wg["date_hint"]

        # Nintendo Wire format (high-priority factual source)
        if nw_formats:
            nwk = fuzzy(g["title"], {k:k for k in nw_formats})
            if nwk:
                _, nw_fmt = nw_formats[nwk]
                # Only apply if not already set by NSCollectors details tab (which has per-region data)
                if not g.get('formats') or len(g['formats']) == 0:
                    g['fmt'] = nw_fmt
                elif nw_fmt in ('b', 'c') and g.get('fmt') not in ('b', 'c'):
                    # NW says CiB or Cart but we have GKC — NW is more specific, trust it
                    g['fmt'] = nw_fmt

        # Overrides (highest priority — manual corrections override everything)
        ok = fuzzy(g["title"], overrides)
        if ok:
            for k,v in overrides[ok].items():
                if not k.startswith("_"): g[k] = v

        # Art
        if nk in art_cache:   g['art'] = art_cache[nk]
        else:                  g['art'] = None; titles_needing_art.append(g["title"])

        merged.append(g)

    # Wiki-only additions
    for wg in wiki:
        if norm(wg["title"]) not in summary_norms:
            g = {"title":wg["title"],"publisher":wg["publisher"],"developer":wg["developer"],
                 "type":wg["type"],"genre":wg["genre"],"date":"TBA","status":"u",
                 "fmt":"?","region":"ww","releases":{},"formats":{},"editions":{},"note":"","art":None}
            ok = fuzzy(g["title"], overrides)
            if ok:
                for k,v in overrides[ok].items():
                    if not k.startswith("_"): g[k] = v
            # Apply NW format for wiki-only games too
            if nw_formats:
                nwk = fuzzy(g["title"], {k:k for k in nw_formats})
                if nwk and g.get('fmt') == '?':
                    _, nw_fmt = nw_formats[nwk]
                    g['fmt'] = nw_fmt
            nk = norm(g["title"])
            if nk in art_cache: g['art'] = art_cache[nk]
            else: titles_needing_art.append(g["title"])
            merged.append(g)

    # Fetch art
    if titles_needing_art:
        new_art = fetch_art_batch(titles_needing_art, session)
        title_map = {g["title"]: g for g in merged}
        for title, url in new_art.items():
            if title in title_map:
                title_map[title]['art'] = url

    return merged


def assign_ids(games):
    for i, g in enumerate(sorted(games, key=lambda x: x["title"].lower()), start=1):
        g["id"] = i
    return games


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    art_cache, existing_games = load_existing()
    print(f"  {len(art_cache)} art URLs cached · {len(existing_games)} existing games")

    try:
        discover_upcoming_gids()
    except Exception as e:
        print(f"  GID discovery: {e}")

    try:
        details = fetch_details()
    except Exception as e:
        print(f"  ⚠ Details tab failed ({e})")
        details = {}

    try:
        summary = fetch_summary()
    except Exception as e:
        print(f"  ⚠ Summary tab failed ({e})")
        summary = []

    try:
        upcoming_extra = fetch_upcoming_summary()
        existing_norms = {norm(g["title"]) for g in summary}
        new_upcoming = [g for g in upcoming_extra if norm(g["title"]) not in existing_norms]
        print(f"  {len(new_upcoming)} new titles from Upcoming tab")
        summary = summary + new_upcoming
    except Exception as e:
        print(f"  ⚠ Upcoming fetch failed ({e})")

    wiki      = fetch_wiki()
    overrides = load_overrides()
    # Fetch Nintendo Wire format map (Playwright-based, most accurate)
    print("Fetching Nintendo Wire format data…")
    nw_formats = get_format_map()
    print(f"  {len(nw_formats)} games with confirmed format from Nintendo Wire")

    games     = merge_all(summary, wiki, details, overrides, art_cache, nw_formats)
    games     = assign_ids(games)

    if len(games) == 0:
        if existing_games:
            print(f"  ⚠ All sources failed — keeping existing {len(existing_games)} games")
            sys.exit(0)
        else:
            print("  ⚠ No games found and no existing data")
            sys.exit(1)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "sources": {"summary": SUMMARY_URL, "details": DETAILS_URL, "wikipedia": WIKI_URL},
        "count": len(games),
        "games": games,
    }
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    released  = sum(1 for g in games if g["status"]=="r")
    upcoming  = sum(1 for g in games if g["status"]=="u")
    with_art  = sum(1 for g in games if g.get("art"))
    with_ed   = sum(1 for g in games if any(len(v)>1 for v in g.get("editions",{}).values()))
    print(f"\n✓ {len(games)} games → {OUT_FILE}")
    print(f"  Released: {released}  Upcoming: {upcoming}")
    print(f"  With art: {with_art}  With multiple editions: {with_ed}")

if __name__ == "__main__":
    main()
