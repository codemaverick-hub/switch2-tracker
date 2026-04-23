#!/usr/bin/env python3
"""
Nintendo Switch 2 game list scraper.

Data sources (priority order):
  1. r/NSCollectors Release Details tab  — per-region Card Type (Game Card / GKC)
  2. r/NSCollectors Release Summary tab  — per-region release dates
  3. Wikipedia                           — publisher / developer / game type
  4. Nintendo Europe search API          — box art URLs (max 40 new per run)
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

SHEET_ID    = "1LEIJUOanvkKq9kv1fSOnD40GdE1Jt5LzSYsg8yAPmb8"
SUMMARY_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=558942722"
DETAILS_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=764784245"
WIKI_URL    = "https://en.wikipedia.org/wiki/List_of_Nintendo_Switch_2_games"
NINTENDO_EU = "https://searching.nintendo-europe.com/en/select"

# Max new art fetches per run (keeps the workflow fast; existing cache is reused)
MAX_NEW_ART_PER_RUN = 40

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

REGION_COLS = ["USA","KOR","JPN","EUR","CHT","AUS","ASI"]

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


# ── 1. Load existing data (for art cache + safety fallback) ─────────────────
def load_existing():
    """Returns (art_cache, existing_games). existing_games used as fallback if scrape fails."""
    try:
        with open(OUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        games = data.get('games', [])
        art_cache = {norm(g['title']): g['art'] for g in games if g.get('art')}
        return art_cache, games
    except:
        return {}, []


# ── 2. Release Details tab (per-region Card Type) ────────────────────────────
def fetch_details():
    print("Fetching Release Details tab…")
    r = requests.get(DETAILS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.text)))
    hdr_idx = next((i for i, row in enumerate(rows) if 'Game Title' in row and 'Card Type' in row), None)
    if hdr_idx is None:
        raise ValueError("Header row not found in details tab")
    hdrs = rows[hdr_idx]
    ti = hdrs.index('Game Title')
    ri = hdrs.index('Region')
    ci = hdrs.index('Card Type')
    pi = next((i for i,h in enumerate(hdrs) if h.strip()=='Publisher'), None)
    ni = next((i for i,h in enumerate(hdrs) if 'NS1' in h), None)

    details = {}
    for row in rows[hdr_idx+1:]:
        if len(row) <= max(ti, ri, ci): continue
        title = row[ti].strip()
        if not title: continue
        region = row[ri].strip().lower()
        fmt = CARD_TYPE_MAP.get(row[ci].strip().lower(), '?')
        publisher = row[pi].strip() if pi and pi < len(row) else ''
        ns1 = (row[ni].strip().lower() == 'yes') if ni and ni < len(row) else False
        nk = norm(title)
        if nk not in details: details[nk] = {}
        details[nk][region] = {'fmt': fmt, 'publisher': publisher, 'ns1': ns1}

    print(f"  {len(details)} unique titles in details tab")
    return details


# ── 3. Release Summary tab (regional dates) ──────────────────────────────────
def fetch_summary():
    print("Fetching Release Summary tab…")
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

        if not filled:                             region = "ww"
        elif set(filled.keys()) == {"JPN"}:        region = "jp"
        elif len(filled) >= 5:                     region = "ww"
        else:                                      region = "var"

        gt = rec.get("Grand Total","").strip()
        if gt and re.match(r'\d{4}/\d{2}/\d{2}', gt):
            date, status = parse_ymd(gt)
        elif filled:
            date, status = parse_ymd(min(filled.values()))
        else:
            date, status = "TBA", "u"

        games.append({"title":title,"publisher":"","developer":"","type":"t",
                      "genre":infer_genre(title),"date":date,"status":status,
                      "fmt":"?","region":region,"releases":releases,"formats":{},"note":""})

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
        dev, pub = cells[1].get_text(", ",strip=True), cells[2].get_text(", ",strip=True)
        if title:
            games.append({"title":title,"publisher":pub,"developer":dev,
                          "type":classify_type(title,pub,dev),"genre":infer_genre(title,pub)})
    print(f"  {len(games)} games from Wikipedia")
    return games


# ── 5. Box art (Nintendo EU) — limited per run ────────────────────────────────
def fetch_art_batch(titles_needing_art, session):
    """Fetch art for up to MAX_NEW_ART_PER_RUN titles. Returns {title: url_or_None}."""
    results = {}
    batch = list(titles_needing_art)[:MAX_NEW_ART_PER_RUN]
    print(f"  Fetching art for {len(batch)} new titles (of {len(titles_needing_art)} needing art)…")
    for title in batch:
        try:
            params = {
                "q": title,
                "fq": "type:GAME AND system_type:nintendoswitch*",
                "rows": "1",
                "fl": "image_url_sq_s,image_url_h2x1_s,title"
            }
            r = session.get(NINTENDO_EU, params=params, timeout=3)
            r.raise_for_status()
            docs = r.json().get("response",{}).get("docs",[])
            if docs:
                doc = docs[0]
                found = re.sub(r'[^a-z0-9 ]','', (doc.get("title") or "").lower())
                query = re.sub(r'[^a-z0-9 ]','', title.lower())
                fw = set(w for w in found.split() if len(w)>2)
                qw = [w for w in query.split() if len(w)>2]
                if qw and fw and sum(1 for w in qw if w in fw)/len(qw) >= 0.5:
                    results[title] = doc.get("image_url_h2x1_s") or doc.get("image_url_sq_s")
                    continue
            results[title] = None
        except:
            results[title] = None
        time.sleep(0.1)
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
def merge_all(summary, wiki, details, overrides, art_cache):
    wiki_map = {norm(g["title"]): g for g in wiki}
    summary_norms = {norm(g["title"]) for g in summary}
    session = requests.Session()
    session.headers.update(HEADERS)

    # Titles we still need art for
    titles_needing_art = []

    merged = []
    for g in summary:
        g = dict(g)
        nk = norm(g["title"])

        # Details tab: per-region formats
        dk = fuzzy(g["title"], {k:k for k in details})
        if dk:
            rd = details[dk]
            g['formats'] = {r: v['fmt'] for r,v in rd.items()}
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

        # Overrides
        ok = fuzzy(g["title"], overrides)
        if ok:
            for k,v in overrides[ok].items():
                if not k.startswith("_"): g[k] = v

        # Art
        if nk in art_cache:
            g['art'] = art_cache[nk]
        else:
            g['art'] = None
            titles_needing_art.append(g["title"])

        merged.append(g)

    # Wiki-only additions
    for wg in wiki:
        if norm(wg["title"]) not in summary_norms:
            g = {"title":wg["title"],"publisher":wg["publisher"],"developer":wg["developer"],
                 "type":wg["type"],"genre":wg["genre"],"date":"TBA","status":"u",
                 "fmt":"?","region":"ww","releases":{},"formats":{},"note":"","art":None}
            ok = fuzzy(g["title"], overrides)
            if ok:
                for k,v in overrides[ok].items():
                    if not k.startswith("_"): g[k] = v
            nk = norm(g["title"])
            if nk in art_cache:
                g['art'] = art_cache[nk]
            else:
                titles_needing_art.append(g["title"])
            merged.append(g)

    # Fetch art for new titles (limited batch)
    if titles_needing_art:
        new_art = fetch_art_batch(titles_needing_art, session)
        # Apply to merged list
        title_map = {g["title"]: g for g in merged}
        for title, url in new_art.items():
            if title in title_map:
                title_map[title]['art'] = url

    return merged


# ── 8. Assign IDs ─────────────────────────────────────────────────────────────
def assign_ids(games):
    for i, g in enumerate(sorted(games, key=lambda x: x["title"].lower()), start=1):
        g["id"] = i
    return games


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    art_cache, existing_games = load_existing()
    print(f"  {len(art_cache)} art URLs cached · {len(existing_games)} existing games")

    try:
        details = fetch_details()
    except Exception as e:
        print(f"  ⚠ Details tab failed ({e}), skipping per-region formats")
        details = {}

    try:
        summary = fetch_summary()
    except Exception as e:
        print(f"  ⚠ Summary tab failed ({e})")
        summary = []

    wiki      = fetch_wiki()
    overrides = load_overrides()
    games     = merge_all(summary, wiki, details, overrides, art_cache)
    games     = assign_ids(games)

    # ── Safety guard: never overwrite with 0 games ──
    if len(games) == 0:
        if existing_games:
            print(f"  ⚠ All sources failed — keeping existing {len(existing_games)} games unchanged")
            sys.exit(0)   # exit cleanly so workflow doesn't fail, but no commit needed
        else:
            print("  ⚠ No games found and no existing data to fall back to")
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

    released = sum(1 for g in games if g["status"]=="r")
    upcoming = sum(1 for g in games if g["status"]=="u")
    with_art  = sum(1 for g in games if g.get("art"))
    print(f"\n✓ {len(games)} games → {OUT_FILE}")
    print(f"  Released: {released}  Upcoming: {upcoming}  With art: {with_art}/{len(games)}")

if __name__ == "__main__":
    main()
