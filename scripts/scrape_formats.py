#!/usr/bin/env python3
"""
Nintendo Switch 2 format scraper.
Sources:
  1. Nintendo Wire  — Full Cart / GKC / Code-in-Box tables (Playwright JS render)
  2. Nintendo Life  — Full Cart list + GKC list (plain HTML)
  3. Nintendo Everything — GKC list (plain HTML)

Returns: {normalised_title: (original_title, format_code)}
  c = Physical Cart (full game on cartridge)
  k = Game-Key Card
  b = Code in Box
  d = Digital Only
"""

import re
import sys

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("  Missing deps")
    sys.exit(1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

NINTENDOLIFE_CART = "https://www.nintendolife.com/guides/every-nintendo-switch-2-physical-release-with-the-full-game-on-the-cart"
NINTENDOLIFE_GKC  = "https://www.nintendolife.com/guides/every-nintendo-switch-2-game-key-card-release"
NINTENDOEVERYTHING_GKC = "https://nintendoeverything.com/list-of-all-nintendo-switch-2-games-with-a-game-key-card-release/"
NINTENDOWIRE_URL  = "https://nintendowire.com/guides/switch-2/all-physical-games-and-type/"


def norm(t):
    import unicodedata
    t = unicodedata.normalize("NFKD", str(t)).encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"[-]", " ", t.lower())
    t = re.sub(r"[^a-z0-9 ]", "", t)
    return re.sub(r"\s+", " ", t).strip()


def clean_game_title(raw):
    """Strip dates, publisher info, and platform suffixes from scraped title text."""
    # Remove "(Switch 2)" suffix
    t = re.sub(r"\s*[\(\[].*?(?:Switch 2|NS2|Nintendo Switch 2).*?[\)\]]", "", raw, flags=re.I)
    # Remove trailing date patterns like "5th Jun 2025", "Q3 2026", "2026"
    t = re.sub(r"\s+\d{1,2}(?:st|nd|rd|th)?\s+\w+\s+20\d{2}.*$", "", t)
    t = re.sub(r"\s+Q[1-4]\s+20\d{2}.*$", "", t)
    t = re.sub(r"\s+20\d{2}.*$", "", t)
    # Remove publisher info after slash
    t = re.sub(r"\s*/.*$", "", t)
    # Remove "– Nintendo Switch 2 Edition" and variants
    t = re.sub(r"\s*[-–]\s*Nintendo Switch 2 Edition.*$", "", t, flags=re.I)
    return t.strip()


def scrape_nintendolife_list(url, fmt_code, session):
    """Scrape a Nintendo Life guide page for game titles."""
    results = {}
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Nintendo Life uses <ul> lists with <li> containing game title text
        # Try multiple selectors
        article = soup.find("article") or soup.find("main") or soup.find("div", class_=re.compile("article|content|guide"))

        if article:
            for li in article.find_all("li"):
                text = li.get_text(" ", strip=True)
                # Remove refs like [1], [2]
                text = re.sub(r"\[\d+\]", "", text)
                title = clean_game_title(text)
                if title and len(title) > 3 and not title.lower().startswith("if you"):
                    nk = norm(title)
                    if nk and len(nk) > 3:
                        results[nk] = (title, fmt_code)

        # Also try strong/bold tags which NL sometimes uses for game titles
        if len(results) < 10:
            for tag in (article or soup).find_all(["strong", "b", "a"]):
                text = tag.get_text(" ", strip=True)
                if len(text) > 5 and "(Switch 2)" in tag.get_text():
                    title = clean_game_title(text)
                    nk = norm(title)
                    if nk and len(nk) > 3:
                        results[nk] = (title, fmt_code)

    except Exception as e:
        print(f"  Nintendo Life [{fmt_code}] failed: {e}")
    return results


def scrape_nintendoeverything_gkc(session):
    """Scrape Nintendo Everything's GKC list."""
    results = {}
    try:
        r = session.get(NINTENDOEVERYTHING_GKC, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        article = soup.find("article") or soup.find("div", class_=re.compile("entry|content|post"))
        if article:
            for li in article.find_all("li"):
                text = re.sub(r"\[\d+\]", "", li.get_text(" ", strip=True))
                title = clean_game_title(text)
                if title and len(title) > 3:
                    nk = norm(title)
                    if nk and len(nk) > 3:
                        results[nk] = (title, "k")
    except Exception as e:
        print(f"  Nintendo Everything GKC failed: {e}")
    return results


def scrape_nintendowire_playwright():
    """Scrape Nintendo Wire using Playwright (JS-rendered tables)."""
    results = {}
    try:
        from playwright.sync_api import sync_playwright
        import time

        SECTION_FMT = {
            "full cartridge": "c",
            "game-key card": "k",
            "game key card": "k",
            "code in a box": "b",
            "code-in-a-box": "b",
            "code in box": "b",
            "unknown physical": "?",
        }

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(NINTENDOWIRE_URL, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)
                text = page.inner_text("article") or page.inner_text("body")
            except Exception as e:
                print(f"  Nintendo Wire Playwright failed: {e}")
                browser.close()
                return results
            browser.close()

        current_fmt = None
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            line_lower = line.lower()
            for section_key, fmt in SECTION_FMT.items():
                if section_key in line_lower:
                    current_fmt = fmt
                    break
            if current_fmt is None or current_fmt == "?":
                continue
            if any(skip in line_lower for skip in ["game", "release date", "notes", "list of all"]):
                continue
            # Strip trailing date/notes
            clean = re.sub(r"\d{1,2}/\d{1,2}/\d{2,4}.*$", "", line).strip()
            clean = re.sub(r"Q[1-4]\s*\d{4}.*$", "", clean).strip()
            clean = re.sub(r"\s+20\d{2}.*$", "", clean).strip()
            if clean and len(clean) > 3:
                nk = norm(clean)
                if nk and len(nk) > 3:
                    results[nk] = (clean, current_fmt)

    except ImportError:
        print("  Playwright not available for Nintendo Wire")
    except Exception as e:
        print(f"  Nintendo Wire error: {e}")

    return results


def get_format_map():
    """
    Fetch format data from all sources and merge.
    Priority: Nintendo Wire > Nintendo Life > Nintendo Everything
    Returns: {norm_title: (title, fmt_code)}
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    print("Fetching Nintendo Life cart list…")
    nl_cart = scrape_nintendolife_list(NINTENDOLIFE_CART, "c", session)
    print(f"  {len(nl_cart)} full-cart games from Nintendo Life")

    print("Fetching Nintendo Life GKC list…")
    nl_gkc = scrape_nintendolife_list(NINTENDOLIFE_GKC, "k", session)
    print(f"  {len(nl_gkc)} GKC games from Nintendo Life")

    print("Fetching Nintendo Everything GKC list…")
    ne_gkc = scrape_nintendoeverything_gkc(session)
    print(f"  {len(ne_gkc)} GKC games from Nintendo Everything")

    print("Fetching Nintendo Wire (Playwright)…")
    nw = scrape_nintendowire_playwright()
    print(f"  {len(nw)} games from Nintendo Wire")

    # Merge — Nintendo Wire most trusted, then NL, then NE
    merged = {}
    for source in [ne_gkc, nl_gkc, nl_cart, nw]:
        for nk, (title, fmt) in source.items():
            if nk not in merged:
                merged[nk] = (title, fmt)
            else:
                # More specific format wins (c > k > b > ?)
                existing_fmt = merged[nk][1]
                priority = {"c": 3, "k": 2, "b": 2, "d": 1, "?": 0}
                if priority.get(fmt, 0) > priority.get(existing_fmt, 0):
                    merged[nk] = (title, fmt)

    counts = {}
    for _, (_, fmt) in merged.items():
        counts[fmt] = counts.get(fmt, 0) + 1
    print(f"  Format map total: {len(merged)} — {counts}")
    return merged


if __name__ == "__main__":
    fm = get_format_map()
    print(f"\nSample (first 15):")
    for nk, (title, fmt) in list(fm.items())[:15]:
        print(f"  [{fmt}] {title[:50]}")
