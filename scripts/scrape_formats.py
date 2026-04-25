#!/usr/bin/env python3
"""
Nintendo Wire format scraper.
Fetches the categorised Switch 2 physical game list from Nintendo Wire
(Full Cart / Game-Key Card / Code in Box) using Playwright for JS rendering.

Returns: {normalised_title: format_code}  where format codes are c / k / b
"""

import re
import sys

def norm(t):
    t = re.sub(r'[-–—\u2019]', ' ', t.lower())
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return re.sub(r'\s+', ' ', t).strip()

NINTENDOWIRE_URL = "https://nintendowire.com/guides/switch-2/all-physical-games-and-type/"

SECTION_MAP = {
    "List of all Switch 2 full cartridge releases": "c",
    "List of all Switch 2 game-key cards": "k",
    "List of all Switch 2 code in a box releases": "b",
    "List of all unknown Switch 2 physical releases": "?",
}

def fetch_nintendowire_playwright():
    """Fetch format data using Playwright (handles JS-rendered tables)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ⚠ Playwright not available — skipping Nintendo Wire")
        return {}

    print("Fetching Nintendo Wire format list (Playwright)…")
    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(NINTENDOWIRE_URL, wait_until="networkidle", timeout=30000)
            # Wait for the FooTable data to render
            page.wait_for_timeout(2000)
            text = page.inner_text("article") or page.inner_text("body")
        except Exception as e:
            print(f"  ⚠ Playwright navigation failed: {e}")
            browser.close()
            return {}
        browser.close()

    # Parse sections from the plain text
    current_fmt = None
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Detect section headers
        for section_title, fmt in SECTION_MAP.items():
            if section_title.lower() in line.lower():
                current_fmt = fmt
                break

        if current_fmt is None:
            continue

        # Skip obvious non-game lines
        if line.lower() in ('game', 'release date', 'notes', 'gamerelease datenotes'):
            continue
        if re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}$', line):
            continue
        if len(line) < 3 or line.startswith('List of'):
            continue
        # Skip lines that are clearly notes (start with lowercase letter after first char)
        if re.match(r'^[A-Z].{0,20}(only|exclusive|edition|available|upgrade|bundle|limited)', line, re.I) and len(line) > 60:
            continue

        # Clean the line — strip trailing date and notes
        # Lines look like "Game Title6/5/25Note text" or just "Game Title"
        clean = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}.*$', '', line).strip()
        clean = re.sub(r'Q[1-4]\s*\d{4}.*$', '', clean).strip()
        clean = re.sub(r'\s+20\d{2}.*$', '', clean).strip()
        if clean and len(clean) > 3 and current_fmt != '?':
            results[norm(clean)] = (clean, current_fmt)

    print(f"  Found {len(results)} games with format data from Nintendo Wire")
    counts = {}
    for _, (_, fmt) in results.items():
        counts[fmt] = counts.get(fmt, 0) + 1
    print(f"  {counts}")
    return results


def fetch_nintendowire_requests():
    """
    Fallback: try to get Nintendo Wire data via requests.
    The page uses JS-rendered tables so this may be incomplete,
    but the script tags contain table IDs we can use with admin-ajax.
    """
    try:
        import requests
    except ImportError:
        return {}

    print("Fetching Nintendo Wire format list (requests fallback)…")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }

    try:
        r = requests.get(NINTENDOWIRE_URL, headers=headers, timeout=15)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print(f"  ⚠ Requests fetch failed: {e}")
        return {}

    # Extract table instance JSON from inline scripts
    # Each instance has table_id and title
    table_pattern = re.compile(
        r"ninja_table_instance_\d+'\s*=\s*(\{.*?\"table_id\":\"(\d+)\".*?\"title\":\"([^\"]+)\".*?\})",
        re.DOTALL
    )

    fmt_by_id = {
        "208457": "c",  # Full cartridge
        "208458": "k",  # Game-Key Card
        "208459": "b",  # Code in Box
        "208460": "?",  # Unknown
    }

    # Try the admin-ajax.php endpoint to get table data
    # Extract nonce from page
    nonce_match = re.search(r'"nonce"\s*:\s*"([a-f0-9]{10})"', html)
    nonce = nonce_match.group(1) if nonce_match else None

    results = {}
    session = requests.Session()
    session.headers.update(headers)

    for table_id, fmt in fmt_by_id.items():
        if fmt == '?':
            continue
        try:
            data = {
                'action': 'ninja_tables_public_data',
                'table_id': table_id,
            }
            if nonce:
                data['_wpnonce'] = nonce
            resp = session.post(
                'https://nintendowire.com/wp-admin/admin-ajax.php',
                data=data, timeout=10
            )
            if resp.status_code == 200 and resp.text and resp.text != '0':
                rows = resp.json()
                if isinstance(rows, list):
                    for row in rows:
                        title = row.get('game') or row.get('Game') or row.get('title') or ''
                        title = re.sub(r'<[^>]+>', '', title).strip()
                        if title:
                            results[norm(title)] = (title, fmt)
        except Exception:
            pass

    print(f"  Found {len(results)} games via requests (may be incomplete)")
    return results


def get_format_map():
    """Main entry point — tries Playwright first, falls back to requests."""
    # Try Playwright first (most complete)
    results = fetch_nintendowire_playwright()
    if not results:
        results = fetch_nintendowire_requests()
    return results


if __name__ == "__main__":
    fm = get_format_map()
    for nk, (title, fmt) in list(fm.items())[:10]:
        print(f"  {fmt}: {title}")
