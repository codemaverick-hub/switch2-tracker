# HANDOFF — Switch 2 Tracker

Public repo: [codemaverick-hub/switch2-tracker](https://github.com/codemaverick-hub/switch2-tracker). This file is the pick-up brief for Claude or ChatGPT.

## What it is

A free Progressive Web App for Nintendo Switch 2 collectors: track physical/digital libraries by region, edition, and format, with daily auto-updated game data.

- Live app: https://codemaverick-hub.github.io/switch2-tracker
- Support: https://www.patreon.com/c/switch2tracker

## Tech stack

- Single-file PWA (`index.html`, `sw.js`, `manifest.json`) — no framework, no build step
- Python scraper: `scripts/scrape.py` (requests, beautifulsoup4)
- GitHub Actions daily at 06:00 UTC (`.github/workflows/update-games.yml`)
- GitHub Pages from branch `main`, folder `/ (root)`

## Where it lives

- GitHub: this repository
- Pages site as above
- Owning bot: **Switch 2 Tracker**

## How to run, build, and install

There is no build. Serve the repo root, or use GitHub Pages.

Scraper:

```bash
pip install requests beautifulsoup4
python scripts/scrape.py
```

Manual Actions run: Actions → Update Switch 2 Game Data → Run workflow.

## Current state (as of 23 Nov 2026, America/New_York)

- Public repo; GitHub reported a push on 23 Nov 2026 (data or workflow update).
- Features as in README: 379+ titles, per-region formats, editions, box art, collection tracking in `localStorage`, consoles, accessories, offline service worker.
- Data sources: r/NSCollectors sheets, Wikipedia, Nintendo Europe search API, `data/overrides.json` for manual corrections.

## Known bugs / watch-outs

- This repo is **public**. Never commit API keys or personal collection exports if they contain private data. Collection state belongs in the browser, not the repo.
- The scraper has a safety guard: it must not overwrite `data/games.json` with 0 games if all sources fail.
- Box art is fetched incrementally (up to 40 new URLs per daily run).

## Next steps

- Prefer `data/overrides.json` for one-off corrections rather than editing generated `games.json` by hand.
- When you finish a task, update this HANDOFF.md.

## Rules

- **No secrets.** Public repository. Keep it public (Frank confirmed).
- **Lumen's files are off-limits to other tools.**
- When you finish a task, update this HANDOFF.md.
