// sw.js — Switch 2 Tracker Service Worker
// Strategy:
//   • App shell (HTML, CSS, JS): cache-first, update in background
//   • games.json: network-first with cache fallback (want fresh data)
//   • Everything else: cache-first

const SHELL_CACHE  = 'sw2-shell-v1';
const DATA_CACHE   = 'sw2-data-v1';

const SHELL_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
];

const DATA_ASSETS = [
  '/data/games.json',
  '/data/consoles.json',
  '/data/accessories.json',
];

// ── Install: pre-cache app shell ─────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then(cache => cache.addAll(SHELL_ASSETS))
  );
  self.skipWaiting();
});

// ── Activate: clean up old caches ────────────────────────────────────────────
self.addEventListener('activate', event => {
  const keep = [SHELL_CACHE, DATA_CACHE];
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => !keep.includes(k)).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// ── Fetch ─────────────────────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // games.json: network-first so data stays fresh; fall back to cache
  if (url.pathname.endsWith('games.json')) {
    event.respondWith(networkFirstWithCache(event.request, DATA_CACHE));
    return;
  }

  // Other data files: same strategy
  if (DATA_ASSETS.some(a => url.pathname.endsWith(a.replace('/data/', '')))) {
    event.respondWith(networkFirstWithCache(event.request, DATA_CACHE));
    return;
  }

  // Everything else: cache-first
  event.respondWith(cacheFirstWithNetwork(event.request, SHELL_CACHE));
});

async function networkFirstWithCache(request, cacheName) {
  const cache = await caches.open(cacheName);
  try {
    const response = await fetch(request);
    if (response.ok) {
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await cache.match(request);
    if (cached) return cached;
    throw new Error(`Offline and no cache for ${request.url}`);
  }
}

async function cacheFirstWithNetwork(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) {
    // Revalidate in background
    fetch(request).then(response => {
      if (response.ok) {
        caches.open(cacheName).then(c => c.put(request, response));
      }
    }).catch(() => {});
    return cached;
  }
  const response = await fetch(request);
  if (response.ok) {
    const cache = await caches.open(cacheName);
    cache.put(request, response.clone());
  }
  return response;
}

// ── Background sync: notify app when new game data is available ───────────────
self.addEventListener('message', event => {
  if (event.data?.type === 'CHECK_UPDATE') {
    checkForUpdates();
  }
});

async function checkForUpdates() {
  try {
    const cache   = await caches.open(DATA_CACHE);
    const cached  = await cache.match('/data/games.json');
    const fresh   = await fetch('/data/games.json?t=' + Date.now());

    if (!fresh.ok) return;

    const freshData = await fresh.clone().json();

    if (cached) {
      const cachedData = await cached.json();
      if (cachedData.updated !== freshData.updated || cachedData.count !== freshData.count) {
        // New data available — notify all clients
        const clients = await self.clients.matchAll({ type: 'window' });
        clients.forEach(client => client.postMessage({
          type: 'DATA_UPDATED',
          count: freshData.count,
          updated: freshData.updated,
        }));
      }
    }

    await cache.put('/data/games.json', fresh);
  } catch {
    // Network unavailable — silently skip
  }
}
