const CACHE_NAME = "money-manager-cache-v3";
const APP_SHELL = [
  "/",
  "/login",
  "/manifest.webmanifest",
  "/static/styles.css",
  "/static/offline.html",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  const requestUrl = new URL(event.request.url);
  if (requestUrl.origin !== self.location.origin) {
    return;
  }

  // Let export/download endpoints go directly to network so attachment responses
  // (xlsx/csv/pdf) are not treated like app page navigations.
  if (
    requestUrl.pathname.startsWith("/records/export") ||
    requestUrl.pathname.startsWith("/records/bulk/export")
  ) {
    return;
  }

  if (event.request.mode === "navigate") {
    event.respondWith(fetch(event.request).catch(() => caches.match("/static/offline.html")));
    return;
  }

  if (requestUrl.pathname.startsWith("/static/")) {
    event.respondWith(
      caches.match(event.request).then(
        (cachedResponse) =>
          cachedResponse ||
          fetch(event.request)
            .then((response) => {
              const responseCopy = response.clone();
              caches.open(CACHE_NAME).then((cache) => cache.put(event.request, responseCopy));
              return response;
            })
            .catch(() => cachedResponse)
      )
    );
    return;
  }

  event.respondWith(fetch(event.request).catch(() => caches.match(event.request)));
});
