/* CARTEL Service Worker — PWA + Push Notifications */
const CACHE_VERSION = 'cartel-v1';
const STATIC_ASSETS = [
  '/static/css/',
  '/static/js/main.js',
  '/static/images/cartel-icon-192.png',
  '/static/manifest.json',
];

// ── 설치 ──────────────────────────────────────────────────────────────────
self.addEventListener('install', event => {
  self.skipWaiting();
});

// ── 활성화 ────────────────────────────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// ── 네트워크 요청 (캐시 우선 전략 — 정적 자산만) ─────────────────────────
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  // 정적 파일만 캐시
  if (url.pathname.startsWith('/static/') || url.pathname.startsWith('/uploads/')) {
    event.respondWith(
      caches.open(CACHE_VERSION).then(cache =>
        cache.match(event.request).then(cached =>
          cached || fetch(event.request).then(resp => {
            if (resp.ok) cache.put(event.request, resp.clone());
            return resp;
          })
        )
      )
    );
  }
  // 그 외 요청은 그냥 네트워크
});

// ── 푸시 수신 ─────────────────────────────────────────────────────────────
self.addEventListener('push', event => {
  let data = { title: 'CARTEL', body: '새 알림이 있어요.', url: '/', icon: '' };
  try {
    if (event.data) data = { ...data, ...event.data.json() };
  } catch (e) {
    if (event.data) data.body = event.data.text();
  }

  const options = {
    body:    data.body,
    icon:    data.icon || '/static/images/cartel-icon-192.png',
    badge:   '/static/images/cartel-icon-192.png',
    vibrate: [100, 50, 100],
    data:    { url: data.url || '/' },
    actions: data.actions || [],
    tag:     data.tag || 'cartel-notif',
    renotify: true,
  };

  event.waitUntil(
    self.registration.showNotification(data.title, options)
  );
});

// ── 알림 클릭 ────────────────────────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(windowClients => {
      // 이미 열려 있는 탭이 있으면 포커스
      for (const c of windowClients) {
        if (c.url === targetUrl && 'focus' in c) return c.focus();
      }
      // 없으면 새 탭
      if (clients.openWindow) return clients.openWindow(targetUrl);
    })
  );
});

// ── 백그라운드 동기화 (향후 확장용) ──────────────────────────────────────
self.addEventListener('sync', event => {
  // 향후 오프라인 → 온라인 전환 시 동기화
});
