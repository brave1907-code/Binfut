// Vadeli Grafik - servis çalışanı: uygulama kabuğunu önbelleğe alır, internet yokken de açılır.
// Sayfa (index.html) önce internetten alınır (güncellemeler hemen gelir), olmazsa önbellekten açılır.
// Fiyat/Binance/Firebase istekleri hiç önbelleğe alınmaz.
const CACHE = 'vg-v2';
const SHELL = ['./', 'index.html', 'manifest.json', 'icon-192.png', 'icon-512.png', 'firebase-config.js'];
const LIBS = ['https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js'];
self.addEventListener('install', e => {
  e.waitUntil((async () => {
    const c = await caches.open(CACHE);
    await Promise.all(SHELL.map(u => c.add(u).catch(() => {})));
    await Promise.all(LIBS.map(u => c.add(new Request(u, { mode: 'cors' })).catch(() => {})));
    self.skipWaiting();
  })());
});
self.addEventListener('activate', e => {
  e.waitUntil((async () => {
    for (const k of await caches.keys()) if (k !== CACHE) await caches.delete(k);
    await self.clients.claim();
  })());
});
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const u = new URL(req.url);
  const same = u.origin === self.location.origin;
  const lib = LIBS.includes(req.url);
  if (!same && !lib) return;                       // Binance, Firebase, ntfy... hep doğrudan internetten
  if (lib){                                        // grafik kütüphanesi: önbellek önce
    e.respondWith(caches.match(req).then(r => r || fetch(req).then(res => { const cp = res.clone(); caches.open(CACHE).then(c => c.put(req, cp)); return res; })));
    return;
  }
  e.respondWith((async () => {                     // kendi dosyalarımız: önce internet, olmazsa önbellek
    try{
      const res = await fetch(req);
      if (res && res.ok){ const cp = res.clone(); caches.open(CACHE).then(c => c.put(req, cp)); }
      return res;
    }catch(err){
      const r = await caches.match(req, { ignoreSearch: true });
      if (r) return r;
      if (req.mode === 'navigate') return (await caches.match('index.html')) || (await caches.match('./')) || Response.error();
      return Response.error();
    }
  })());
});
