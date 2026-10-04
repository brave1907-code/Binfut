#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vadeli Grafik - Alarm Sunucusu
==============================
Uygulama (telefon) kapalıyken de Binance Futures fiyatlarını izler; alarm tetiklenince
ntfy uygulamasıyla (ve istersen Telegram'dan) telefona bildirim gönderir.

Nasıl çalışır
  * Uygulamadaki alarmlar, ntfy.sh üzerindeki gizli bir konuya (NTFY_TOPIC) küçük JSON
    mesajları olarak gönderilir. Bu sunucu o konuyu dinler; sunucuya gelen bağlantı yoktur,
    sadece dışarıya bağlanır (port açmak / alan adı gerekmez).
  * Fiyat her POLL_SECONDS saniyede bir Binance'ten alınır. Ayrıca 1 dakikalık mumların
    en yüksek/en düşük değerine de bakılır: iki kontrol arasında ya da bağlantı kopukken
    seviyeye değen iğneler (fitiller) kaçmaz.
  * Alarm türleri: fiyat seviyesi ve trend çizgisi (çizgi fiyatı zamanla değişir).
  * Alarmlar STATE_FILE içinde saklanır; sunucu yeniden başlasa da kaybolmaz.

Gerekenler:  Python 3.8+   ve   pip install requests

Bildirim kanalları (ikisi de kullanılabilir)
  ntfy uygulaması (önerilen)  Telefona ntfy uygulamasını kur (Android/iOS), şu kanala abone ol:  <NTFY_TOPIC>-bildirim
                              Uygulama kapalıyken de bildirim gelir. Bilgisayarda tarayıcıdan da abone olabilirsin:
                              https://ntfy.sh/<NTFY_TOPIC>-bildirim
  Telegram (isteğe bağlı)     TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID verirsen oraya da gider.

Ortam değişkenleri
  NTFY_TOPIC          (zorunlu) Uygulamada  Alarm > 📱 Telefon  bölümünde görünen kod
  TELEGRAM_BOT_TOKEN  (isteğe bağlı) Telegram botunun anahtarı
  TELEGRAM_CHAT_ID    (isteğe bağlı) Telegram sohbet numarası
  NTFY_PUSH           1/0: bildirimi ntfy uygulamasına gönder. Telegram verilmediyse varsayılan 1
  APP_URL             (isteğe bağlı) bildirime dokununca açılacak adres, ör. https://kullanici.github.io/Binfut/
  STATE_FILE          varsayılan: alarm_state.json
  POLL_SECONDS        varsayılan: 2
  QUIET=1             "kaydedildi" onay mesajlarını kapat

Önemli: Binance Futures bazı ülkelerin IP'lerini engeller (ABD gibi, HTTP 451). Sunucuyu Türkiye ya da
Avrupa'daki bir makinede çalıştır. Mevcut sinyal botun nerede çalışıyorsa orası uygundur.

Çalıştırma:
  export NTFY_TOPIC=vg-xxxxxxxx
  python3 alarm_server.py
"""
import json
import math
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime

import requests


# --------------------------------------------------------------------------- yardımcılar
def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def now_ms():
    return int(time.time() * 1000)


def log(msg):
    print("[%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def fp(x):
    """Fiyatı sade yaz: 67000.50000000 -> 67000.5"""
    s = ("%.8f" % float(x)).rstrip("0").rstrip(".")
    return s or "0"


def clock(ms):
    return datetime.fromtimestamp(ms / 1000.0).strftime("%d.%m %H:%M")


def ceil_minute(ms):
    return int(math.ceil(ms / 60000.0)) * 60000


def floor_minute(ms):
    return int(math.floor(ms / 60000.0)) * 60000


# --------------------------------------------------------------------------- alarm mantığı
SYM_RE = re.compile(r"^[A-Z0-9_]{3,30}$")
ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,60}$")


def clean_alarm(raw):
    """Uygulamadan gelen alarmı doğrula; geçersizse None."""
    try:
        a = {
            "id": str(raw["id"]),
            "symbol": str(raw["symbol"]).upper(),
            "dir": raw["dir"],
            "price": float(raw["price"]),
            "created": int(raw["created"]),
        }
        if not ID_RE.match(a["id"]) or not SYM_RE.match(a["symbol"]):
            return None
        if a["dir"] not in ("up", "down") or not math.isfinite(a["price"]) or a["price"] <= 0:
            return None
        ln = raw.get("line")
        if ln:
            a["line"] = {k: float(ln[k]) for k in ("t1", "p1", "t2", "p2")}
            if not all(math.isfinite(v) for v in a["line"].values()):
                return None
        a["active"] = True
        return a
    except Exception:
        return None


def level(a, t_ms):
    """Alarmın o andaki hedef fiyatı. Trend çizgisi sağa doğru uzatılmış sayılır."""
    ln = a.get("line")
    if ln:
        dt = ln["t2"] - ln["t1"]
        if not dt:
            return ln["p1"]
        return ln["p1"] + (ln["p2"] - ln["p1"]) * (t_ms / 1000.0 - ln["t1"]) / dt
    return a["price"]


def crossed(a, price, t_ms):
    lv = level(a, t_ms)
    return price >= lv if a["dir"] == "up" else price <= lv


def alarm_text(a, hit, cur, late):
    what = ("trend çizgisine (%s)" % fp(hit)) if a.get("line") else ("%s seviyesine" % fp(a["price"]))
    how = "yükselerek" if a["dir"] == "up" else "düşerek"
    t = "⏰ ALARM · %s\n%s %s ulaştı" % (a["symbol"], what, how)
    if cur is not None:
        t += "\nŞu an: %s" % fp(cur)
    if late:
        t += "\n(Bağlantı kopukken gerçekleşti · %s)" % clock(a["fired_at"])
    return t


# --------------------------------------------------------------------------- kalıcı durum
class State:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = {"alarms": {}, "ntfy_since": None, "outbox": []}
        self.fresh = not os.path.exists(path)
        if not self.fresh:
            try:
                with open(path, encoding="utf-8") as f:
                    loaded = json.load(f)
                for k in self.data:
                    if k in loaded:
                        self.data[k] = loaded[k]
            except Exception as e:
                log("UYARI: durum dosyası okunamadı (%s); boş başlanıyor" % e)
                self.fresh = True

    def save(self):
        with self.lock:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)

    def prune(self):
        cut = now_ms() - 3 * 86400 * 1000
        with self.lock:
            al = self.data["alarms"]
            for k in [k for k, a in al.items() if not a["active"] and a.get("fired_at", 0) < cut]:
                del al[k]
            self.data["outbox"] = [o for o in self.data["outbox"] if o["ts"] > now_ms() - 86400 * 1000]


# --------------------------------------------------------------------------- sunucu
class Server:
    def __init__(self, cfg):
        self.cfg = cfg
        self.st = State(cfg["state_file"])
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.last_price = {}
        self.last_ok_ms = None
        self.warned_451 = False

    # ---- bildirim kuyruğu: gönderilemezse tekrar dener, alarm kaybolmaz
    def enqueue(self, text):
        with self.st.lock:
            self.st.data["outbox"].append({"text": text, "ts": now_ms()})
            self.st.save()
        self.wake.set()

    def deliver(self, text):
        c = self.cfg
        ok = False
        if c["tg_token"] and c["tg_chat"]:
            try:
                r = requests.post("%s/bot%s/sendMessage" % (c["tg_api"], c["tg_token"]),
                                  json={"chat_id": c["tg_chat"], "text": text}, timeout=15)
                if r.ok:
                    ok = True
                else:
                    log("Telegram hatası: %s %s" % (r.status_code, r.text[:200]))
            except Exception as e:
                log("Telegram bağlantı hatası: %s" % e)
        if c["push_ntfy"]:
            try:
                hdr = {"Priority": "urgent", "Tags": "alarm_clock", "Title": "Vadeli Grafik Alarm"}
                if c.get("app_url"):
                    hdr["Click"] = c["app_url"]          # bildirime dokununca uygulama açılır
                r = requests.post("%s/%s-bildirim" % (c["ntfy"], c["topic"]), data=text.encode("utf-8"),
                                  headers=hdr, timeout=15)
                if r.ok:
                    ok = True
                else:
                    log("ntfy bildirim hatası: %s" % r.status_code)
            except Exception as e:
                log("ntfy bağlantı hatası: %s" % e)
        return ok

    def outbox_loop(self):
        while not self.stop.is_set():
            with self.st.lock:
                item = self.st.data["outbox"][0] if self.st.data["outbox"] else None
            if item is None:
                self.wake.wait(5)
                self.wake.clear()
                continue
            if self.deliver(item["text"]):
                with self.st.lock:
                    if self.st.data["outbox"] and self.st.data["outbox"][0] is item:
                        self.st.data["outbox"].pop(0)
                    self.st.save()
            else:
                self.stop.wait(self.cfg["retry"])

    # ---- uygulamadan gelen işlemler
    def apply_op(self, op):
        kind = op.get("op")
        with self.st.lock:
            alarms = self.st.data["alarms"]
            if kind == "set":
                a = clean_alarm(op.get("a") or {})
                if not a:
                    log("Geçersiz alarm yok sayıldı")
                    return
                old = alarms.get(a["id"])
                if old and not old["active"] and old.get("created") == a["created"]:
                    return  # aynı alarm zaten tetiklenmiş; tekrar kurulmasın
                is_new = old is None or not old["active"]
                changed = (not is_new) and (old.get("price") != a["price"] or old.get("line") != a.get("line")
                                            or old["dir"] != a["dir"])
                alarms[a["id"]] = a
                self.st.save()
                if is_new or changed:
                    log("Alarm %s: %s %s %s" % ("kaydedildi" if is_new else "güncellendi", a["symbol"], a["dir"],
                                                 "trend" if a.get("line") else fp(a["price"])))
                    if not self.cfg["quiet"]:
                        what = "trend çizgisi" if a.get("line") else fp(a["price"])
                        self.enqueue("✅ Alarm sunucuya %s · %s · %s %s" % (
                            "kaydedildi" if is_new else "güncellendi", a["symbol"], what,
                            "↗" if a["dir"] == "up" else "↘"))
            elif kind == "del":
                if alarms.pop(str(op.get("id")), None) is not None:
                    self.st.save()
                    log("Alarm silindi: %s" % op.get("id"))
            elif kind == "keep":
                ids = set(str(i) for i in (op.get("ids") or []))
                gone = [k for k, a in alarms.items() if a["active"] and k not in ids]
                for k in gone:
                    del alarms[k]
                if gone:
                    self.st.save()
                    log("Uygulamada olmayan %d alarm temizlendi" % len(gone))

    def handle_message(self, m):
        try:
            op = json.loads(m.get("message", ""))
            if isinstance(op, dict):
                self.apply_op(op)
        except ValueError:
            pass

    def ntfy_loop(self):
        c = self.cfg
        backoff = 1
        while not self.stop.is_set():
            since = self.st.data.get("ntfy_since")
            if not since:  # ilk kurulum: eski mesajları tekrar oynatma
                since = str(int(time.time()))
            url = "%s/%s/json?since=%s" % (c["ntfy"], c["topic"], since)
            try:
                with requests.get(url, stream=True, timeout=(10, 90)) as r:
                    r.raise_for_status()
                    backoff = 1
                    for line in r.iter_lines(chunk_size=1):   # satır gelir gelmez işle (tamponda bekleme)
                        if self.stop.is_set():
                            break
                        if not line:
                            continue
                        m = json.loads(line)
                        if m.get("event") != "message":
                            continue
                        self.handle_message(m)
                        with self.st.lock:
                            self.st.data["ntfy_since"] = m.get("id")
                            self.st.save()
            except Exception as e:
                if not self.stop.is_set():
                    log("ntfy bağlantısı koptu (%s); %ds sonra tekrar" % (e, backoff))
                    self.stop.wait(backoff)
                    backoff = min(backoff * 2, 60)

    # ---- tetikleme
    def active_alarms(self):
        with self.st.lock:
            return [dict(a) for a in self.st.data["alarms"].values() if a["active"]]

    def fire(self, aid, hit, cur, when_ms, late):
        with self.st.lock:
            a = self.st.data["alarms"].get(aid)
            if not a or not a["active"]:
                return False
            a["active"] = False
            a["fired_at"] = when_ms
            a["hit"] = hit
            text = alarm_text(a, hit, cur, late)
            self.st.data["outbox"].append({"text": text, "ts": now_ms()})
            self.st.save()
        log("TETİKLENDİ %s %s hedef=%s güncel=%s%s" % (a["symbol"], a["dir"], fp(hit),
                                                       fp(cur) if cur is not None else "-", " (kopukluk)" if late else ""))
        self.wake.set()
        return True

    def check_prices(self, prices, t_ms):
        for a in self.active_alarms():
            p = prices.get(a["symbol"])
            if p is not None and crossed(a, p, t_ms):
                self.fire(a["id"], level(a, t_ms), p, t_ms, False)

    def get_prices(self):
        r = requests.get("%s/fapi/v1/ticker/price" % self.cfg["binance"], timeout=10)
        if r.status_code == 451 and not self.warned_451:
            self.warned_451 = True
            log("HATA 451: Binance bu IP'den erişimi engelliyor. Sunucuyu Türkiye/Avrupa'daki bir makinede çalıştır.")
        r.raise_for_status()
        return {x["symbol"]: float(x["price"]) for x in r.json()}

    def wick_check(self, since_ms, late):
        """1 dk'lık mumların en yüksek/en düşük değerine bakarak iğneleri ve kopukluk sırasını yakala."""
        now = now_ms()
        by = {}
        for a in self.active_alarms():
            by.setdefault(a["symbol"], []).append(a)
        for sym, alarms in by.items():
            # Alarm kurulduktan sonra açılan mumlar sayılır (kurulum anındaki kısmi mum atlanır);
            # kopukluğun başladığı mum dahildir: o ana kadar fiyat her 2 sn'de kontrol edildi.
            starts = {a["id"]: max(ceil_minute(a["created"]), floor_minute(since_ms)) for a in alarms}
            frm = max(min(starts.values()), now - 1499 * 60000)
            if frm > now:
                continue
            try:
                r = requests.get("%s/fapi/v1/klines" % self.cfg["binance"],
                                 params={"symbol": sym, "interval": "1m", "startTime": frm, "limit": 1500}, timeout=15)
                if not r.ok:
                    continue
                ks = r.json()
            except Exception as e:
                log("Mum verisi alınamadı (%s): %s" % (sym, e))
                continue
            if not isinstance(ks, list):
                continue
            for a in alarms:
                for k in ks:
                    if k[0] < starts[a["id"]]:
                        continue
                    lv = level(a, k[0] + 30000)
                    if (float(k[2]) >= lv) if a["dir"] == "up" else (float(k[3]) <= lv):
                        self.fire(a["id"], lv, self.last_price.get(sym), min(int(k[6]), now), late)
                        break

    # ---- ana döngü
    def poll_loop(self):
        c = self.cfg
        last_wick = time.time()
        fails = 0
        while not self.stop.is_set():
            t0 = time.time()
            try:
                prices = self.get_prices()
                self.last_price = prices
                now = now_ms()
                if self.last_ok_ms and now - self.last_ok_ms > c["gap"] * 1000:
                    log("Fiyat akışında %.0f sn kopukluk oldu; mumlardan geriye dönük kontrol" % ((now - self.last_ok_ms) / 1000))
                    self.wick_check(self.last_ok_ms - 5000, True)
                self.check_prices(prices, now)
                self.last_ok_ms = now
                fails = 0
            except Exception as e:
                fails += 1
                if fails in (1, 5) or fails % 30 == 0:
                    log("Fiyat alınamadı (%d. deneme): %s" % (fails, e))
            if time.time() - last_wick >= c["wick_every"]:
                last_wick = time.time()
                try:
                    self.wick_check(now_ms() - 120000, False)
                except Exception as e:
                    log("Mum kontrolü hatası: %s" % e)
            self.stop.wait(max(0.0, c["poll"] - (time.time() - t0)) + (min(fails, 10) * 2.0))

    def run(self):
        c = self.cfg
        self.st.prune()
        self.st.save()
        n = len(self.active_alarms())
        log("Alarm sunucusu başladı · %d aktif alarm · kanal: %s" % (
            n, ", ".join(x for x, on in (("Telegram", c["tg_token"] and c["tg_chat"]), ("ntfy", c["push_ntfy"])) if on)))
        if not c["quiet"]:
            self.enqueue("🟢 Alarm sunucusu çalışıyor · %d aktif alarm" % n)
        for fn in (self.outbox_loop, self.ntfy_loop):
            threading.Thread(target=fn, daemon=True).start()
        self.poll_loop()


def load_config():
    return {
        "topic": env("NTFY_TOPIC"),
        "ntfy": env("NTFY_SERVER", "https://ntfy.sh").rstrip("/"),
        "tg_token": env("TELEGRAM_BOT_TOKEN"),
        "tg_chat": env("TELEGRAM_CHAT_ID"),
        "tg_api": env("TELEGRAM_API", "https://api.telegram.org").rstrip("/"),
        "binance": env("BINANCE_REST", "https://fapi.binance.com").rstrip("/"),
        "state_file": env("STATE_FILE", "alarm_state.json"),
        "poll": float(env("POLL_SECONDS", "2")),
        "wick_every": float(env("WICK_EVERY", "15")),
        "gap": float(env("GAP_SECONDS", "15")),
        # Telegram bilgisi verilmediyse bildirim ntfy uygulamasına gider (varsayılan); NTFY_PUSH=0 ile kapatılabilir
        "push_ntfy": env("NTFY_PUSH", "0" if (env("TELEGRAM_BOT_TOKEN") and env("TELEGRAM_CHAT_ID")) else "1") == "1",
        "app_url": env("APP_URL"),
        "quiet": env("QUIET", "0") == "1",
        "retry": float(env("OUTBOX_RETRY", "10")),
    }


def main():
    cfg = load_config()
    if not cfg["topic"]:
        sys.exit("NTFY_TOPIC gerekli: uygulamada Alarm > 📱 Telefon bölümündeki kodu ver.")
    if not ((cfg["tg_token"] and cfg["tg_chat"]) or cfg["push_ntfy"]):
        sys.exit("Bildirim kanalı yok: NTFY_PUSH=1 yap ya da TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID ver.")
    srv = Server(cfg)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: srv.stop.set())
    srv.run()
    log("Kapandı")


if __name__ == "__main__":
    main()
