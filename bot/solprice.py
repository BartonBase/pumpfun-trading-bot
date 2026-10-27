import json
import os
import statistics
import threading
import time

import requests

import nethttp

SOURCES = [
    ("coinbase", "https://api.coinbase.com/v2/prices/SOL-USD/spot", lambda j: float(j["data"]["amount"])),
    ("kraken", "https://api.kraken.com/0/public/Ticker?pair=SOLUSD",
     lambda j: float(next(iter(j["result"].values()))["c"][0])),
]
FALLBACK = [
    ("binance_us", "https://api.binance.us/api/v3/ticker/price?symbol=SOLUSDT", lambda j: float(j["price"])),
]


def _local(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts))


class SolPrice:
    def __init__(self, path, cfg=None, log=print):
        c = (cfg or {}).get("sol_price", {}) if isinstance(cfg, dict) else {}
        self.refresh_s = float(c.get("refresh_minutes", 30)) * 60
        self.max_age_s = float(c.get("max_age_hours", 6)) * 3600
        self.path = path
        self.log = log
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "trench-bot/sol-price"})
        self.usd, self.ts, self.sources = None, 0.0, {}
        self.last_try = 0.0
        self._lock = threading.Lock()
        self._thread = None
        try:
            with open(path) as f:
                d = json.load(f)
            self.usd, self.ts, self.sources = float(d["usd"]), float(d["ts"]), d.get("sources", {})
        except Exception:
            pass


    def price(self, now=None):
        now = now or time.time()
        with self._lock:
            return self.usd if self.usd and now - self.ts <= self.max_age_s else None

    def age_s(self, now=None):
        return (now or time.time()) - self.ts if self.ts else None

    def status(self):
        with self._lock:
            return {"usd": self.usd, "fetched_ts": self.ts, "fetched_local": _local(self.ts) if self.ts else None,
                    "sources": dict(self.sources)}


    def _get(self, name, url, parse):
        r = nethttp.request(self.session, "GET", url, read_timeout=6, total=10)
        r.raise_for_status()
        return parse(r.json())

    def fetch(self, why="scheduled"):
        self.last_try = time.time()
        got, errs = {}, []
        for name, url, parse in SOURCES:
            try:
                p = self._get(name, url, parse)
                if 1 < p < 100000:
                    got[name] = p
            except Exception as e:
                errs.append(f"{name}:{type(e).__name__}")
        if not got:
            for name, url, parse in FALLBACK:
                try:
                    p = self._get(name, url, parse)
                    if 1 < p < 100000:
                        got[name] = p
                except Exception as e:
                    errs.append(f"{name}:{type(e).__name__}")
        if not got:
            old = f"keeping ${self.usd:.2f} from {_local(self.ts)}" if self.usd else "no price yet"
            self.log(f"  [sol_usd] fetch FAILED ({why}): {', '.join(errs)}; {old}")
            return None
        px = statistics.median(got.values())
        now = time.time()
        with self._lock:
            self.usd, self.ts, self.sources = px, now, got
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"usd": px, "ts": now, "fetched_local": _local(now), "sources": got}, f)
            os.replace(tmp, self.path)
        except Exception as e:
            self.log(f"  [sol_usd] could not save cache: {type(e).__name__}")
        spread = (max(got.values()) / min(got.values()) - 1) * 100
        self.log(f"  [sol_usd] ${px:.2f} fetched {_local(now)} ({why}) from "
                 + ", ".join(f"{k} ${v:.2f}" for k, v in got.items())
                 + (f"  !! sources disagree by {spread:.1f}%" if spread > 2 else "")
                 + (f"  [errors: {', '.join(errs)}]" if errs else ""))
        return px

    def startup(self):
        age = self.age_s()
        if self.usd is None or age is None or age > self.max_age_s:
            self.fetch("startup: cache " + ("missing" if self.usd is None else f"{age / 3600:.1f} h old"))
        else:
            self.log(f"  [sol_usd] using cached ${self.usd:.2f} fetched {_local(self.ts)} ({age / 60:.0f} min old)")

    def maybe_refresh(self, now=None):
        now = now or time.time()
        if self._thread and self._thread.is_alive():
            return
        due = (not self.ts) or now - self.ts >= self.refresh_s
        if due and now - self.last_try >= 120:
            self.last_try = now
            self._thread = threading.Thread(target=self.fetch, args=("scheduled",), daemon=True, name="sol_usd")
            self._thread.start()
