import json
import queue
import sqlite3
import threading
import time
from datetime import datetime

import requests

HOLDERS_URL = "https://frontend-api-v3.pump.fun/coins/top-holders/{mint}"
TRADES_URL = "https://swap-api.pump.fun/v2/coins/{mint}/trades?limit=100"

SCHEMA = """CREATE TABLE IF NOT EXISTS holder_counts(
  id INTEGER PRIMARY KEY, mint TEXT, symbol TEXT, signal_id INTEGER, signal_ts REAL, signal_action TEXT, fetched_ts REAL,
  total_holders INTEGER, top10_share REAL, top1_share REAL, bundler_share REAL, sniper_share REAL, dev_share REAL,
  unique_buyers INTEGER, unique_buyers_at_signal INTEGER, buys INTEGER, sells INTEGER, buys_at_signal INTEGER,
  trades_seen INTEGER, trades_complete INTEGER, holders_ok INTEGER, trades_ok INTEGER, error TEXT)"""


def _iso(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


class HolderRecorder(threading.Thread):
    def __init__(self, db_path, log=print, max_pages=3, timeout=8, gap=0.25, dedup_seconds=170):
        super().__init__(name="holder-recorder", daemon=True)
        self.db_path, self.log, self.max_pages, self.timeout, self.gap = db_path, log, max_pages, timeout, gap
        self.dedup_seconds = dedup_seconds
        self.q = queue.Queue(maxsize=300)
        self.last = {}
        self.dropped = 0
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "Mozilla/5.0"
        db = sqlite3.connect(db_path, timeout=30)
        db.execute(SCHEMA)
        db.execute("CREATE INDEX IF NOT EXISTS ix_hc_mint ON holder_counts(mint)")
        db.execute("CREATE INDEX IF NOT EXISTS ix_hc_signal ON holder_counts(signal_id)")
        if "bundle_limits" not in [r[1] for r in db.execute("PRAGMA table_info(holder_counts)")]:
            db.execute("ALTER TABLE holder_counts ADD COLUMN bundle_limits TEXT")
        db.commit()
        db.close()

    def submit(self, mint, symbol, signal_id, signal_ts, action, limits=None):
        try:
            if action == "entered" or time.time() - self.last.get(mint, 0) >= self.dedup_seconds:
                self.last[mint] = time.time()
                self.q.put_nowait((mint, symbol, signal_id, signal_ts, action, limits))
        except queue.Full:
            self.dropped += 1
        except Exception:
            pass

    def _get(self, url):
        time.sleep(self.gap)
        r = self.http.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def fetch(self, mint, signal_ts):
        out, err = {"holders_ok": 0, "trades_ok": 0}, []
        try:
            d = self._get(HOLDERS_URL.format(mint=mint))
            hs = d.get("topHolders") or []
            S = 1e9
            out.update(total_holders=d.get("totalHolders"),
                       top10_share=round(sum(float(h.get("amount") or 0) for h in hs[:10]) / S, 4),
                       top1_share=round((float(hs[0].get("amount") or 0) / S) if hs else 0.0, 4),
                       bundler_share=round(sum(float(h.get("amount") or 0) for h in hs if h.get("isBundler")) / S, 4),
                       sniper_share=round(sum(float(h.get("amount") or 0) for h in hs if h.get("isSniper")) / S, 4),
                       dev_share=round(sum(float(h.get("amount") or 0) for h in hs if h.get("isDev")) / S, 4),
                       holders_ok=1 if d.get("totalHolders") is not None else 0)
        except Exception as e:
            err.append(f"holders:{type(e).__name__}")
        try:
            trades, cursor, complete = [], None, False
            for _ in range(self.max_pages):
                d = self._get(TRADES_URL.format(mint=mint) + (f"&cursor={cursor}" if cursor else ""))
                trades += d.get("trades") or []
                pg = d.get("pagination") or {}
                if not pg.get("hasMore"):
                    complete = True
                    break
                cursor = pg.get("nextCursor")
                if not cursor:
                    break
            buys = [t for t in trades if t.get("type") == "buy"]
            at = [t for t in buys if (_iso(t.get("timestamp") or "") or 0) <= signal_ts]
            out.update(unique_buyers=len({t.get("userAddress") for t in buys}),
                       unique_buyers_at_signal=len({t.get("userAddress") for t in at}),
                       buys=len(buys), sells=sum(1 for t in trades if t.get("type") == "sell"),
                       buys_at_signal=len(at), trades_seen=len(trades), trades_complete=int(complete), trades_ok=1)
        except Exception as e:
            err.append(f"trades:{type(e).__name__}")
        out["error"] = ",".join(err) or None
        return out

    def run(self):
        db = sqlite3.connect(self.db_path, timeout=30)
        while True:
            mint, symbol, sid, sts, action, limits = self.q.get()
            try:
                r = self.fetch(mint, sts)
                row = dict(mint=mint, symbol=symbol, signal_id=sid, signal_ts=sts, signal_action=action,
                           fetched_ts=time.time(), bundle_limits=limits, **r)
                db.execute(f"INSERT INTO holder_counts ({','.join(row)}) VALUES ({','.join('?' * len(row))})",
                           tuple(row.values()))
                db.commit()
                self.log(f"  HOLDERS {str(symbol)[:12]:<12} holders {r.get('total_holders')} | buyers {r.get('unique_buyers')} "
                         f"({r.get('unique_buyers_at_signal')} at signal) | buys {r.get('buys')} sells {r.get('sells')}"
                         f"{'' if r.get('trades_complete') else '+'} | {action}" + (f" | {limits}" if limits else "") + (f" | err {r['error']}" if r.get("error") else ""))
            except Exception as e:
                self.log(f"  HOLDERS error {type(e).__name__}: {e}")
