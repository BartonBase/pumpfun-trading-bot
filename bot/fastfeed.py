import os
import sqlite3
import statistics
import time

DEFAULTS = {
    "enabled": True,
    "stream_db": None,
    "check_seconds": 1.0,
    "stale_seconds": 15,
    "band_margin": 0.15,
    "recheck_seconds": 2.0,
    "open_min_seconds": 1.0,
    "max_candidates": 60,
    "add_new_coins": True,
}


class FastFeed:
    def __init__(self, bot, allow_entries=True):
        self.bot = bot
        self.allow_entries = allow_entries
        here = os.path.dirname(os.path.abspath(__file__))
        self.cfgd = dict(DEFAULTS, **{k: v for k, v in (bot.cfg.get("fast_feed") or {}).items() if not k.startswith("_")})
        self.path = os.environ.get("TRENCH_STREAM_DB") or self.cfgd["stream_db"] or \
            os.path.normpath(os.path.join(here, "..", "stream", "stream.db"))
        self.db = None
        self.cursor_ms = int(time.time() * 1000) - 5000
        self.last_eval = {}
        self.last_open_tick = 0.0
        self.last_check = 0.0
        self.lat_new, self.lat_move = [], []
        self.backoff_until = 0.0
        self.stats = {"checks": 0, "new": 0, "evals": 0, "open_ticks": 0}


    @property
    def enabled(self):
        return bool(self.cfgd.get("enabled"))

    def _conn(self):
        if self.db is None:
            if not os.path.exists(self.path):
                return None
            self.db = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=2, check_same_thread=False)
            self.db.row_factory = sqlite3.Row
        return self.db

    def live(self):
        if not self.enabled:
            return False
        try:
            db = self._conn()
            if db is None:
                return False
            r = db.execute("SELECT v FROM meta WHERE k='heartbeat'").fetchone()
            return bool(r) and time.time() - float(r[0]) <= self.cfgd["stale_seconds"]
        except sqlite3.Error:
            self.db = None
            return False

    def status(self):
        def med(xs):
            return round(statistics.median(xs), 2) if xs else None
        return {"live": self.live(), **self.stats, "median_new_s": med(self.lat_new[-500:]),
                "median_move_s": med(self.lat_move[-500:])}


    def rows_since(self):
        db = self._conn()
        rows = db.execute("SELECT * FROM coins WHERE recv_ms > ? ORDER BY recv_ms LIMIT 5000", (self.cursor_ms,)).fetchall()
        if rows:
            self.cursor_ms = max(r["recv_ms"] for r in rows)
        return rows

    def coin_from_stream(self, r, sol_usd):
        cfg = self.bot.cfg
        init = cfg["curve"]["initial_real_token_reserves"] / 10 ** cfg["curve"].get("token_decimals", 6)
        tip = r["tokens_in_pool"]
        curve = max(0.0, min(100.0, (1 - tip / init) * 100)) if (tip is not None and init) else None
        mc = r["mcap_sol"]
        return {"mint": r["mint"], "symbol": (r["symbol"] or "").strip(), "name": (r["name"] or "").strip(),
                "creator": r["creator"], "bonding_curve": r["bonding_curve"], "created_ts": r["created_ms"] / 1000.0,
                "quote_mint": cfg["feed"]["sol_quote_mint"], "mayhem_state": "stream_mayhem" if r["mayhem"] else None,
                "nsfw": 0, "has_socials": 0, "reply_count": 0, "complete": 0, "mcap_sol": mc,
                "mcap_usd": (mc * sol_usd) if (mc and sol_usd) else None, "curve_pct": curve, "real_sol": None,
                "ath_mcap_usd": None, "ath_ts": None, "last_trade_ts": r["last_ms"] / 1000.0}

    def pick(self, rows, now, sol_usd, open_mints):
        e = self.bot.cfg["entry"]
        mg = self.cfgd["band_margin"]
        lo = e["min_mcap_usd"] * (1 - mg) / sol_usd if sol_usd else None
        hi = e["max_mcap_usd"] * (1 + mg) / sol_usd if sol_usd else None
        max_age = e["max_age_minutes"] * 60 + 60
        new, cand, opn = [], [], []
        for r in rows:
            m = r["mint"]
            if m in open_mints:
                opn.append(m)
                continue
            if r["created_ms"] and r["created_ms"] >= (now - 120) * 1000 and r["bonding_curve"]:
                new.append(r)
            mc = r["mcap_sol"]
            if lo is None or mc is None or not (lo <= mc <= hi) or r["mayhem"]:
                continue
            if r["created_ms"] and now - r["created_ms"] / 1000 > max_age:
                continue
            if now - self.last_eval.get(m, 0) < self.cfgd["recheck_seconds"]:
                continue
            cand.append(m)
        return new, cand[: self.cfgd["max_candidates"]], opn

    def tick(self, verdict):
        now = time.time()
        if now - self.last_check < self.cfgd["check_seconds"] or now < self.backoff_until or not self.live():
            return False
        self.last_check = now
        b = self.bot
        try:
            rows = self.rows_since()
            self.stats["checks"] += 1
            sol_usd = b.usd_now(now)
            opens = b.store.open_paper()
            open_mints = {p["mint"] for p in opens}
            new, cand, opn = self.pick(rows, now, sol_usd, open_mints)
            touched = False
            if self.allow_entries and self.cfgd["add_new_coins"] and new:
                for r in new:
                    if b.store.get_coin(r["mint"]) is None:
                        b.store.upsert_coin(self.coin_from_stream(r, sol_usd), now, from_new_feed=True, source="stream")
                        self.lat_new.append(now - r["created_ms"] / 1000.0)
                        self.stats["new"] += 1
                b.store.commit()
                touched = True
            fast_open = b.cfg["poll"].get("fast_open_seconds", 0) or 0
            do_open = bool(open_mints) and (
                (opn and now - self.last_open_tick >= self.cfgd["open_min_seconds"])
                or (fast_open and now - self.last_open_tick >= fast_open))
            evals = []
            if self.allow_entries:
                for m in cand:
                    row = b.store.get_coin(m)
                    if row and row["bonding_curve"] and not row["complete"]:
                        evals.append(m)
            if not (do_open or evals) or not b.rpc:
                return touched
            price = list(open_mints) if do_open else []
            price += [m for m in evals if m not in price]
            rows_by = {m: b.store.get_coin(m) for m in price}
            by_curve = {r["bonding_curve"]: m for m, r in rows_by.items() if r and r["bonding_curve"]}
            t_rpc = time.time()
            states = b.rpc.curve_states(list(by_curve))
            if by_curve and not states:

                self._fails = getattr(self, "_fails", 0) + 1
                self.backoff_until = time.time() + min(30, 2 ** self._fails)
                return touched
            self._fails = 0
            now2 = time.time()
            for curve, stt in states.items():
                b.store.update_from_rpc(by_curve[curve], stt, b.usd_now(now2), now2)
            b.store.commit()
            if do_open:
                self.last_open_tick = now2
                self.stats["open_ticks"] += 1
                b.manage_positions(verdict)
            if evals:
                trade_ms = {r["mint"]: r["last_ms"] for r in rows}
                for m in evals:
                    self.last_eval[m] = now2
                    if trade_ms.get(m):
                        self.lat_move.append(t_rpc - trade_ms[m] / 1000.0)
                self.stats["evals"] += len(evals)
                b.evaluate(evals, verdict)
            if len(self.last_eval) > 20000:
                cut = now2 - 600
                self.last_eval = {k: v for k, v in self.last_eval.items() if v > cut}
            return True
        except Exception as ex:
            print(f"  fast feed error: {type(ex).__name__}: {ex}")
            self.db = None
            return False
