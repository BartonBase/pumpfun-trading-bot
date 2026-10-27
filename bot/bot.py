
import argparse
import threading
import sqlite3
import requests
import nethttp
import solprice
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone

import exits
import fastfeed
import regime
import score
import store as st
from feed import PumpFeed, SolanaRPC, normalize_coin, sol_usd_from_coins
import feed as feedmod
import learn
import metrics
from wallets import RiskBook, WalletIntel


def now_local():
    return time.strftime("%H:%M:%S %Z")


HEARTBEAT = os.path.join(st.DATA_DIR, "heartbeat")


HOLDER_CHECK_DEADLINE_S = 120


def beat(stage, poll=None, worker=None):
    try:
        tmp = HEARTBEAT + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"ts": time.time(), "local": now_local(), "pid": os.getpid(), "poll": poll, "stage": stage,
                       "worker_queue": worker.queue_size() if worker else None,
                       "worker_job": (worker.current[1] if worker and worker.current else None)}, f)
        os.replace(tmp, HEARTBEAT)
    except OSError:
        pass


def needs_holder_unavailable(sc, min_score, check, worker, bundle, cfg):
    return (sc >= min_score and not check and worker is None and bundle is None
            and cfg.get("data_sources", {}).get("require_holder_check", True))


def utc_day_start(ts):
    d = datetime.fromtimestamp(ts, timezone.utc)
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()


def in_band(c, now, cfg):
    e = cfg["entry"]
    if c.get("complete") or c.get("mcap_usd") is None or c.get("curve_pct") is None or not c.get("created_ts"):
        return False
    if "mayhem_state" not in c or c.get("mayhem_state"):
        return False
    if e["sol_quote_only"] and c.get("quote_mint") != cfg["feed"]["sol_quote_mint"] \
            and c.get("quote_mint") not in (e.get("allowed_quote_mints") or []):
        return False
    if not c.get("mcap_sol"):
        return False
    return (e["min_mcap_usd"] <= c["mcap_usd"] <= e["max_mcap_usd"]
            and e["min_curve_pct"] <= c["curve_pct"] <= e["max_curve_pct"]
            and (now - c["created_ts"]) / 60 <= e["max_age_minutes"])


class Bot:
    def __init__(self):
        self.cfg = st.load_config()
        snap = st.snapshot_config_if_changed()
        if snap:
            print(f"config snapshot saved: {os.path.relpath(snap, st.ROOT)}")
        self.cfg_mtime = os.path.getmtime(st.CONFIG_PATH)
        self.store = st.Store()
        self.feed = PumpFeed(self.cfg, self.store)
        self.rpc = SolanaRPC(self.cfg, self.store) if self.cfg["rpc"]["enabled"] else None
        self.wi = WalletIntel(self.cfg, self.rpc or SolanaRPC(self.cfg, self.store), log=print)
        self.wi.db.isolation_level = None
        self.rb = RiskBook(self.wi, self.cfg)
        self.sol_usd = None
        self.sol_usd_ts = 0.0
        self.sol_usd_src = None
        self._sol_warn_ts = 0.0
        self.solpx = solprice.SolPrice(os.path.join(st.DATA_DIR, "sol_usd.json"), self.cfg, log=print)
        self.launch_rate = None
        self.page_rates = []
        self.grad_page_created = []
        self.report = None
        self.last_regime_ts = 0
        self.bundle_cache = {}
        self.awaiting = {}
        self.awaiting_logged = set()
        self.worker = None
        self.fast_checks = 0
        self._regime_thread = None
        self.pump_hc_cache = {}
        self.pump_http = requests.Session()
        self.pump_http.headers["User-Agent"] = "Mozilla/5.0"

        try:
            from holdercounts import HolderRecorder
            self.holder_rec = HolderRecorder(st.DB_PATH, log=print)
            self.holder_rec.start()
        except Exception as e:
            self.holder_rec = None
            print(f"  holder recorder not started: {type(e).__name__}: {e}")
        if self.wi.bundle_check_available:
            from worker import Phase2Worker
            self.worker = Phase2Worker(self.cfg, self.bundle_cache, log=print)
            self.worker.start()


    def maybe_reload_config(self):
        m = os.path.getmtime(st.CONFIG_PATH)
        if m != self.cfg_mtime:
            try:
                new = st.load_config()
            except json.JSONDecodeError as e:
                print(f"!! config.json has a typo ({e}); keeping previous settings")
                self.cfg_mtime = m
                return
            self.cfg = new
            self.cfg_mtime = m
            self.wi.cfg, self.wi.w = new, new["wallets"]
            self.rb.cfg, self.rb.L = new, new["learn"]
            if self.worker:
                self.worker.update_cfg(new)
            snap = st.snapshot_config_if_changed()
            print(f"config.json changed: reloaded" + (f", snapshot {os.path.relpath(snap, st.ROOT)}" if snap else ""))
            self.feed.cfg = new["feed"]

    def effective_mode(self):
        mode = self.cfg.get("mode", "learning")
        if mode != "auto_tune":
            return mode, None
        ok, why = learn.unlock_status(self.store, self.cfg)
        return ("auto_tune", None) if ok else ("learning", why)


    def reconcile_sol_usd(self, sp, now):
        px = getattr(self, "solpx", None)
        ex = px.price(now) if px else None
        tol = float(self.cfg.get("sol_price", {}).get("max_feed_gap_pct", 5)) / 100
        if sp and (not ex or abs(sp / ex - 1) <= tol):
            self.sol_usd, self.sol_usd_ts, self.sol_usd_src = sp, now, "pump.fun feed"
            return None
        if ex:
            if sp and now - getattr(self, "_sol_warn_ts", 0) > 600:
                self._sol_warn_ts = now
                print(f"  [sol_usd] pump.fun implies ${sp:.2f}/SOL, exchange ${ex:.2f} (fetched "
                      f"{time.strftime('%H:%M %Z', time.localtime(px.ts))}): using the exchange price for dollar mcaps")
            self.sol_usd, self.sol_usd_ts, self.sol_usd_src = ex, px.ts, "exchange"
            return ex if sp else None
        return None

    def usd_now(self, now=None):
        now = now or time.time()
        px = getattr(self, "solpx", None)
        cur = getattr(self, "sol_usd", None)
        if cur and now - (getattr(self, "sol_usd_ts", 0) or 0) <= (px.max_age_s if px else 6 * 3600):
            return cur
        ex = px.price(now) if px else None
        if ex:
            self.sol_usd, self.sol_usd_ts, self.sol_usd_src = ex, px.ts, "exchange"
        return ex


    def ingest(self, raws, from_new, rate=False):
        now = time.time()
        touched = []
        if not raws:
            return touched
        sp = sol_usd_from_coins(raws, self.cfg)
        fix = self.reconcile_sol_usd(sp, now)
        for r in raws:
            if not r.get("mint"):
                continue
            c = normalize_coin(r, self.cfg)
            if fix and c["mcap_sol"] and c.get("quote_mint") != feedmod.USDC_MINT:
                c["mcap_usd"] = c["mcap_sol"] * fix
                if c["ath_mcap_usd"]:
                    c["ath_mcap_usd"] *= fix / sp
            self.store.upsert_coin(c, now, from_new_feed=from_new)
            if c["mcap_sol"]:
                self.store.bump_peak(c["mint"], c["mcap_sol"], now)
            touched.append(c["mint"])
        if from_new:
            ts = sorted(r["created_timestamp"] for r in raws if r.get("created_timestamp"))
            if len(ts) > 5 and ts[-1] > ts[0]:
                self.page_rates.append((now, (len(ts) - 1) / ((ts[-1] - ts[0]) / 60000)))
                self.page_rates = [x for x in self.page_rates if now - x[0] < 3600]
                self.launch_rate = statistics.median(r_ for _, r_ in self.page_rates)
        self.store.commit()
        return touched

    def tick_open_positions(self, verdict, stage="fast open-position check"):
        if not self.rpc or not self.store.open_paper():
            beat(stage, None, self.worker)
            return False
        try:
            opens = self.store.open_paper()
            rows = [self.store.get_coin(p["mint"]) for p in opens]
            by_curve = {r["bonding_curve"]: r["mint"] for r in rows if r and r["bonding_curve"]}
            now = time.time()
            states, completed = self.rpc.curve_states_ex(list(by_curve))
            for curve, stt in states.items():
                self.store.update_from_rpc(by_curve[curve], stt, self.usd_now(now), now)


            for curve in completed:
                if curve in by_curve:
                    self.mark_graduated(by_curve[curve], now, "rpc_curve_flag")
            self.store.commit()
            self.manage_positions(verdict)
            self.fast_checks += 1
        except Exception as e:
            print(f"  fast price check error: {type(e).__name__}")
        beat(stage, None, self.worker)
        return True

    def mark_graduated(self, mint, now, source):
        self.store.db.execute("UPDATE coins SET complete=1, complete_seen_ts=COALESCE(complete_seen_ts, ?) WHERE mint=?",
                              (now, mint))
        self.store.db.execute("""CREATE TABLE IF NOT EXISTS grad_exits(paper_id INTEGER PRIMARY KEY, mint TEXT, symbol TEXT,
            detected_ts REAL, detect_source TEXT, pool_first_ts REAL, sell_ts REAL, lag_s REAL, reason TEXT, multiple REAL)""")
        for p in self.store.q("SELECT id, mint, symbol FROM paper WHERE status='open' AND mint=?", (mint,)):
            self.store.db.execute("INSERT OR IGNORE INTO grad_exits(paper_id, mint, symbol, detected_ts, detect_source) "
                                  "VALUES(?,?,?,?,?)", (p["id"], mint, p["symbol"], now, source))

    def fast_wait(self, t0, verdict):
        interval = self.cfg["poll"]["interval_seconds"]
        fast = self.cfg["poll"].get("fast_open_seconds", 0)
        ff = getattr(self, "fastfeed", None)
        last_beat = 0.0
        while True:
            left = interval - (time.time() - t0)
            if left <= 0:
                return
            if ff is not None and ff.live():
                ff.tick(verdict)
                if time.time() - last_beat >= 2:
                    beat("fast feed", None, self.worker)
                    last_beat = time.time()
                time.sleep(min(max(left, 0), 0.2))
                continue
            if not (fast and self.rpc and self.store.open_paper()):
                time.sleep(min(left, fast or left, 1.0 if ff is not None else left))
                continue
            time.sleep(min(left, fast))
            if interval - (time.time() - t0) <= 0.5:
                return
            self.tick_open_positions(verdict)

    def pump_holder_check(self, c):
        hc = self.cfg["data_sources"].get("pump_holders", {})
        try:
            r = nethttp.request(self.pump_http, "GET", hc["url"].format(mint=c["mint"]), read_timeout=hc.get("timeout_seconds", 8))
            if r.status_code != 200:
                return None
            d = r.json()
            hs = d.get("topHolders")
            if not isinstance(hs, list) or not hs:
                return None
        except Exception:
            return None
        S = 1e9
        bc = c.get("bonding_curve")
        hs = [h for h in hs if h.get("address") != bc]
        top10 = sum(float(h.get("amount") or 0) for h in hs[:10]) / S
        top1 = (float(hs[0].get("amount") or 0) / S) if hs else 0.0
        bund = sum(float(h["amount"]) for h in hs if h.get("isBundler")) / S
        snip = sum(float(h["amount"]) for h in hs if h.get("isSniper")) / S
        dev = sum(float(h["amount"]) for h in hs if h.get("isDev")) / S

        why, parts, tripped = [], [], []
        for key, name, val, lim in (("top1", "top holder", top1, hc["max_top1_share"]),
                                    ("top10", "top10", top10, hc["max_top10_share"]),
                                    ("bundsnip", "bundlers+snipers", bund + snip, hc["max_bundler_sniper_share"]),
                                    ("dev", "dev", dev, hc["max_dev_share"])):
            hit = val > lim
            parts.append(f"{key}={val:.0%}>{lim:.0%}" + (" TRIP" if hit else "? no"))
            if hit:
                why.append(f"{name} {val:.0%}")
                tripped.append(key)
        limits = ("bundled[" if why else "clean[") + "; ".join(parts) + "]"
        return {"bundled": bool(why), "holder_src": "pumpfun", "bundle_limits": limits, "bundle_tripped": tripped,
                "reason": (("pump.fun holders: " + ", ".join(why)) if why else
                          f"pump.fun holders ok (top10 {top10:.0%}, bund+snip {bund + snip:.0%}, dev {dev:.0%}, n={d.get('totalHolders')})")
                          + " " + limits,
                "top10_holder_share": round(top10, 4), "top1_share": round(top1, 4), "bundler_share": round(bund, 4),
                "sniper_share": round(snip, 4), "dev_share": round(dev, 4), "total_holders": d.get("totalHolders")}

    def rpc_refresh(self, extra_mints):
        if not self.rpc:
            return 0
        now = time.time()
        L = self.cfg["learn"]
        must = [p["mint"] for p in self.store.open_paper()]
        must += [r["mint"] for r in self.store.q("SELECT mint FROM paper WHERE status='closed' AND close_ts>=?",
                                                 (now - L["track_after_close_minutes"] * 60,))]
        must += [r["mint"] for r in self.store.q(
            """SELECT mint FROM paper WHERE (mcap_1h_sol IS NULL AND entry_ts <= ? AND entry_ts >= ?)
               OR (mcap_6h_sol IS NULL AND entry_ts <= ? AND entry_ts >= ?)""",
            (now - 3600, now - 3600 - L["label_grace_minutes"] * 60, now - 6 * 3600, now - 6 * 3600 - L["label_grace_minutes"] * 60))]
        watch = self.store.q("""SELECT mint FROM coins WHERE band_ts >= ? AND complete=0 ORDER BY band_ts DESC LIMIT ?""",
                             (now - self.cfg["poll"]["watch_minutes_after_band"] * 60, self.cfg["poll"]["max_watch"]))
        mints = list(dict.fromkeys(must + list(extra_mints) + [r["mint"] for r in watch]))
        mints = mints[: self.cfg["poll"]["max_watch"] + len(must)]
        rows = {r["mint"]: r for r in (self.store.get_coin(m) for m in mints) if r and r["bonding_curve"]}
        by_curve = {r["bonding_curve"]: m for m, r in rows.items()
                    if r["quote_mint"] == self.cfg["feed"]["sol_quote_mint"]
                    or r["quote_mint"] in (self.cfg["entry"].get("allowed_quote_mints") or [])}
        states = self.rpc.curve_states(list(by_curve))
        for curve, stt in states.items():
            self.store.update_from_rpc(by_curve[curve], stt, self.usd_now(now), now)
        self.store.commit()
        return len(states)

    def fill_labels(self):
        now = time.time()
        grace = self.cfg["learn"]["label_grace_minutes"] * 60
        for p in self.store.q("SELECT * FROM paper WHERE mcap_1h_sol IS NULL OR mcap_6h_sol IS NULL"):
            c = self.store.get_coin(p["mint"])
            if not c or not c["mcap_sol"] or not p["entry_mcap_sol"]:
                continue
            fresh = (c["last_seen_ts"] or 0) >= now - 120
            upd = {}
            for h, col in ((1, "1h"), (6, "6h")):
                due = p["entry_ts"] + h * 3600
                if p[f"mcap_{col}_sol"] is None and due <= now <= due + grace and fresh:
                    upd[f"mcap_{col}_sol"] = c["mcap_sol"]
                    upd[f"mult_{col}"] = round(c["mcap_sol"] / p["entry_mcap_sol"], 4)
            if c["complete"] and not p["later_graduated"]:
                upd["later_graduated"] = 1
            if upd:
                self.store.update_paper(p["id"], **upd)
        self.store.commit()


    def refresh_regime(self, deep=True):
        if deep:
            return self._start_regime_deep()
        return self._finish_regime(self.store, deep=False, grad_page_created=self.grad_page_created)

    def _start_regime_deep(self):
        if getattr(self, "_regime_thread", None) and self._regime_thread.is_alive():
            print("  regime deep refresh already running in background")
            return self.report
        def run():
            try:
                self._regime_deep_worker()
            except Exception as e:
                print(f"  regime background error: {type(e).__name__}: {e}")
            finally:
                self._regime_thread = None
        self._regime_thread = threading.Thread(target=run, name="regime-deep", daemon=True)
        self._regime_thread.start()
        print("  regime deep refresh started in background (exits keep running)")
        return self.report

    def _regime_deep_worker(self):

        bg = st.Store()

        feed = PumpFeed(self.cfg, store=bg)
        now = time.time()
        for off in self.cfg["feed"]["history_sample_offsets"]:
            raw = feed.newest(off)
            if raw is not None:
                for r in raw:
                    if not r.get("mint"):
                        continue
                    c = normalize_coin(r, self.cfg)
                    bg.upsert_coin(c, now, from_new_feed=True)
                    if c.get("mcap_sol"):
                        bg.bump_peak(c["mint"], c["mcap_sol"], now)
                bg.commit()
            beat(f"regime: history page {off}", worker=self.worker)
        created = []
        for i in range(self.cfg["feed"]["graduated_pages"]):
            g = feed.graduated(i * self.cfg["feed"]["page_limit"])
            if g is None:
                created = None
                break
            now = time.time()
            for r in g:
                if not r.get("mint"):
                    continue
                c = normalize_coin(r, self.cfg)
                bg.upsert_coin(c, now, from_new_feed=False)
            bg.commit()
            beat(f"regime: graduated page {i}", worker=self.worker)
            created += [r["created_timestamp"] / 1000 for r in g if r.get("created_timestamp")]
        if created is not None:
            self.grad_page_created = created
        if self.worker:
            self.worker.learn()
        self._finish_regime(bg, deep=True, grad_page_created=self.grad_page_created)
        print(f"  regime refreshed (bg) -> {self.report['verdict'].upper()} (data/regime.md)")

    def _gradtrack_worker(self):
        bg = st.Store()
        feed = PumpFeed(self.cfg, store=bg)
        for off in self.cfg["feed"]["history_sample_offsets"]:
            raw = feed.newest(off)
            if raw is not None:
                now = time.time()
                for r in raw:
                    if not r.get("mint"):
                        continue
                    c = normalize_coin(r, self.cfg)
                    bg.upsert_coin(c, now, from_new_feed=True)
                    if c.get("mcap_sol"):
                        bg.bump_peak(c["mint"], c["mcap_sol"], now)
                bg.commit()
        n = 0
        for i in range(self.cfg["feed"]["graduated_pages"]):
            g = feed.graduated(i * self.cfg["feed"]["page_limit"])
            if g is None:
                break
            now = time.time()
            for r in g:
                if r.get("mint"):
                    bg.upsert_coin(normalize_coin(r, self.cfg), now, from_new_feed=False); n += 1
            bg.commit()
        print(f"  grad bookkeeping (bg): {n} graduated rows read")

    def _start_gradtrack(self):
        t = getattr(self, "_grad_thread", None)
        if t and t.is_alive():
            return
        def run():
            try:
                self._gradtrack_worker()
            except Exception as e:
                print(f"  grad bookkeeping error: {type(e).__name__}: {e}")
        self._grad_thread = threading.Thread(target=run, name="gradtrack", daemon=True)
        self._grad_thread.start()

    def _finish_regime(self, store, deep, grad_page_created):
        extra = {"sol_usd": self.sol_usd, "launch_rate": self.launch_rate, "sources": self.source_status(),
                 "grad_page_created": grad_page_created}
        if self.wi.bundle_check_available:
            extra["trade_data_fn"] = self.trade_data_summary
        rep = regime.compute(store, self.cfg, extra=extra)
        store.add_regime(rep["verdict"], rep["windows"].get("24h", {}).get("verdict"), rep)
        store.commit()
        regime.write(rep)
        self.report = rep
        self.last_regime_ts = time.time()
        if deep and self.cfg.get("mode") == "auto_tune":
            mode, why = self.effective_mode()
            if mode == "auto_tune":
                self.run_auto_tune(rep)
            else:
                print(f"  [auto_tune] LOCKED, acting as learning: {why}")
        return rep






    def _shadow_db(self):
        db = getattr(self, "_sdb", None)
        if db is None:
            db = sqlite3.connect(st.DB_PATH, timeout=30, isolation_level=None)
            db.execute("""CREATE TABLE IF NOT EXISTS shadow_serial(id INTEGER PRIMARY KEY, mint TEXT UNIQUE, symbol TEXT,
                signal_id INTEGER, n_prior INTEGER, entry_ts REAL, entry_mcap_sol REAL, entry_mcap_usd REAL, curve_pct REAL,
                total_holders INTEGER, top10_share REAL, bundler_share REAL, sniper_share REAL, dev_share REAL, bundle_limits TEXT,
                last_ts REAL, last_mult REAL, peak_mult REAL, status TEXT, exit_ts REAL, exit_reason TEXT, exit_mult REAL, pnl_sol REAL)""")
            self._sdb = db
        return db

    def shadow_serial_add(self, c, sid, now, n_prior, bundle):
        try:
            b = bundle or {}
            cur = self._shadow_db().execute("""INSERT OR IGNORE INTO shadow_serial(mint,symbol,signal_id,n_prior,entry_ts,
                entry_mcap_sol,entry_mcap_usd,curve_pct,total_holders,top10_share,bundler_share,sniper_share,dev_share,
                bundle_limits,last_ts,last_mult,peak_mult,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1.0,1.0,'open')""",
                (c["mint"], c["symbol"], sid, n_prior, now, c.get("mcap_sol"), c.get("mcap_usd"), c.get("curve_pct"),
                 b.get("total_holders"), b.get("top10_holder_share"), b.get("bundler_share"), b.get("sniper_share"),
                 b.get("dev_share"), b.get("bundle_limits"), now))
            if cur.rowcount:
                print(f"  SHADOW open  {c['symbol'][:12]:<12} creator prior {n_prior} | ${c.get('mcap_usd') or 0:,.0f} "
                      f"curve {c.get('curve_pct') or 0:.0f}% | skipped (serial_creator), shadow-tracked only (holders via holder_counts signal {sid})")
        except Exception as ex:
            print(f"  SHADOW add error: {ex}")

    def shadow_serial_tick(self):
        try:
            db = self._shadow_db()
            now = time.time()
            for sid_, mint, sym, ets, emc, last_m, peak in db.execute(
                    "SELECT id,mint,symbol,entry_ts,entry_mcap_sol,last_mult,peak_mult FROM shadow_serial WHERE status='open'").fetchall():
                r = db.execute("SELECT mcap_sol,complete,last_seen_ts FROM coins WHERE mint=?", (mint,)).fetchone()
                why, m = None, last_m
                if r and r[0] and emc:
                    if r[1]:
                        why = "graduation"
                    else:
                        m = r[0] / emc
                        if m <= 1 - self.cfg["exits"]["hard_stop_pct"] / 100:
                            why = "stop"
                if not why and now - ets >= self.cfg["exits"]["time_stop_minutes"] * 60:
                    why = "time_stop"
                if not why and r and r[2] and now - r[2] > 1800:
                    why = "stale"
                peak = max(peak or 1.0, m or 0)
                if why:
                    sz = self.cfg["paper"]["size_sol"]; fee = 1 - self.cfg["paper"]["fee_pct"] / 100
                    pnl = sz * m * fee * fee - sz
                    db.execute("UPDATE shadow_serial SET last_ts=?,last_mult=?,peak_mult=?,status='closed',exit_ts=?,exit_reason=?,"
                               "exit_mult=?,pnl_sol=? WHERE id=?", (now, m, peak, now, why, m, pnl, sid_))
                    print(f"  SHADOW close {sym[:12]:<12} {why} at {m:.2f}x after {(now - ets) / 60:.1f}m | peak {peak:.2f}x | "
                          f"would-be PnL {pnl:+.4f} SOL (0.15 SOL, not traded)")
                else:
                    db.execute("UPDATE shadow_serial SET last_ts=?,last_mult=?,peak_mult=? WHERE id=?", (now, m, peak, sid_))
        except Exception as ex:
            print(f"  SHADOW tick error: {ex}")

    def run_auto_tune(self, rep):
        diffs, _ = collect_suggestions(self.cfg, self.store, auto=True)
        applied = learn.auto_tune(diffs, self.cfg, rep.get("stand_down_warning") or rep["verdict"] == "stand_down")
        for k, old, new, why in applied:
            print(f"  [auto_tune] {k}: {old} -> {new}  ({why})")
        if applied:
            self.maybe_reload_config()

    def source_status(self):
        f = self.feed.stats
        return {
            "pump.fun /coins feed": f"ok={f['ok']} rate_limited={f['rate_limited']} errors={f['error']} (since start)",
            "Solana RPC (prices of watched coins)": (self.rpc.label if self.rpc else "disabled"),
            "Helius / trade stream": self.wi.status_line(),
            "SolScanner": "not used: no public data API (only a paid, key-gated embed iframe)",
            "Third-party wallet lists": "none used (no KOL / leaderboard / tracked-wallet feeds)",
        }

    def trade_data_summary(self, start):
        if not self.wi.bundle_check_available:
            return None
        w = self.cfg["wallets"]
        out = {"available": True, "first_slots": w["first_slots"], "window_minutes": w["trade_profile_minutes"]}
        profs = [dict(r) for r in self.wi.db.execute("SELECT * FROM trade_profiles WHERE created_ts>=?", (start,))]

        def med(k):
            v = [p[k] for p in profs if p.get(k) is not None]
            return round(statistics.median(v), 4) if v else None
        out.update(profiled_coins=len(profs), capped=sum(p["capped"] for p in profs),
                   median_first_slot_supply_share=med("first_slot_supply_share"),
                   median_first_slot_supply_share_ex_creator=med("first_slot_supply_share_ex_creator"),
                   median_unique_buyers_window=med("unique_buyers_window"),
                   median_trade_count_window=med("trade_count"),
                   coins_with_multi_wallet_sigs=sum(1 for p in profs if (p.get("multi_wallet_sigs") or 0) > 0),
                   median_multi_wallet_supply_share=med("multi_wallet_supply_share"))
        rows = self.wi.db.execute("SELECT bundled, detail FROM coin_bundle WHERE ts>=?", (start,)).fetchall()
        if rows:
            shares = [json.loads(r["detail"]).get("largest_cluster_share") for r in rows]
            shares = [s_ for s_ in shares if s_ is not None]
            out.update(coins_cluster_checked=len(rows),
                       bundled_share_of_checked=round(sum(r["bundled"] for r in rows) / len(rows), 4),
                       median_largest_funding_cluster_share=round(statistics.median(shares), 4) if shares else None)
        n_book = self.wi.db.execute("SELECT COUNT(*) FROM wallet_stats WHERE in_book=1").fetchone()[0]
        tiers = dict(self.wi.db.execute("SELECT tier, COUNT(*) FROM wallet_tiers GROUP BY tier").fetchall())
        out["wallet_book"] = (f"{n_book} wallets in book; risk-book tiers: "
                              + (", ".join(f"{k}={v}" for k, v in sorted(tiers.items())) or "none yet"))
        out["reason"] = ("sampled band coins via Helius RPC (sample, not every launch)" if profs
                         else "Helius key present; no band coin old enough to profile yet")
        return out







    def _post_exit_init(self):
        if getattr(self, "_pe_ready", False):
            return
        self.store.db.execute("""CREATE TABLE IF NOT EXISTS post_exit(paper_id INTEGER PRIMARY KEY, mint TEXT, symbol TEXT,
            entry_ts REAL, close_ts REAL, entry_mcap_sol REAL, entry_mcap_usd REAL, exit_mult REAL,
            held_peak_mult REAL, held_peak_ts REAL, post_peak_mult REAL, post_peak_ts REAL,
            last_ts REAL, last_mult REAL, low_since_ts REAL, status TEXT, done_reason TEXT, source TEXT)""")
        self.store.commit()
        self._pe_ready, self._pe_last = True, 0.0

    def post_exit_tick(self):
        try:
            self._post_exit_init()
            now = time.time()
            pe = self.cfg.get("post_exit") or {}
            hours, every = float(pe.get("track_hours", 6)), float(pe.get("every_seconds", 60))
            since = (st.load_baseline() or {}).get("ts") if hasattr(st, "load_baseline") else None
            since = since or (now - 24 * 3600)
            db = self.store.db

            for p in self.store.q("""SELECT * FROM paper WHERE status='closed' AND entry_ts>=? AND close_ts IS NOT NULL
                                     AND id NOT IN (SELECT paper_id FROM post_exit)""", (since,)):
                ex = self.store.one("SELECT multiple FROM exits WHERE paper_id=? ORDER BY ts DESC, id DESC LIMIT 1", (p["id"],))
                hk = self.store.one("""SELECT ts, mult FROM paper_ticks WHERE paper_id=? AND ts<=? AND mult IS NOT NULL
                                       ORDER BY mult DESC, ts ASC LIMIT 1""", (p["id"], p["close_ts"]))
                pk = self.store.one("""SELECT ts, mult FROM paper_ticks WHERE paper_id=? AND ts>? AND mult IS NOT NULL
                                       ORDER BY mult DESC, ts ASC LIMIT 1""", (p["id"], p["close_ts"]))
                lk = self.store.one("SELECT ts, mult FROM paper_ticks WHERE paper_id=? ORDER BY ts DESC LIMIT 1", (p["id"],))
                c = self.store.get_coin(p["mint"])
                done = None
                if now - p["close_ts"] > hours * 3600:
                    done = "window_over"
                elif c and c["complete"] and (c["complete_seen_ts"] or now) <= p["close_ts"]:
                    done = "graduated_while_held"
                db.execute("""INSERT OR IGNORE INTO post_exit(paper_id,mint,symbol,entry_ts,close_ts,entry_mcap_sol,entry_mcap_usd,
                    exit_mult,held_peak_mult,held_peak_ts,post_peak_mult,post_peak_ts,last_ts,last_mult,status,done_reason,source)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p["id"], p["mint"], p["symbol"], p["entry_ts"], p["close_ts"], p["entry_mcap_sol"], p["entry_mcap_usd"],
                     ex["multiple"] if ex else p["last_mult"], p["peak_mult"], hk["ts"] if hk else p["entry_ts"],
                     pk["mult"] if pk else None, pk["ts"] if pk else None, lk["ts"] if lk else None, lk["mult"] if lk else None,
                     "done" if done else "tracking", done, "backfill_ticks" if p["close_ts"] < now - 120 else "live"))
            self.store.commit()

            if now - self._pe_last < every:
                return
            self._pe_last = now
            rows = self.store.q("SELECT * FROM post_exit WHERE status='tracking'")
            if not rows:
                return
            coins = {r["paper_id"]: self.store.get_coin(r["mint"]) for r in rows}
            by_curve = {}
            for r in rows:
                c = coins[r["paper_id"]]
                if c and c["bonding_curve"]:
                    by_curve.setdefault(c["bonding_curve"], []).append(r)
            states, completed = self.rpc.curve_states_ex(list(by_curve)) if self.rpc else ({}, set())
            for curve, rs in by_curve.items():
                for r in rs:
                    m, done = None, None
                    stt = states.get(curve)
                    if curve in completed or (stt and stt["complete"]):
                        pm = self.pumpswap_mcap_sol(r["mint"])
                        m = (pm / r["entry_mcap_sol"]) if (pm and r["entry_mcap_sol"]) else None
                        done = "graduated"
                    elif stt and r["entry_mcap_sol"]:
                        m = stt["mcap_sol"] / r["entry_mcap_sol"]
                    upd = {}
                    if m is not None:
                        upd.update(last_ts=now, last_mult=m)
                        if r["post_peak_mult"] is None or m > r["post_peak_mult"]:
                            upd.update(post_peak_mult=m, post_peak_ts=now)
                        if m < 0.10:
                            low = r["low_since_ts"] or now
                            upd["low_since_ts"] = low
                            if now - low > 3600:
                                done = done or "dead"
                        elif r["low_since_ts"]:
                            upd["low_since_ts"] = None
                    if now - r["close_ts"] > hours * 3600:
                        done = done or "window_over"
                    if done:
                        upd.update(status="done", done_reason=done)
                        pk = upd.get("post_peak_mult", r["post_peak_mult"])
                        print(f"  POSTEXIT {r['symbol'][:12]:<12} {done}: exit {r['exit_mult'] or 0:.2f}x | held peak "
                              f"{r['held_peak_mult'] or 0:.2f}x | after-exit peak {(pk or 0):.2f}x")
                    if upd:
                        db.execute(f"UPDATE post_exit SET {', '.join(k + '=?' for k in upd)} WHERE paper_id=?",
                                   (*upd.values(), r["paper_id"]))
            self.store.commit()
        except Exception as ex:
            print(f"  POSTEXIT error {type(ex).__name__}: {ex}")

    def log_post_close_ticks(self):
        now = time.time()
        for p in self.store.q("SELECT * FROM paper WHERE status='closed' AND close_ts>=?",
                              (now - self.cfg["learn"]["track_after_close_minutes"] * 60,)):
            c = self.store.get_coin(p["mint"])
            last = self.store.one("SELECT MAX(ts) t FROM paper_ticks WHERE paper_id=?", (p["id"],))["t"] or 0
            if c and c["mcap_sol"] and p["entry_mcap_sol"] and (c["last_seen_ts"] or 0) > last:
                self.store.add_tick(p["id"], c["last_seen_ts"], c["mcap_sol"] / p["entry_mcap_sol"],
                                    "post:" + (c["price_source"] or ""))
        self.store.commit()


    def manage_positions(self, verdict):
        now = time.time()
        p_cfg, ex_cfg = self.cfg["paper"], self.cfg["exits"]
        for p in self.store.open_paper():
            c = self.store.get_coin(p["mint"])
            stale = (now - ((c["last_seen_ts"] if c else 0) or 0)) / 60 > p_cfg["stale_price_minutes"]
            mult = (c["mcap_sol"] / p["entry_mcap_sol"]) if c and c["mcap_sol"] and p["entry_mcap_sol"] else p["last_mult"]
            state = {"entry_ts": p["entry_ts"], "remaining": p["remaining_frac"], "peak": p["peak_mult"],
                     "tp1": bool(p["tp1_done"]), "tp2": bool(p["tp2_done"])}
            if c and not stale and c["mcap_sol"]:
                self.store.add_tick(p["id"], now, mult, c["price_source"])
            acts = []

            pcc = self.cfg.get("realistic_exits") or {}
            pb_stale = self.__dict__.setdefault("pb_stale", {})
            pb_grad = self.__dict__.setdefault("pb_grad", {})
            pend0 = p["pending_frac"] or 0.0
            grad_mult, grad_reason = None, None
            if c and c["complete"]:
                self.mark_graduated(p["mint"], now, "position_check")
                stale = False
            if stale:


                since = pb_stale.setdefault(p["id"], now)
                if (now - since) / 60 < float(pcc.get("stale_hard_close_minutes", 60)):
                    continue
                acts.append({"reason": "stale_no_price", "frac": state["remaining"] + pend0, "mult": 0.0})
                state = dict(state, remaining=0)
                p = dict(p, pending_frac=0.0)
                pb_stale.pop(p["id"], None)
            elif c and c["complete"]:


                pm = self.pumpswap_mcap_sol(c["mint"])
                first = pb_grad.setdefault(p["id"], now)
                if pm:
                    self.store.db.execute("UPDATE grad_exits SET pool_first_ts=COALESCE(pool_first_ts, ?) WHERE paper_id=?",
                                          (now, p["id"]))
                    grad_mult, grad_reason = pm / p["entry_mcap_sol"], "graduated_exit"
                elif (now - first) / 60 < float(pcc.get("graduation_pool_wait_minutes", 5)):
                    continue
                else:
                    grad_mult, grad_reason = mult, "graduated_exit_curve"
                mult = grad_mult
            pending = p["pending_frac"] or 0.0
            if pending > 1e-9 and not stale:
                acts.append({"reason": f"{p['pending_reason']}_retry", "frac": pending, "mult": mult, "retry": True})
                pending = 0.0
            if acts and acts[0]["reason"] == "stale_no_price":
                pending = 0.0
            elif grad_reason:
                acts.append({"reason": grad_reason, "frac": state["remaining"], "mult": grad_mult})
                g = self.store.one("SELECT detected_ts, detect_source FROM grad_exits WHERE paper_id=?", (p["id"],))
                det = (g["detected_ts"] if g else None) or (c["complete_seen_ts"] if c else None) or now
                self.store.db.execute("UPDATE grad_exits SET sell_ts=?, lag_s=?, reason=?, multiple=? WHERE paper_id=?",
                                      (now, now - det, grad_reason, grad_mult, p["id"]))
                print(f"  GRAD   {p['symbol'][:12]:<12} detected {time.strftime('%H:%M:%S', time.localtime(det))} "
                      f"({g['detect_source'] if g else 'feed'}) sold {time.strftime('%H:%M:%S', time.localtime(now))} "
                      f"lag {now - det:.1f}s @ {grad_mult:.2f}x ({grad_reason})")
                state = dict(state, remaining=0)
                pb_grad.pop(p["id"], None)
            elif p["id"] in pb_stale:

                pb_stale.pop(p["id"], None)
                acts.append({"reason": "stale_price", "frac": state["remaining"], "mult": mult})
                state = dict(state, remaining=0)
            else:

                ec = self.__dict__.setdefault("early_checked_ids", set())
                state["early_checked"] = p["id"] in ec or (now - p["entry_ts"]) > 90
                new_acts, state = exits.step(state, mult, now, ex_cfg)
                if state.get("early_checked"):
                    ec.add(p["id"])
                acts += new_acts
            proceeds = p["proceeds_sol"]
            fees = p["fees_sol"] or exits.entry_cost(p_cfg)
            mark = p["mark_value_sol"] or 0.0
            failed = p["failed_sells"] or 0
            n_prev = self.store.one("SELECT COUNT(*) n FROM exits WHERE paper_id=?", (p["id"],))["n"]
            t2x, tstop, s1 = p["time_to_2x_min"], p["time_to_stop_min"], p["scale1_sold"] or 0
            pending_reason = p["pending_reason"]
            for k, a in enumerate(acts):
                mins = (now - p["entry_ts"]) / 60
                if not a.get("retry") and a["reason"] not in ("stale_price", "graduated_exit", "graduated_exit_curve", "stale_no_price") and \
                        exits.sell_fails(p["id"], n_prev + k, p_cfg["failed_sell_rate"]):
                    pending += a["frac"]
                    pending_reason = a["reason"]
                    failed += 1
                    if a["reason"] == "scale1":
                        s1 = 0
                    self.store.add_exit({"paper_id": p["id"], "ts": now, "reason": a["reason"] + "_failed",
                                         "frac_sold": 0, "multiple": a["mult"], "proceeds_sol": 0, "minutes_in": mins,
                                         "regime_verdict": verdict, "score_reasons": p["score_reasons"]})
                    print(f"  EXIT   {p['symbol'][:12]:<12} {a['reason']:<14} FAILED (modelled {p_cfg['failed_sell_rate']:.0%} "
                          f"failed-sell rate); retry next poll")
                    continue
                got = exits.proceeds_sol(p["size_sol"], a["frac"], a["mult"], p_cfg)
                mk = exits.mark_value(p["size_sol"], a["frac"], a["mult"])
                proceeds += got
                mark += mk
                fees += p_cfg.get("priority_fee_sol", 0) + mk * p_cfg["fee_pct"] / 100 * 2
                if a["reason"].startswith("scale1"):
                    s1 = 1
                    t2x = t2x or mins
                if a["reason"].startswith(("hard_stop", "stall", "time_stop", "trail", "early_stop", "early_check")):
                    tstop = tstop or mins
                self.store.add_exit({"paper_id": p["id"], "ts": now, "reason": a["reason"], "frac_sold": a["frac"],
                                     "multiple": a["mult"], "proceeds_sol": got, "minutes_in": mins,
                                     "regime_verdict": verdict, "score_reasons": p["score_reasons"]})
                print(f"  EXIT   {p['symbol'][:12]:<12} {a['reason']:<14} sold {a['frac']:.0%} @ {a['mult']:.2f}x "
                      f"-> {got:.4f} SOL  ({mins:.1f} min in)")
            remaining_total = state["remaining"] + pending
            upd = dict(remaining_frac=state["remaining"], peak_mult=state["peak"], tp1_done=int(state["tp1"]),
                       tp2_done=int(state["tp2"]), proceeds_sol=proceeds, last_mult=mult,
                       last_price_ts=c["last_seen_ts"] if c else p["last_price_ts"], pending_frac=pending,
                       pending_reason=pending_reason, failed_sells=failed, fees_sol=fees, mark_value_sol=mark,
                       time_to_2x_min=t2x, time_to_stop_min=tstop, scale1_sold=s1)
            if remaining_total <= 1e-9:
                sol_in = p["size_sol"] + exits.entry_cost(p_cfg)
                upd.update(status="closed", close_ts=now, pnl_sol=proceeds - sol_in, sol_in=sol_in, sol_out=proceeds,
                           slippage_sol=max(0.0, mark - proceeds - (fees - exits.entry_cost(p_cfg))),
                           exit_reason_final=acts[-1]["reason"] if acts else "unknown")
                print(f"  CLOSED {p['symbol'][:12]:<12} PnL {proceeds - sol_in:+.4f} SOL")
            self.store.update_paper(p["id"], **upd)
        self.store.commit()


    def signal_features(self, c, now, dup, bundle, hits):
        early = (bundle or {}).get("early") or {}
        n_prior, g_prior = self.store.creator_prior(c.get("creator"), c.get("created_ts"))
        ath = c.get("ath_mcap_usd")
        return {
            "real_sol": c.get("real_sol"), "mayhem": 1 if c.get("mayhem_state") else 0,
            "ath_retained": round(min(1.0, c["mcap_usd"] / ath), 4) if ath else None,
            "dup_ticker": int(bool(dup)),
            "unique_buyers": early.get("unique_buyers_window"), "trade_count": early.get("trade_count"),
            "same_block_supply_share": early.get("first_slot_supply_share"),
            "multi_wallet_sig_supply_share": early.get("multi_wallet_supply_share"),
            "creator_funded_share": (bundle or {}).get("creator_share"),
            "top10_holder_share": (bundle or {}).get("top10_holder_share"),
            "qualified_wallets": (hits or {}).get("follow"),
            "creator_prior_grad_rate": g_prior, "creator_prior_coins": n_prior,
            "missing": [k for k, v in (("trade data", early), ("holder check", bundle)) if not v],
        }

    def evaluate(self, mints, verdict):
        now = time.time()
        cfg = self.cfg
        min_score = score.min_score_for(verdict, cfg)
        prior = (self.report or {}).get("windows", {}).get("6h", {}).get("what_worked")
        skips, scored = {}, []
        open_n = len(self.store.open_paper())
        day_pnl = self.store.closed_pnl_since(utc_day_start(now))
        for m in dict.fromkeys(mints):
            row = self.store.get_coin(m)
            if not row:
                continue
            c = dict(row)
            if in_band(c, now, cfg):
                self.store.mark_band(m, now, c["mcap_usd"], c["mcap_sol"], c["curve_pct"], c["reply_count"],
                                     (now - c["created_ts"]) / 60)
            dup = self.store.dup_ticker_exists(m, c["symbol"], c["created_ts"] or now,
                                               cfg["entry"]["dup_ticker_window_minutes"] * 60)
            why = score.hard_filter(c, now, cfg, dup_ticker=dup)
            if why:
                skips[why] = skips.get(why, 0) + 1
                if m in self.awaiting_logged:
                    self.awaiting_logged.discard(m)
                    sc0, bd0 = score.score_coin(c, now, cfg, prior=prior, bundle=None)
                    self.store.add_signal({"ts": now, "mint": m, "symbol": c["symbol"], "score": sc0, "min_score": min_score,
                                           "label": "filter_only", "breakdown": bd0, "regime_verdict": verdict,
                                           "action": f"filtered:{why}", "mcap_usd": c["mcap_usd"], "curve_pct": c["curve_pct"],
                                           "age_min": (now - c["created_ts"]) / 60, "reply_count": c["reply_count"],
                                           "features": {}})
                    print(f"  RESOLVED {c['symbol'][:12]} left the band while awaiting its holder check -> filtered:{why}")
                continue
            if cfg["score"].get("gate_entries", True) is False:
                if self.evaluate_first_seen(c, now, cfg, verdict, prior, dup, open_n, day_pnl, scored):
                    open_n += 1
                continue
            sc, bd = score.score_coin(c, now, cfg, prior=prior, bundle=None)
            bundle, hits, check = None, None, None

            sk = cfg.get("skips") or {}
            n_prior = (self.store.creator_prior(c.get("creator"), c.get("created_ts"))[0]
                       if sk.get("serial_creator_at") is not None else None)
            ftags = score.filter_tags(c, now, cfg, n_prior, dup_ticker=dup)
            pre = score.pre_skip(c, now, cfg, n_prior)
            if pre:
                bd.append([f"SKIP {pre[0]}: {pre[1]}", 0])

            hbonus = score.holder_bonus_max(cfg)
            margin = cfg["learn"]["wallet_check_margin"] if self.rb.available else 0
            if not pre and sc + hbonus >= min_score - margin and self.worker:
                recheck = cfg["signal"]["bundle_recheck_minutes"] * 60
                qualified = sc + hbonus >= min_score
                hit = self.bundle_cache.get(m)
                first = self.awaiting.get(m)
                if first and now - first > HOLDER_CHECK_DEADLINE_S and now - first < recheck:
                    check = "timeout"
                elif hit and now - hit[0] < recheck and hit[1] is not None:
                    bundle = hit[1]
                elif hit and now - hit[0] < recheck:
                    check = "failed"
                else:
                    if hit and hit[1] is not None:
                        bundle = hit[1]
                    else:
                        check = "awaiting"
                        if qualified and (not first or now - first >= recheck):
                            self.awaiting[m] = now

                    self.worker.request_bundle(c, urgent=qualified, score=sc)
                hits = self.rb.hits(bundle, c.get("creator")) if (self.rb.available and bundle) else None
                sc, bd = score.score_coin(c, now, cfg, prior=prior, bundle=bundle, wallet_hits=hits)
            phc = cfg["data_sources"].get("pump_holders", {})
            if not pre and sc + hbonus >= min_score and self.worker is None and bundle is None and not check and phc.get("enabled"):
                hit = self.pump_hc_cache.get(m)
                if hit and now - hit[0] < phc.get("cache_seconds", 60):
                    bundle = hit[1]
                else:
                    bundle = self.pump_holder_check(c)
                    self.pump_hc_cache[m] = (now, bundle)
                if bundle is None:
                    check = "failed"
                else:
                    sc, bd = score.score_coin(c, now, cfg, prior=prior, bundle=bundle, wallet_hits=None)
            hskip = score.holder_count_skip(bundle, cfg) if bundle else None
            htag = score.holder_count_tag(bundle, cfg) if bundle else None
            tags = [t for t, _ in ftags] + ([htag] if htag else [])
            if tags and not pre and not hskip:
                bd.append([f"WOULD-HIT old filters (logged only, not blocking): {', '.join(tags)}", 0])
            if needs_holder_unavailable(sc, min_score, check, self.worker, bundle, cfg):
                check = "unavailable"
            confirm = self.wi.wallet_confirm(c) if (sc >= min_score and not check) else None
            label = "wallet_confirm" if confirm else "filter_only"
            if self.worker is None and bundle is None and not check and sc >= min_score:
                label = "no_holder_check"
            elif bundle and bundle.get("holder_src") == "pumpfun":
                label = "holder_pumpfun"
            if self.store.ever_traded(m):
                action = "already_traded"
            elif verdict == "stand_down":
                action = "regime_stand_down"
            elif pre:
                action = pre[0]
            elif sc < min_score:
                action = "below_min_score"
            elif bundle and bundle.get("bundled") and cfg["score"]["bundle_hard_skip"]:
                action = "bundled_skip"
            elif hskip:
                action = hskip
            elif check == "awaiting":
                action = "awaiting_holder_check"
            elif check == "timeout":
                action = "holder_check_timeout"
            elif check == "failed":
                action = "holder_check_failed"
            elif check == "unavailable":
                action = "holder_check_unavailable"
            elif cfg["signal"]["require_wallet_confirm"] and label != "wallet_confirm":
                action = "wallet_confirm_required"
            elif open_n >= cfg["paper"]["max_open"]:
                action = "max_open"
            elif cfg["paper"].get("daily_loss_cap_sol") is not None and day_pnl <= -cfg["paper"]["daily_loss_cap_sol"]:
                action = "daily_loss_cap"
            else:
                action = "entered"
            last = self.store.last_signal_ts(m)
            if action == "already_traded" and last:
                continue
            resolving = m in self.awaiting_logged and action != "awaiting_holder_check"
            new_wait = action == "awaiting_holder_check" and m not in self.awaiting_logged
            if (action != "entered" and not resolving and not new_wait and last
                    and now - last < cfg["paper"]["rescore_minutes"] * 60):
                continue
            if action == "awaiting_holder_check":
                self.awaiting_logged.add(m)
            elif resolving:
                self.awaiting_logged.discard(m)
                waited = now - self.awaiting.get(m, now)
                print(f"  RESOLVED {c['symbol'][:12]} holder check wait {waited:.0f}s -> {action}")
            sig = {"ts": now, "mint": m, "symbol": c["symbol"], "score": sc, "min_score": min_score, "label": label,
                   "breakdown": bd, "regime_verdict": verdict, "action": action, "mcap_usd": c["mcap_usd"],
                   "curve_pct": c["curve_pct"], "age_min": (now - c["created_ts"]) / 60, "reply_count": c["reply_count"],
                   "features": self.signal_features(c, now, dup, bundle, hits), "filter_tags": ",".join(tags)}
            sid = self.store.add_signal(sig)
            if getattr(self, "holder_rec", None):
                self.holder_rec.submit(m, c["symbol"], sid, now, action)
            scored.append((sc, c["symbol"], action))
            top = ", ".join(f"{r} {p:+g}" for r, p in sorted(bd[1:], key=lambda x: -abs(x[1]))[:3] if p)
            print(f"  SIGNAL {c['symbol'][:12]:<12} score {sc:5.1f} (min {min_score}) {label:<14} -> {action:<21} "
                  f"${c['mcap_usd']:,.0f} curve {c['curve_pct']:.0f}% age {sig['age_min']:.1f}m | {top}"
                  + (f" | would-hit: {','.join(tags)}" if tags else ""))
            if action == "entered":
                self.execute_paper_entry(c, sid, sc, bd, label, verdict, now, tags)
                open_n += 1
                if self.rb.available and self.worker:
                    self.worker.record(c, "entered")
        self.store.commit()
        return skips, scored

    def evaluate_first_seen(self, c, now, cfg, verdict, prior, dup, open_n, day_pnl, scored):
        m, e, sk = c["mint"], cfg["entry"], cfg.get("skips") or {}
        first_usd = c.get("band_mcap_usd") or c["mcap_usd"]
        first_ts = c.get("band_ts") or now
        n_prior = self.store.creator_prior(c.get("creator"), c.get("created_ts"))[0]
        ftags = score.filter_tags(c, now, cfg, n_prior, dup_ticker=dup)
        tags = [t for t, _ in ftags]
        if c.get("quote_mint") == feedmod.USDC_MINT:
            tags.append("quote_usdc")
        bundle, label = None, "filter_only"
        floor = float(e.get("first_seen_min_mcap_usd") or 0)
        win = e.get("first_seen_entry_window_seconds")
        climbers = bool(e.get("buy_climbers"))
        entry_path = "first_seen" if first_usd >= floor else "climbed"
        extra = [[f"first seen in range at ${first_usd:,.0f} ({'>=' if first_usd >= floor else '<'} ${floor:,.0f}), "
                  f"{now - first_ts:.0f}s ago", 0], ["no minimum score (score logged only)", 0]]
        if climbers:
            extra.append([f"entry path: {entry_path} (buy floor ${floor:,.0f} on the current mcap ${c['mcap_usd']:,.0f})", 0])
        rej = self.__dict__.setdefault("holder_reject", {})
        rj = rej.get(m)
        if "mayhem_state" not in c or c.get("mayhem_state"):
            action = "mayhem_skip"
        elif self.store.ever_traded(m):
            action = "already_traded"
        elif verdict == "stand_down":
            action = "regime_stand_down"
        elif climbers and c["mcap_usd"] < floor:
            action = "below_buy_floor"
        elif not climbers and first_usd < floor:
            action = "first_seen_below_min"
        elif not climbers and win and now - first_ts > float(win):
            action = "first_seen_window_passed"
        elif sk.get("serial_creator") and n_prior >= int(sk["serial_creator_at"]):
            action = "serial_creator"
        elif open_n >= cfg["paper"]["max_open"]:
            action = "max_open"
        elif cfg["paper"].get("daily_loss_cap_sol") is not None and day_pnl <= -cfg["paper"]["daily_loss_cap_sol"]:
            action = "daily_loss_cap"
        elif rj and now - rj[0] < cfg["paper"]["rescore_minutes"] * 60:
            action = rj[1]
        else:
            phc = cfg["data_sources"].get("pump_holders", {})
            if phc.get("enabled"):
                hit = self.pump_hc_cache.get(m)
                if hit and now - hit[0] < phc.get("cache_seconds", 60):
                    bundle = hit[1]
                else:
                    bundle = self.pump_holder_check(c)
                    self.pump_hc_cache[m] = (now, bundle)
                    self.holder_lookups = getattr(self, "holder_lookups", 0) + 1
            action = "entered"
            if bundle is None:
                tags.append("holder_data_missing")
                if sk.get("holder_data_missing"):



                    action = "holder_data_missing"
                    extra.append(["holder data missing (pump.fun lookup failed/empty) -> skip (skips.holder_data_missing)", 0])
                else:
                    extra.append(["holder data missing (pump.fun lookup failed/empty) -> holder filters not applied, still buy", 0])
            else:
                label = "holder_pumpfun"
                t10 = bundle.get("top10_holder_share")
                htag = score.holder_count_tag(bundle, cfg)
                if htag:
                    tags.append(htag)
                mt = sk.get("min_top10_share")
                low10 = mt is not None and t10 is not None and float(t10) < float(mt)
                if low10:
                    tags.append("top10_below_min")



                bsmin = sk.get("min_bundsnip_pct")
                bsh, ssh = bundle.get("bundler_share"), bundle.get("sniper_share")
                lowbs = False
                if bsmin is not None and bsh is not None and ssh is not None:
                    bs_pct = float(format(float(bsh) + float(ssh), ".0%")[:-1])
                    lowbs = bs_pct < float(bsmin)
                    if lowbs:
                        tags.append("bundsnip_below_min")
                bundled = bool(bundle.get("bundled"))
                if bundle.get("bundle_limits"):
                    extra.append([f"bundle limits: {bundle['bundle_limits']}", 0])
                if bundled:
                    tags.append("bundled")
                    tags.extend(f"bl_{k}" for k in (bundle.get("bundle_tripped") or []))


                    extra.append([f"pump.fun holders bundled, tag only (top10 {float(t10 or 0):.0%}, "
                                  f"bund+snip {float(bundle.get('bundler_share') or 0) + float(bundle.get('sniper_share') or 0):.0%}, "
                                  f"dev {float(bundle.get('dev_share') or 0):.0%}, n={bundle.get('total_holders')})", 0])
                if bundled and sk.get("bundled_skip", True) and cfg["score"].get("bundle_hard_skip", True):
                    action = "bundled_skip"
                elif htag == "too_few_holders" and sk.get("too_few_holders"):
                    action = "too_few_holders"
                elif htag == "holder_count_missing" and sk.get("holder_count_missing"):
                    action = "holder_count_missing"
                elif low10 and sk.get("top10_below_min", True):
                    action = "top10_below_min"
                elif lowbs:
                    action = "bundsnip_below_min"
                if htag == "holder_count_missing" or t10 is None:
                    tags.append("holder_data_missing")
                if action != "entered":
                    rej[m] = (now, action)
        sc, bd = score.score_coin(c, now, cfg, prior=prior, bundle=bundle)
        bd = bd + extra
        if tags:
            bd.append([f"tags (logged): {', '.join(dict.fromkeys(tags))}", 0])
        last = self.store.last_signal_ts(m)
        if action == "already_traded" and last:
            return False
        if action != "entered" and last and now - last < cfg["paper"]["rescore_minutes"] * 60:
            return False
        tags = list(dict.fromkeys(tags))

        v2, v2r = self.score_v2_label(c, now, bundle, n_prior, entry_path, dup, tags)
        sig = {"ts": now, "mint": m, "symbol": c["symbol"], "score": sc, "min_score": None, "label": label,
               "breakdown": bd, "regime_verdict": verdict, "action": action, "mcap_usd": c["mcap_usd"],
               "curve_pct": c["curve_pct"], "age_min": (now - c["created_ts"]) / 60, "reply_count": c["reply_count"],
               "features": self.signal_features(c, now, dup, bundle, None), "filter_tags": ",".join(tags),
               "score_v2": v2, "score_v2_reasons": v2r}
        sid = self.store.add_signal(sig)
        if getattr(self, "holder_rec", None):
            self.holder_rec.submit(m, c["symbol"], sid, now, action, limits=(bundle or {}).get("bundle_limits"))
        if action == "serial_creator" and sk.get("shadow_serial_min", 0) <= (n_prior or 0) <= sk.get("shadow_serial_max", 0):
            self.shadow_serial_add(c, sid, now, n_prior, bundle)
        scored.append((sc, c["symbol"], action))
        if action not in ("first_seen_below_min", "below_buy_floor"):
            print(f"  SIGNAL {c['symbol'][:12]:<12} score {sc:5.1f} (no min) {label:<19} -> {action:<24} "
                  f"${c['mcap_usd']:,.0f} curve {c['curve_pct']:.0f}% first ${first_usd:,.0f} {now - first_ts:.0f}s ago"
                  + (f" [{entry_path}]" if climbers else "")
                  + (f" | {(bundle or {}).get('reason', '')}" if bundle else "") + (f" | tags: {','.join(tags)}" if tags else ""))
        if action == "entered":
            self.execute_paper_entry(c, sid, sc, bd, label, verdict, now, tags,
                                     extra={"entry_path": entry_path, "first_seen_mcap_usd": first_usd,
                                            "score_v2": v2, "score_v2_reasons": v2r})
            return True
        return False

    @staticmethod
    def score_v2_label(c, now, bundle, n_prior, entry_path, dup, tags):
        try:
            b = bundle or {}
            try:
                n = int(b.get("total_holders")) if b.get("holder_src") == "pumpfun" else None
            except (TypeError, ValueError):
                n = None
            have = b.get("holder_src") == "pumpfun" and b.get("top10_holder_share") is not None
            f = {"holders": n, "top10": b.get("top10_holder_share") if have else None,
                 "bs": (float(b.get("bundler_share") or 0) + float(b.get("sniper_share") or 0)) if have else None,
                 "dev": b.get("dev_share") if have else None, "n_prior": n_prior,
                 "age_min": (now - c["created_ts"]) / 60 if c.get("created_ts") else None, "entry_path": entry_path,
                 "entry_mcap_usd": c.get("mcap_usd"), "dup_ticker": bool(dup), "vertical": "vertical" in (tags or [])}
            v2, why = score.score_v2(f)
            return v2, (why if v2 is not None else None)
        except Exception as ex:
            print(f"  score_v2 label error: {ex}")
            return None, None

    def pumpswap_mcap_sol(self, mint):
        try:
            import base64
            import poolprice as pp
            cq = self.store.get_coin(mint)
            usdc = bool(cq) and cq["quote_mint"] == feedmod.USDC_MINT
            want = feedmod.USDC_MINT if usdc else pp.WSOL_STR
            r = self.rpc.call("getAccountInfo", [pp.canonical_pool(mint, want), {"encoding": "base64"}])
            v = (r or {}).get("value")
            if not v or v.get("owner") != pp.PUMP_AMM_STR:
                return None
            pool = pp.decode_pool(base64.b64decode(v["data"][0]))
            if pool["base_mint"] != mint or pool["quote_mint"] != want:
                return None
            r2 = self.rpc.call("getMultipleAccounts", [[pool["pool_base_token_account"], pool["pool_quote_token_account"]],
                                                       {"encoding": "base64"}])
            vals = (r2 or {}).get("value") or []
            if len(vals) != 2 or not all(vals):
                return None
            base = pp.token_amount(base64.b64decode(vals[0]["data"][0]))
            quote = pp.token_amount(base64.b64decode(vals[1]["data"][0])) + int(pool.get("virtual_quote_reserves") or 0)
            if base <= 0 or quote <= 0:
                return None
            if usdc:
                sp = self.usd_now()
                return ((quote / 1e6) / base * 1_000_000_000_000_000) / sp if sp else None
            return (quote / 1e9) / base * 1_000_000_000_000_000
        except Exception as e:
            print(f"  pumpswap price error {type(e).__name__}: {e}")
            return None

    def execute_paper_entry(self, c, sid, sc, bd, label, verdict, now, tags=None, extra=None):
        size = self.cfg["paper"]["size_sol"]
        pid = self.store.add_paper(dict({"mint": c["mint"], "symbol": c["symbol"], "signal_id": sid, "entry_ts": now,
                                         "entry_mcap_sol": c["mcap_sol"], "entry_mcap_usd": c["mcap_usd"], "size_sol": size,
                                         "score": sc, "score_reasons": bd, "label": label, "regime_verdict": verdict,
                                         "filter_tags": ",".join(tags or [])}, **(extra or {})))
        self.store.add_tick(pid, now, 1.0, "entry")
        self.store.commit()
        _f = lambda v, spec: "n/a" if v is None else format(v, spec)
        print(f"  ENTER  {str(c.get('symbol') or '?')[:12]:<12} PAPER {size} SOL @ ${_f(c.get('mcap_usd'), ',.0f')} mcap "
              f"({_f(c.get('mcap_sol'), '.2f')} SOL), curve {_f(c.get('curve_pct'), '.0f')}%  mint {c['mint']}")


    def run(self, minutes=None):
        if self.cfg.get("mode") == "live":
            print(learn.set_mode("live"))
            print("Refusing to start in live mode. Switch back with: python bot.py mode learning")
            return
        stop_at = time.time() + minutes * 60 if minutes else None
        print("=" * 100)
        mode, why = self.effective_mode()
        print("TRENCH BOT - PAPER TRADING ONLY (mode = %s%s, execution.enabled = %s; this build has no signer)"
              % (self.cfg.get("mode"), f" -> LOCKED, acting as learning" if why else "", self.cfg["execution"]["enabled"]))
        if why:
            print(f"auto_tune is locked: {why}")
        print(self.wi.status_line())
        print("=" * 100)
        print("startup: studying the trenches before the first poll ...")
        beat("startup", worker=self.worker)
        self.solpx.startup()
        feedmod.SOL_USD_FN = self.usd_now
        self.ingest(self.feed.newest(0), from_new=True, rate=True)

        self.report = {"verdict": "off", "windows": {}}
        self.last_regime_ts = time.time()
        self.fastfeed = fastfeed.FastFeed(self, allow_entries=True)
        print(f"fast feed: {'stream LIVE' if self.fastfeed.live() else 'stream not available (30 s poll only)'} ({self.fastfeed.path})")
        poll = 0
        while True:


            _iv = float(self.cfg["poll"]["interval_seconds"])
            _nb = (int(time.time() // _iv) + 1) * _iv if poll == 0 else None
            if _nb is not None:
                time.sleep(max(0.0, _nb - time.time()))
            t0 = (time.time() // _iv) * _iv
            poll += 1
            beat("poll start", poll, self.worker)
            self.maybe_reload_config()
            self.shadow_serial_tick()
            self.solpx.maybe_refresh()
            v = self.report["verdict"]
            w6 = self.report["windows"].get("6h", {})
            open_n = len(self.store.open_paper())
            day = self.store.closed_pnl_since(utc_day_start(t0))
            print(f"\n=== {now_local()} | poll {poll} | open {open_n}/{self.cfg['paper']['max_open']}"
                  f" | day PnL {day:+.3f} SOL ===")
            if self.report.get("stand_down_warning"):
                print("  !! 24h regime stand_down for %d refreshes: stand down, don't loosen filters"
                      % self.report["stand_down_streak_24h"])
            new = self.feed.newest(0)
            t_new = self.ingest(new, from_new=True, rate=True)
            act = self.feed.active()
            t_act = self.ingest(act, from_new=False)
            if new is None or act is None:
                print(f"  feed: newest={'ok' if new is not None else 'FAILED'} active={'ok' if act is not None else 'FAILED'}"
                      " (rate limited or blocked; using RPC prices for watched coins)")
            beat("feed done", poll, self.worker)
            n_rpc = self.rpc_refresh(t_act)
            beat("prices done", poll, self.worker)
            self.log_post_close_ticks()
            self.post_exit_tick()
            self.fill_labels()
            if time.time() - getattr(self, "last_grad_ts", 0) >= self.cfg["regime"]["refresh_minutes"] * 60:
                self.last_grad_ts = time.time()
                self._start_gradtrack()
            if False:
                self.refresh_regime(deep=True)
                v = self.report["verdict"]
            self.manage_positions(v)
            watched = [r["mint"] for r in self.store.q(
                "SELECT mint FROM coins WHERE last_seen_ts>=? AND band_ts IS NOT NULL AND complete=0", (t0,))]
            now_ = time.time()
            for k in [k for k, t in self.awaiting.items() if now_ - t > 1800]:
                self.awaiting.pop(k, None)
            skips, scored = self.evaluate(t_act + t_new + watched + list(self.awaiting_logged), v)
            rate = f"{self.launch_rate:.0f}/min" if self.launch_rate else "n/a"
            print(f"  new {len(new) if new else 0} (launch rate {rate}) | active {len(act) if act else 0} | "
                  f"RPC-priced {n_rpc} | scored {len(scored)} | skipped: "
                  + (", ".join(f"{k}={n}" for k, n in sorted(skips.items(), key=lambda x: -x[1])[:6]) or "none"))
            if getattr(self, "fastfeed", None) is not None:
                fs = self.fastfeed.status()
                print(f"  fast feed: {'LIVE' if fs['live'] else 'off'} | new {fs['new']} evals {fs['evals']} open-ticks {fs['open_ticks']}"
                      f" | median notice: new coin {fs['median_new_s']}s, price move {fs['median_move_s']}s")
            if stop_at and time.time() >= stop_at:
                print("\nrun time reached; stopping. Open paper positions stay open and resume next run.")
                if self.worker:
                    self.worker.stop()
                break
            beat("poll done", poll, self.worker)
            self.fast_wait(t0, v)



def _print_lead(m, indent="  "):
    print(f"{indent}expectancy         {m['expectancy_sol']:+.4f} SOL/trade   ({m['expectancy_r']:+.2f}R, 1R = planned loss)")
    print(f"{indent}profit factor      {metrics.fmt_pf(m['profit_factor'])}")
    print(f"{indent}PF ex-best trade   {metrics.fmt_pf(m['profit_factor_ex_best'])}   (best trade {m['best_trade_sol']:+.4f} SOL)"
          + ("  <- flips under 1 without the best trade: no system yet" if (m['profit_factor_ex_best'] or 0) < 1 <= (m['profit_factor'] or 0) else ""))
    print(f"{indent}payoff ratio       {('%.2f' % m['payoff_ratio']) if m['payoff_ratio'] else 'n/a'}   (avg win / avg loss)")
    print(f"{indent}max drawdown       {m['max_drawdown_sol']:.4f} SOL   (plan on live being 1.5-2x this)")
    print(f"{indent}time underwater    {m['time_underwater_h']:.1f} h total, longest {m['longest_underwater_h']:.1f} h"
          + ("  (still underwater)" if m["still_underwater"] else ""))
    print(f"{indent}longest losing run {m['longest_losing_streak']} trades")
    print(f"{indent}exit-reason mix    " + ", ".join(f"{k}={v}" for k, v in sorted(m["exit_mix"].items(), key=lambda x: -x[1])))


def _print_exit65():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "shadow.db")
    sp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "shadow", "streams.json")
    try:
        sc = json.load(open(sp))["streams"].get("exit65")
        if not sc or not os.path.exists(p):
            return
        con = sqlite3.connect(p)
        rows = con.execute("SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult, label FROM paper "
                           "WHERE stream='exit65' AND entry_ts>=?", (sc.get("changed_at_ts") or 0,)).fetchall()
    except (OSError, ValueError, sqlite3.Error):
        return
    mk = lambda pr, sz, rf, lm: (pr or 0) + sz * (rf or 0) * (lm or 1) - (sz + 0.0005)
    cl = [r for r in rows if r[0] == "closed"]
    op = [r for r in rows if r[0] != "closed"]
    net, mark = sum(r[1] or 0 for r in cl), sum(mk(*r[2:6]) for r in op)
    print(f"  SIDE BOT (NOT main PnL) {sc.get('label')}{'' if sc.get('enabled', True) else ' [DISABLED ' + str(sc.get('disabled_at_local', '')) + ']'} since "
          f"{sc.get('changed_at_local')}: {len(cl)} closed, {sum(1 for r in cl if (r[1] or 0) > 0)}W/"
          f"{sum(1 for r in cl if (r[1] or 0) <= 0)}L, net {net:+.4f} SOL, open {len(op)} marked {mark:+.4f} | total {net + mark:+.4f}")
    ids = [int(r[6].split(":")[1]) for r in rows if (r[6] or "").startswith("mirror_live:")]
    if ids:
        m = sqlite3.connect(st.DB_PATH).execute(
            f"SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult FROM paper WHERE id IN "
            f"({','.join('?' * len(ids))})", ids).fetchall()
        mnet = sum(r[1] or 0 for r in m if r[0] == "closed")
        mmark = sum(mk(*r[2:6]) for r in m if r[0] != "closed")
        print(f"    main on the same {len(ids)} trades: closed net {mnet:+.4f} SOL, open marked {mmark:+.4f} | total {mnet + mmark:+.4f}")


def _print_bot_c():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "shadow.db")
    sp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "shadow", "streams.json")
    try:
        sc = json.load(open(sp))["streams"].get("bot_c")
        if not sc or not os.path.exists(p) or not sc.get("changed_at_ts"):
            return
        con = sqlite3.connect(p)
        since = sc["changed_at_ts"]
        rows = con.execute("SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult FROM paper "
                           "WHERE stream='bot_c' AND entry_ts>=?", (since,)).fetchall()
        dec = con.execute("SELECT live_paper_id, decision, reasons FROM mirror_filter WHERE stream='bot_c' AND ts>=?",
                          (since,)).fetchall()
    except (OSError, ValueError, sqlite3.Error):
        return
    mk = lambda pr, sz, rf, lm: (pr or 0) + sz * (rf or 0) * (lm or 1) - (sz + 0.0005)
    def summ(rs):
        cl = [r for r in rs if r[0] == "closed"]
        net, mark = sum(r[1] or 0 for r in cl), sum(mk(*r[2:6]) for r in rs if r[0] != "closed")
        return (f"{len(cl)} closed, {sum(1 for r in cl if (r[1] or 0) > 0)}W/{sum(1 for r in cl if (r[1] or 0) <= 0)}L, "
                f"net {net:+.4f} SOL, open {len(rs) - len(cl)} marked {mark:+.4f} | total {net + mark:+.4f}")
    def main_rows(ids):
        if not ids:
            return []
        return sqlite3.connect(st.DB_PATH).execute(
            f"SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult FROM paper WHERE id IN "
            f"({','.join('?' * len(ids))})", ids).fetchall()
    print(f"  SIDE BOT (NOT main PnL) {sc.get('label')} since {sc.get('changed_at_local')}: {summ(rows)}")
    taken = [i for i, d, _ in dec if d == "take"]
    skipped = [i for i, d, _ in dec if d == "skip"]
    by = {}
    for _, d, rs in dec:
        if d == "skip":
            for x in (rs or "").split(","):
                by[x] = by.get(x, 0) + 1
    print(f"    main on the same {len(taken)} trades: {summ(main_rows(taken))}")
    print(f"    main trades side bot C skipped: {len(skipped)} ({', '.join(f'{k} {v}' for k, v in by.items()) or '-'}) -> on main: "
          f"{summ(main_rows(skipped))}")


def _print_bot_m():
    try:
        import dashboard
        d = dashboard._bot_m_summary()
    except Exception as e:
        print(f"  side bot M: unavailable ({type(e).__name__})")
        return
    if d.get("error") or d.get("closed_n") is None:
        return
    mb = d.get("moonbags") or {}
    ms = d.get("main_same") or {}
    print(f"  SIDE BOT (NOT main PnL) {d.get('label')} since {d.get('changed_at_local')}: {d['closed_n']} closed, "
          f"{d['wins']}W/{d['losses']}L, open {d['open_n']} | PnL marked at current value {d.get('pnl_marked_sol', 0):+.4f} SOL"
          f" | if bags sold at peak {d.get('pnl_if_sold_at_peak_sol', 0):+.4f} SOL")
    print(f"    main on the same trades: {ms.get('closed_n', 0)} closed, net {ms.get('closed_net_sol', 0):+.4f} SOL, "
          f"open {ms.get('open_n', 0)} marked {ms.get('open_unrealized_sol', 0):+.4f}")
    print(f"    moonbags: {mb.get('holding_n', 0)} holding ({mb.get('n', 0)} ever), value now {mb.get('value_now_sol', 0):.4f} SOL, "
          f"peak value {mb.get('peak_value_sol', 0):.4f} SOL")
    for b in mb.get("bags") or []:
        print(f"      {b['symbol'][:12]:<12} {b['status']:<12} {b['mult_now']:.2f}x now / peak {b['peak_mult']:.2f}x  "
              f"value {b['value_now_sol']:.4f} / peak {b['peak_value_sol']:.4f} SOL  entered {b['entry_local']} ({b['age_h']:.1f} h)")


def _print_no_vertical():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "shadow.db")
    sp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "shadow", "streams.json")
    try:
        allsc = json.load(open(sp))["streams"]
        sc, dc = allsc.get("bot_b"), allsc.get("bot_d")
        if not sc or not sc.get("enabled") or not os.path.exists(p):
            return
        con = sqlite3.connect(p)
        q = "SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult, label, id FROM paper WHERE stream=? AND entry_ts>=?"
        rows = con.execute(q, ("bot_b", sc.get("changed_at_ts") or 0)).fetchall()
        drows = con.execute(q, ("bot_d", dc.get("changed_at_ts") or 0)).fetchall() if dc and dc.get("enabled") else []
    except (OSError, ValueError, sqlite3.Error):
        return
    mk = lambda pr, sz, rf, lm: (pr or 0) + sz * (rf or 0) * (lm or 1) - (sz + 0.0005)
    def summ(rs):
        cl = [r for r in rs if r[0] == "closed"]
        net, mark = sum(r[1] or 0 for r in cl), sum(mk(*r[2:6]) for r in rs if r[0] != "closed")
        return (f"{len(rs)} trades, {len(cl)} closed {sum(1 for r in cl if (r[1] or 0) > 0)}W/{sum(1 for r in cl if (r[1] or 0) <= 0)}L, "
                f"net {net:+.4f} SOL, open {len(rs) - len(cl)} marked {mark:+.4f} | total {net + mark:+.4f}")
    ex = [r for r in rows if (r[6] or "").startswith("nv_extra:")]
    print(f"  COPY BOT (NOT main PnL) {sc.get('label')} since {sc.get('changed_at_local')}: {summ(rows)}")
    print(f"    EXTRA trades (main did not take): {summ(ex)}")
    print(f"    shared with main (main's buys, copied): {summ([r for r in rows if r not in ex])}")
    if dc and dc.get("enabled"):
        ids = {int(r[6].split(":")[1]) for r in drows if (r[6] or "").startswith("mirror_bot_b:")}
        print(f"  COPY BOT (NOT main PnL) {dc.get('label')} since {dc.get('changed_at_local')}: {summ(drows)}")
        print(f"    side bot B on the same {len(ids)} trades: {summ([r for r in rows if r[7] in ids])}")
        try:
            sk = dict(con.execute("SELECT live_paper_id, reasons FROM mirror_filter WHERE stream='bot_d' AND decision='skip' "
                                  "AND ts>=?", (dc.get("changed_at_ts") or 0,)).fetchall())
        except sqlite3.Error:
            sk = {}
        by = {}
        for rs in sk.values():
            for x in (rs or "").split(","):
                by[x] = by.get(x, 0) + 1
        print(f"    side bot B trades side bot D skipped (filter): {len(sk)} ({', '.join(f'{k} {v}' for k, v in by.items()) or '-'}) -> on side bot B: "
              f"{summ([r for r in con.execute(q.replace('stream=? AND entry_ts>=?', 'stream=? AND entry_ts>=? AND id IN (' + ','.join('?' * len(sk)) + ')'), ('bot_b', 0, *sk)).fetchall()]) if sk else '-'}")


def _print_spike_skipped(since_ts):

    for stream, label in (("vertical_skipped", "vertical-skipped (would-have-bought)"),):
        _print_skip_counterfactual(since_ts, stream, label)


def _print_skip_counterfactual(since_ts, stream, label):
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "shadow.db")
    if not os.path.exists(p):
        return
    try:
        con = sqlite3.connect(p)
        cl = con.execute("SELECT pnl_sol FROM paper WHERE stream=? AND status='closed' AND entry_ts>=?",
                         (stream, since_ts)).fetchall()
        op = con.execute("SELECT proceeds_sol, size_sol, remaining_frac, last_mult FROM paper WHERE stream=? "
                         "AND status!='closed' AND entry_ts>=?", (stream, since_ts)).fetchall()
        seen = con.execute("SELECT outcome, COUNT(*) FROM spike_mirror WHERE stream=? AND ts>=? GROUP BY outcome",
                           (stream, since_ts)).fetchall()
    except sqlite3.Error:
        return
    net = sum(r[0] or 0 for r in cl)
    w = sum(1 for r in cl if (r[0] or 0) > 0)
    mark = sum((a or 0) + s_ * (rf or 0) * (m or 1) - (s_ + 0.0005) for a, s_, rf, m in op)
    print(f"  [DISABLED] COUNTERFACTUAL (NOT main PnL) {label}: {len(cl)} closed, {w}W/{len(cl) - w}L, "
          f"net {net:+.4f} SOL, open {len(op)} marked {mark:+.4f} SOL")
    if seen:
        print(f"    {stream.split('_')[0]} skips (all-time): " + ", ".join(f"{o} {n}" for o, n in sorted(seen, key=lambda x: -x[1]))
              + "  (details: python3 shadow.py report)")


def cmd_report():
    cfg = st.load_config()
    s = st.Store()
    snaps = s.last_regimes(cfg["regime"]["stand_down_streak_warn"])
    print("=" * 80)
    print("TRENCH BOT REPORT (paper trading only; all PnL after modelled fees, priority fees, slippage, failed sells)")
    print("=" * 80)
    b = st.load_baseline()
    if b:
        bs = st.baseline_summary(sqlite3.connect(st.DB_PATH), b["ts"], cfg["paper"].get("priority_fee_sol", 0.0005))
        wr = f"{bs['wins'] / bs['closed_n']:.0%}" if bs["closed_n"] else "n/a"
        print(f"\nSINCE BASELINE ({b['local']}: {b.get('note', '')})")
        print(f"  closed {bs['closed_n']} (W {bs['wins']} / L {bs['losses']}, win {wr})  net {bs['closed_net_sol']:+.4f} SOL"
              f" | open {bs['open_n']} marked {bs['open_unrealized_sol']:+.4f} SOL"
              f" | total {bs['closed_net_sol'] + bs['open_unrealized_sol']:+.4f} SOL")
        if cfg["score"].get("gate_entries", True) is False:
            acts = s.q("SELECT action, COUNT(*) n FROM signals WHERE ts>=? GROUP BY action ORDER BY n DESC", (b["ts"],))
            mins = max((time.time() - b["ts"]) / 60, 1e-9)
            print(f"  new-main decisions since baseline ({mins:.0f} min; one row per coin per decision, re-logged at most every "
                  f"{cfg['paper'].get('rescore_minutes', 3)} min): " + (", ".join(f"{r['action']} {r['n']}" for r in acts) or "none yet"))
        print("  by tag on trades entered (a trade with 2 tags counts in both):")
        for d in st.tag_breakdown(sqlite3.connect(st.DB_PATH), b["ts"], cfg["paper"].get("priority_fee_sol", 0.0005)):
            print(f"    {d['tag']:<21} {d['n']:>3} trades | closed {d['closed_n']} (W {d['wins']} / L {d['losses']}) "
                  f"net {d['closed_net_sol']:+.4f} SOL | open {d['open_n']} marked {d['open_unrealized_sol']:+.4f}")


        _print_bot_c()
        _print_bot_m()
        _print_no_vertical()
        print("  (all-time numbers follow)")
    closed = [dict(r) for r in s.q("SELECT * FROM paper WHERE status='closed' ORDER BY close_ts")]
    R = learn.one_r(cfg)
    m = metrics.book_metrics(closed, R, now=time.time())
    print(f"\nClosed paper trades: {m['n']}" + (f"  - {metrics.sample_note(m['n'])}" if m["n"] else ""))
    if m["n"]:
        _print_lead(m)
        regimes = sorted({t["regime_verdict_entry"] or "unknown" for t in closed})
        print("\n  split by regime at entry:")
        for rg in regimes:
            sub = [t for t in closed if (t["regime_verdict_entry"] or "unknown") == rg]
            mm = metrics.book_metrics(sub, R)
            print(f"   [{rg}] {mm['n']} trades: expectancy {mm['expectancy_sol']:+.4f} SOL ({mm['expectancy_r']:+.2f}R), "
                  f"PF {metrics.fmt_pf(mm['profit_factor'])}, PF ex-best {metrics.fmt_pf(mm['profit_factor_ex_best'])}, "
                  f"max DD {mm['max_drawdown_sol']:.4f}, losing run {mm['longest_losing_streak']}")
        print(f"\n  win rate           {m['win_rate']:.0%}  ({sum(1 for t in closed if t['pnl_sol'] > 0)}W / "
              f"{sum(1 for t in closed if t['pnl_sol'] <= 0)}L)")
        print(f"  mean / median      {m['mean_sol']:+.4f} / {m['median_sol']:+.4f} SOL per trade   total {m['total_sol']:+.4f} SOL")
        print(f"  recovery factor    {('%.2f' % m['recovery_factor']) if m['recovery_factor'] is not None else 'n/a'}"
              f"   (net / max drawdown; under 1 = worst hole bigger than everything made)")
        cap = cfg["paper"].get("daily_loss_cap_sol")
        worst = min(m["daily_pnl"].values()) if m["daily_pnl"] else 0.0
        if cap is None:
            print(f"  daily loss cap     OFF (paper); worst UTC day {worst:+.4f} SOL — re-enable before live")
        else:
            hits = sum(1 for v in m["daily_pnl"].values() if v <= -cap)
            print(f"  daily loss cap     worst UTC day {worst:+.4f} SOL vs cap -{cap}; cap hit on {hits} of {len(m['daily_pnl'])} days")
        print(f"  daily Sharpe/Sortino " + (f"{m['sharpe_daily']:.2f} / {m['sortino_daily']:.2f}" if m["sharpe_daily"]
                                           and m["sortino_daily"] else "n/a (needs 5+ trading days; never the objective)"))
        fees = sum(t["fees_sol"] or 0 for t in closed)
        slip = sum(t["slippage_sol"] or 0 for t in closed)
        failed = sum(t["failed_sells"] or 0 for t in closed)
        tp = [t for t in closed if t["tp1_done"]]
        missed = sum(1 for t in tp if t.get("scale1_sold") == 0)
        print(f"  costs              fees+priority {fees:.4f} SOL, slippage vs mark {slip:.4f} SOL, "
              f"failed sells {failed} (modelled), 2x hit {len(tp)} / scale1 sold {len(tp) - missed}")
        t2 = [t["time_to_2x_min"] for t in closed if t.get("time_to_2x_min")]
        ts_ = [t["time_to_stop_min"] for t in closed if t.get("time_to_stop_min")]
        print(f"  timing             median time to 2x {('%.1f min' % statistics.median(t2)) if t2 else 'n/a'}, "
              f"median time to stop/exit {('%.1f min' % statistics.median(ts_)) if ts_ else 'n/a'}")
        l1 = [t["mult_1h"] for t in closed if t.get("mult_1h")]
        l6 = [t["mult_6h"] for t in closed if t.get("mult_6h")]
        gr = sum(1 for t in closed if t.get("later_graduated"))
        print(f"  labels (not targets) mcap 1h after entry: median {('%.2fx' % statistics.median(l1)) if l1 else 'n/a'} "
              f"(n={len(l1)}); 6h: {('%.2fx' % statistics.median(l6)) if l6 else 'n/a'} (n={len(l6)}); later graduated {gr}")

    print("\nOpen paper positions:")
    opens = s.open_paper()
    if not opens:
        print("  none")
    for p in opens:
        print(f"  {p['symbol'][:14]:<14} entry ${p['entry_mcap_usd']:,.0f}  now {p['last_mult']:.2f}x  peak {p['peak_mult']:.2f}x "
              f"left {p['remaining_frac']:.0%}  {(time.time() - p['entry_ts']) / 60:.0f} min  score {p['score']}")
    ex = s.q("SELECT reason, COUNT(*) n FROM exits GROUP BY reason ORDER BY n DESC")
    if ex:
        print("\nEvery partial/final exit: " + ", ".join(f"{r['reason']}={r['n']}" for r in ex))

    print()
    if snaps:
        rep = json.loads(snaps[0]["report"])
        print(f"Regime: {rep['verdict'].upper()}  (as of {rep['generated_at_utc']} UTC)")
        for name, w in rep["windows"].items():
            print(f"  {name}: {w['verdict']}{' (low confidence)' if w['low_confidence'] else ''} - "
                  + "; ".join(w["verdict_reasons"]))
            gr = w.get("graduation_rate")
            print(f"      launches seen {w['mints_seen']}, graduation rate "
                  f"{(f'{gr:.2%}' if gr is not None else 'n/a')} [{w.get('graduation_rate_source', '')}], "
                  f"band coins resolved {w['band_coins_resolved']} / did 2x {w['band_2x']}, in band now {w['in_entry_band_now']}")
        streak = rep.get("stand_down_streak_24h", 0)
        if streak >= cfg["regime"]["stand_down_streak_warn"]:
            print(f"  !! 24h regime has been STAND_DOWN for {streak} refreshes in a row.")
            print("     Recommendation: stand down. Do not loosen filters to force trades.")
    else:
        print("Regime: no snapshot yet (run `python bot.py` first)")

    sig = s.q("SELECT action, COUNT(*) n FROM signals GROUP BY action ORDER BY n DESC")
    tot = sum(r["n"] for r in sig)
    print(f"\nSignals logged: {tot}" + (" (" + ", ".join(f"{r['action']}={r['n']}" for r in sig) + ")" if sig else ""))
    lab = s.q("SELECT label, COUNT(*) n FROM signals GROUP BY label")
    if lab:
        print("  labels: " + ", ".join(f"{r['label']}={r['n']}" for r in lab))
    fe = s.one("""SELECT COUNT(*) n, COUNT(same_block_supply_share) sb, COUNT(top10_holder_share) th,
                  COUNT(creator_prior_grad_rate) cg FROM signals WHERE features IS NOT NULL""")
    if fe and fe["n"]:
        print(f"  feature coverage ({fe['n']} signals with features): same-block share {fe['sb']}, top-10 holder share {fe['th']}, "
              f"creator prior graduation rate {fe['cg']} (blank = data missing, never guessed)")
    tr = s.q("SELECT run_id, COUNT(*) n FROM tune_trials WHERE kind='grid' GROUP BY run_id ORDER BY MAX(ts) DESC LIMIT 1")
    tot_tr = s.one("SELECT COUNT(*) n FROM tune_trials")["n"]
    if tr:
        print(f"  tune_trials: {tot_tr} rows logged (last sweep {tr[0]['run_id']}: {tr[0]['n']} configs)")
    fl = s.q("SELECT source, status, COUNT(*) n FROM feed_log GROUP BY source, status ORDER BY source, n DESC")
    if fl:
        print("\nData requests (all time): " + "; ".join(f"{r['source']} {r['status']}={r['n']}" for r in fl))
    print()



def suggest(cfg, closed, exits_rows, streak_warn):
    t = cfg["tune"]
    notes, diffs = [], []
    n = len(closed)
    if streak_warn:
        notes.append("24h regime has been stand_down repeatedly: recommendation is to STAND DOWN. "
                     "No loosening suggestions will be made.")
    if n < t["min_trades"]:
        notes.append(f"Only {n} closed paper trades (need {t['min_trades']}): not enough data to suggest changes.")
        return notes, diffs
    by_final = {}
    for c in closed:
        by_final.setdefault(c["exit_reason_final"], []).append(c)
    first_exit = {}
    for e in exits_rows:
        first_exit.setdefault(e["paper_id"], e)
    stops = by_final.get("hard_stop", [])
    fast = [c for c in stops if first_exit.get(c["id"]) and first_exit[c["id"]]["minutes_in"] <= t["fast_stop_minutes"]]
    ids = lambda lst: ", ".join(f"#{c['id']}" for c in lst[:12])
    if stops and len(stops) / n >= t["dominant_share"] * 0.6:
        msg = (f"{len(stops)} of {n} trades hit the -{cfg['exits']['hard_stop_pct']}% hard stop; {len(fast)} of those "
               f"inside {t['fast_stop_minutes']} min (trades {ids(stops)})")
        if len(fast) >= len(stops) / 2:
            old = cfg["entry"]["min_curve_pct"]
            diffs.append(("entry.min_curve_pct", old, old + t["curve_step"], msg + ": raise min_curve_pct (or cut size)"))
        else:
            old = cfg["score"]["min_score"]
            diffs.append(("score.min_score", old, old + t["score_step"], msg + ": raise min_score"))
    stalls = by_final.get("stall", [])
    if stalls and len(stalls) / n >= t["dominant_share"]:
        old = cfg["score"]["min_score"]
        if not any(d[0] == "score.min_score" for d in diffs):
            diffs.append(("score.min_score", old, old + t["score_step"],
                          f"{len(stalls)} of {n} trades stalled under +{cfg['exits']['stall_min_gain_pct']}% at "
                          f"{cfg['exits']['stall_minutes']} min (trades {ids(stalls)}): entries lack follow-through, raise min_score"))
    ts_ = by_final.get("time_stop", [])
    green_ts = [c for c in ts_ if c["pnl_sol"] > 0]
    if ts_ and len(ts_) / n >= t["dominant_share"] and len(green_ts) >= len(ts_) * 0.6 and not streak_warn:
        old = cfg["exits"]["time_stop_minutes"]
        diffs.append(("exits.time_stop_minutes", old, old + 4,
                      f"{len(ts_)} of {n} trades ended on the {old}-min time stop and {len(green_ts)} were green "
                      f"(trades {ids(green_ts)}): consider a slightly longer time stop"))

    wins = [c for c in closed if c["pnl_sol"] > 0]
    losses = [c for c in closed if c["pnl_sol"] <= 0]
    if wins and losses:
        ms = cfg["score"]["min_score"]
        marginal = [c for c in losses if c["score"] < ms + t["score_step"] + cfg["score"]["selective_min_score_bump"]]
        if len(marginal) >= len(losses) * 0.6 and statistics.mean(c["score"] for c in wins) > statistics.mean(
                c["score"] for c in losses) and not any(d[0] == "score.min_score" for d in diffs):
            diffs.append(("score.min_score", ms, ms + t["score_step"],
                          f"{len(marginal)} of {len(losses)} losers scored within {t['score_step'] + cfg['score']['selective_min_score_bump']} "
                          f"of the minimum (trades {ids(marginal)}); winners averaged "
                          f"{statistics.mean(c['score'] for c in wins):.1f} vs losers {statistics.mean(c['score'] for c in losses):.1f}"))
    tp1 = [c for c in closed if c["tp1_done"]]
    notes.append(f"{len(tp1)} of {n} trades reached the 2x scale; {len(wins)} closed green.")
    return notes, diffs


def collect_suggestions(cfg, s, auto=False):
    lb = cfg["tune"]["lookback_trades"]
    closed = [dict(r) for r in s.q("SELECT * FROM paper WHERE status='closed' ORDER BY close_ts DESC LIMIT ?", (lb,))]
    ids = [c["id"] for c in closed] or [-1]
    ex = [dict(r) for r in s.q(f"SELECT * FROM exits WHERE paper_id IN ({','.join('?' * len(ids))}) ORDER BY ts", ids)]
    snaps = s.last_regimes(1)
    streak_warn = bool(snaps) and json.loads(snaps[0]["report"]).get("stand_down_warning", False)
    notes, diffs = suggest(cfg, closed, ex, streak_warn)
    ad = learn.adaptive_exits(s, cfg)
    ok, why = learn.unlock_status(s, cfg, ad["stats"])
    notes.append(f"auto_tune: {'UNLOCKED (' + why + ')' if ok else 'LOCKED - ' + why}")
    if auto and not ok:
        return [], notes
    notes += ad["notes"]
    have = {d[0] for d in diffs}
    diffs += [d for d in ad["diffs"] if d[0] not in have]
    return diffs, notes


def cmd_tune(apply=False):
    cfg = st.load_config()
    s = st.Store()
    n = s.one("SELECT COUNT(*) n FROM paper WHERE status='closed'")["n"]
    print(f"TUNE (mode: {cfg.get('mode')}): {n} closed paper trades on record; rules study the last "
          f"{min(n, cfg['tune']['lookback_trades'])} (lookback {cfg['tune']['lookback_trades']})")
    diffs, notes = collect_suggestions(cfg, s)
    for n_ in notes:
        print("  - " + n_)
    if not diffs:
        print("No config changes suggested.")
        return
    print("\nSuggested config diffs:")
    for k, old, new, why in diffs:
        print(f"  {k}: {old} -> {new}\n      because {why}")
    if not apply:
        print("\nNothing was written. Re-run with `python bot.py tune --apply` to write these to config.json "
              "(a dated copy is kept in config/history/).")
        return
    raw = st.load_raw_config()
    for k, old, new, _ in diffs:
        st.set_config_value(raw, k, new)
    snap = st.write_config(raw)
    for k, old, new, why in diffs:
        learn.log_change(f"tune --apply {k}: {old} -> {new} because {why}")
    print(f"\nApplied. New config snapshot: {os.path.relpath(snap, st.ROOT) if snap else '(unchanged)'}")


def cmd_wallets(limit=15):
    import sqlite3
    from wallets import WALLETS_DB
    if not os.path.exists(WALLETS_DB):
        print("No wallet data yet. The risk book needs HELIUS_API_KEY and some paper entries.")
        return
    db = sqlite3.connect(WALLETS_DB)
    db.row_factory = sqlite3.Row
    cfg = st.load_config()
    tiers = db.execute("SELECT tier, COUNT(*) n FROM wallet_tiers GROUP BY tier").fetchall()
    rec = db.execute("SELECT COUNT(DISTINCT mint) FROM coin_wallets").fetchone()[0]
    outs = db.execute("SELECT outcome, COUNT(*) n FROM coin_outcomes GROUP BY outcome").fetchall()
    print("WALLET RISK BOOK (the bot's own observations; no third-party wallet lists)")
    print(f"coins recorded: {rec}  outcomes: " + (", ".join(f"{r['outcome']}={r['n']}" for r in outs) or "none yet"))
    print("tiers: " + (", ".join(f"{r['tier']}={r['n']}" for r in tiers) or "none yet"))
    for tier in ("follow", "bundler", "risky", "clean"):
        rows = db.execute("""SELECT * FROM wallet_tiers WHERE tier=? ORDER BY winners DESC, bundle_flags DESC, coins DESC
                             LIMIT ?""", (tier, limit)).fetchall()
        print(f"\n[{tier}] " + ("" if rows else "none"))
        for r in rows:
            print(f"  {r['wallet']}  coins {r['coins']}  winners {r['winners']} (unrelated {r['unrelated_winners']})  "
                  f"dumped {r['rugs']}  bundle-flags {r['bundle_flags']}  first-slot {r['first_slot_count']}  "
                  f"dev-coins {r['dev_coins']}  - {r['reason']}")
    try:
        from wallets import RiskBook
        rb = RiskBook.__new__(RiskBook)
        rb.db, rb.L = db, cfg["learn"]
        t = RiskBook.skill_test(rb)
        f = lambda x: "n/a" if x is None else f"{x:.0%}"
        print(f"\nOut-of-sample skill check (coins recorded after the {cfg['learn']['skill_test_hours']}h cutoff): "
              f"{t['test_coins']} coins resolved; with a follow wallet {t['with_follow']} -> success {f(t['success_with_follow'])}, "
              f"without {f(t['success_without'])}, base {f(t['base_rate'])}, lift "
              f"{'n/a' if t['lift'] is None else '%.2fx' % t['lift']}")
    except Exception as e:
        print(f"\nskill check unavailable ({type(e).__name__})")
    unk = db.execute("SELECT COUNT(*) FROM wallet_tiers WHERE tier='unknown'").fetchone()[0]
    print(f"\n[unknown] {unk} wallets (seen too rarely to judge)")
    L = cfg["learn"]
    print(f"\nTier rules live under 'learn' in config.json. follow = {L['skill_min_tokens']}+ prior coins with "
          f">= {L['skill_min_success_rate']:.0%} success (built before the cutoff), {L['follow_min_winners']}+ unrelated winners, "
          f"sold into strength, not dev-funded, not bundle-clustered. Busy is not skilled. Follow wallets are a modest score "
          f"plus only; exits never wait on them (they leave early).")


def cmd_bundle(mint):
    cfg = st.load_config()
    s = st.Store()
    rpc = SolanaRPC(cfg, s)
    wi = WalletIntel(cfg, rpc)
    if not wi.bundle_check_available:
        print(wi.status_line())
        print("Set HELIUS_API_KEY, or set wallets.enabled_without_key to true to try the slow public-RPC version.")
        return
    c = s.get_coin(mint)
    if not c:
        print("coin not in local database yet (run the poller first so the bot knows its creator and curve)")
        return
    res = wi.bundle_check(dict(c))
    print(json.dumps(res, indent=2) if res else "bundle check failed (RPC unavailable or rate limited)")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Paper-first pump.fun trench bot (no live trading).")
    ap.add_argument("command", nargs="?", default="run", choices=["run", "report", "tune", "regime", "bundle", "wallets", "mode", "baseline"])
    ap.add_argument("arg", nargs="?")
    ap.add_argument("--apply", action="store_true", help="tune: write the suggested changes to config.json")
    ap.add_argument("--minutes", type=float, help="run: stop after this many minutes")
    a = ap.parse_args(argv)
    if a.command == "run":
        try:
            Bot().run(minutes=a.minutes)
        except KeyboardInterrupt:
            print("\nstopped by you (Ctrl+C). Everything so far is saved in data/.")
    elif a.command == "report":
        cmd_report()
    elif a.command == "baseline":
        if a.arg:
            print("baseline reset:", st.set_baseline(a.arg))
        else:
            print("current baseline:", st.load_baseline(), "\nreset with: python3 bot.py baseline \"what changed\"")
    elif a.command == "tune":
        cmd_tune(apply=a.apply)
    elif a.command == "regime":
        b = Bot()
        b.ingest(b.feed.newest(0), from_new=True)
        rep = b.refresh_regime(deep=True)
        print(regime.to_markdown(rep))
    elif a.command == "wallets":
        cmd_wallets()
    elif a.command == "mode":
        if not a.arg:
            print(f"current mode: {st.load_config().get('mode')}. Use: python bot.py mode learning|auto_tune")
        else:
            print(learn.set_mode(a.arg))
    elif a.command == "bundle":
        if not a.arg:
            sys.exit("usage: python bot.py bundle <mint>")
        cmd_bundle(a.arg)


if __name__ == "__main__":
    main()
