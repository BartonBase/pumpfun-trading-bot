import queue
import threading
import time

import store as st
from feed import SolanaRPC
from wallets import RiskBook, WalletIntel


class Preempted(Exception):
    pass


URGENT, RECORD, MARGIN, LEARN = 0, 1, 2, 3
URGENT_MAX_AGE = 90
MARGIN_MAX_AGE = 60
MAX_MARGIN_QUEUED = 3


class Phase2Worker(threading.Thread):
    def __init__(self, cfg, bundle_cache, log=print):
        super().__init__(daemon=True, name="phase2")
        self.cfg = cfg
        self.bundle_cache = bundle_cache
        self.log = log
        self.q = queue.PriorityQueue()
        self.lock = threading.Lock()
        self.pending = {}
        self.seq = 0
        self.stop_flag = threading.Event()
        self.last_profile = 0.0
        self.ready = threading.Event()
        self.current = None
        self.learn_queued = False


    def _put(self, prio, task):
        with self.lock:
            self.seq += 1
            self.q.put((prio, self.seq, time.time(), task))

    def request_bundle(self, coin, urgent=False, score=0.0):
        prio = URGENT if urgent else MARGIN
        m = coin["mint"]
        with self.lock:
            have = self.pending.get(m)
            if have is not None and have <= prio:
                return
            if prio == MARGIN and sum(1 for p in self.pending.values() if p == MARGIN) >= MAX_MARGIN_QUEUED:
                return
            self.pending[m] = prio

        self._put(prio - min(score, 100) / 1000.0 if urgent else prio, ("bundle", dict(coin)))

    def record(self, coin, reason):
        self._put(RECORD, ("record", dict(coin), reason))

    def learn(self):
        if not self.learn_queued:
            self.learn_queued = True
            self._put(LEARN, ("learn",))

    def queue_size(self):
        return self.q.qsize()

    def urgent_waiting(self):
        with self.lock:
            return any(p == URGENT for p in self.pending.values())

    def update_cfg(self, cfg):
        self.cfg = cfg
        if self.ready.is_set():
            self.wi.cfg, self.wi.w = cfg, cfg["wallets"]
            self.rb.cfg, self.rb.L = cfg, cfg["learn"]

    def stop(self):
        self.stop_flag.set()

    def _abort_check(self):
        cur = self.current
        if cur and cur[0] >= MARGIN and self.urgent_waiting():
            raise Preempted(cur[1])


    def run(self):
        self.store = st.Store(autocommit=True)
        self.rpc = SolanaRPC(self.cfg, self.store)
        self.rpc.abort_check = self._abort_check
        self.wi = WalletIntel(self.cfg, self.rpc, log=self.log)
        self.wi.db.isolation_level = None
        self.rb = RiskBook(self.wi, self.cfg)
        self.ready.set()
        while not self.stop_flag.is_set():
            try:
                prio, _, queued_ts, task = self.q.get(timeout=1.0)
            except queue.Empty:
                prio, task = None, None
            try:
                if task and task[0] == "bundle":
                    m = task[1]["mint"]
                    with self.lock:
                        best = self.pending.get(m)
                    cls = URGENT if prio < 0.5 else int(prio)
                    if best is None or best < cls:
                        continue
                    age = time.time() - queued_ts
                    if age > (URGENT_MAX_AGE if cls == URGENT else MARGIN_MAX_AGE):
                        with self.lock:
                            self.pending.pop(m, None)
                        if cls == URGENT:
                            self.log(f"  [phase2] dropped stale holder check for {task[1]['symbol'][:12]} (waited {age:.0f}s)")
                        continue
                if task:
                    self.current = (URGENT if prio < 0.5 else int(prio), task[0])
                    self._do(task)
                elif time.time() - self.last_profile >= self.cfg["poll"]["interval_seconds"]:
                    self.last_profile = time.time()
                    self.current = (LEARN, "profile")
                    self._profile_some(self.cfg["wallets"]["trade_profiles_per_poll"])
                self.store.commit()
            except Preempted as e:
                self.log(f"  [phase2] paused low-priority {e} for a qualified coin's holder check")
                if task and task[0] == "bundle":
                    with self.lock:
                        self.pending.pop(task[1]["mint"], None)
                if task and task[0] == "learn":
                    self.learn_queued = False
            except Exception as e:
                msg = str(e)[:80]
                if self.wi.key:
                    msg = msg.replace(self.wi.key, "<key>")
                self.log(f"  [phase2] task failed: {type(e).__name__}: {msg}")
                if task and task[0] == "bundle":
                    self.bundle_cache[task[1]["mint"]] = (time.time(), None)
                    with self.lock:
                        self.pending.pop(task[1]["mint"], None)
            finally:
                self.current = None

    def _do(self, task):
        kind = task[0]
        if kind == "bundle":
            c = task[1]
            t = time.time()
            res = self.wi.bundle_check(c)
            self.bundle_cache[c["mint"]] = (time.time(), res)
            with self.lock:
                self.pending.pop(c["mint"], None)
            if res is not None:
                if self.cfg["learn"]["record_scored_coins"]:
                    self.rb.record_coin(c, "scored", res)
                self.log(f"  [phase2] holders of {c['symbol'][:12]}: {'BUNDLED' if res['bundled'] else 'ok'} "
                         f"- {res['reason']} ({time.time() - t:.0f}s)")
        elif kind == "record":
            c, reason = task[1], task[2]
            hit = self.bundle_cache.get(c["mint"])
            n = self.rb.record_coin(c, reason, hit[1] if hit else None)
            self.log(f"  [phase3] risk book recorded {c['symbol'][:12]}: dev + {max(0, (n or 1) - 1)} holders")
        elif kind == "learn":
            self.learn_queued = False
            res = self.wi.build_book(self.store)
            if res.get("ran"):
                self.log(f"  [phase2] wallet book: {res['winners_found']} real winners known, "
                         f"{res['new_winners_processed']} newly processed, {res['wallets_in_book']} wallets in book")
            if self.rb.available:
                self.rb.resolve_outcomes(self.store)
                self.rb.recompute_tiers()
                t = dict(self.wi.db.execute("SELECT tier, COUNT(*) FROM wallet_tiers GROUP BY tier").fetchall())
                self.log("  [phase3] wallet risk book tiers: " + (", ".join(f"{k}={v}" for k, v in sorted(t.items())) or "empty"))

    def _profile_some(self, limit):
        if limit <= 0 or not self.wi.key:
            return
        w = self.cfg["wallets"]
        now = time.time()
        done = {r[0] for r in self.wi.db.execute("SELECT mint FROM trade_profiles")}
        cands = self.store.q("""SELECT * FROM coins WHERE band_ts IS NOT NULL AND quote_mint=? AND created_ts <= ?
                                AND created_ts >= ? ORDER BY band_ts DESC LIMIT 200""",
                             (self.cfg["feed"]["sol_quote_mint"], now - w["trade_profile_minutes"] * 60, now - 6 * 3600))
        n = 0
        for c in cands:
            if n >= limit:
                break
            if c["mint"] in done:
                continue
            n += 1
            t = time.time()
            prof = self.wi.trade_profile(dict(c))
            msg = (f"first-slot supply {prof['first_slot_supply_share']:.0%}, {prof['unique_buyers_window']}"
                   f"{'+' if prof['capped'] else ''} buyers / {prof['trade_count']} trades in {w['trade_profile_minutes']}m, "
                   f"{prof['multi_wallet_sigs']} multi-wallet signatures") if prof else "failed"
            if w["cluster_check_profiled_coins"]:
                b_ = self.wi.bundle_check(dict(c))
                if b_:
                    msg += f"; clusters: {'BUNDLED' if b_['bundled'] else 'ok'} ({b_['reason']})"
            self.log(f"  [phase2] profiled {c['symbol'][:12]}: {msg} ({time.time() - t:.0f}s)")
