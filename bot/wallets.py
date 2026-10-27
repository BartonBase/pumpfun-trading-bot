import json
import os
import sqlite3
import time

import requests

import nethttp

from store import DATA_DIR

WALLETS_DB = os.path.join(DATA_DIR, "wallets.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS funding (
  wallet TEXT PRIMARY KEY, funder TEXT, funder_type TEXT, funded_ts REAL,
  amount_sol REAL, source TEXT, busy INTEGER DEFAULT 0, checked_ts REAL
);
CREATE TABLE IF NOT EXISTS hubs (address TEXT PRIMARY KEY, is_hub INTEGER, n_sigs INTEGER, checked_ts REAL);
CREATE TABLE IF NOT EXISTS coin_bundle (
  mint TEXT, ts REAL, bundled INTEGER, reason TEXT, detail TEXT, PRIMARY KEY (mint, ts)
);
CREATE TABLE IF NOT EXISTS early_buyers (
  mint TEXT, wallet TEXT, slot INTEGER, block_time REAL, first_slot_buyer INTEGER,
  creator_funded INTEGER, root TEXT, PRIMARY KEY (mint, wallet)
);
CREATE TABLE IF NOT EXISTS wallet_stats (
  wallet TEXT PRIMARY KEY, winners INTEGER, unrelated_winners INTEGER, bundled_coins INTEGER,
  sold_into_strength INTEGER, profit_sol REAL, in_book INTEGER DEFAULT 0, detail TEXT, updated_ts REAL
);
CREATE TABLE IF NOT EXISTS trade_profiles (
  mint TEXT PRIMARY KEY, ts REAL, created_ts REAL, first_slot_supply_share REAL,
  first_slot_supply_share_ex_creator REAL, first_slot_buyers INTEGER, unique_buyers_window INTEGER,
  txs_parsed INTEGER, txs_in_window INTEGER, capped INTEGER
);
CREATE TABLE IF NOT EXISTS coin_wallets (
  mint TEXT, wallet TEXT, role TEXT, rank INTEGER, share REAL, cluster_size INTEGER,
  coin_bundled INTEGER, creator_linked INTEGER, first_slot INTEGER, creator TEXT, reason TEXT, recorded_ts REAL,
  PRIMARY KEY (mint, wallet, role)
);
CREATE TABLE IF NOT EXISTS coin_outcomes (
  mint TEXT PRIMARY KEY, outcome TEXT, peak_mult REAL, resolved_ts REAL
);
CREATE TABLE IF NOT EXISTS wallet_tiers (
  wallet TEXT PRIMARY KEY, tier TEXT, coins INTEGER, winners INTEGER, unrelated_winners INTEGER, rugs INTEGER,
  bundle_flags INTEGER, first_slot_count INTEGER, creator_linked_count INTEGER, dev_coins INTEGER, dev_rugs INTEGER,
  profitable_winners INTEGER, reason TEXT, updated_ts REAL
);
CREATE TABLE IF NOT EXISTS processed_winners (mint TEXT PRIMARY KEY, ts REAL, n_buyers INTEGER);
"""


WALLET_MIGRATIONS = {
    "trade_profiles": [("trade_count", "INTEGER"), ("multi_wallet_sigs", "INTEGER"), ("multi_wallet_supply_share", "REAL")],
    "wallet_tiers": [("resolved_coins", "INTEGER"), ("success_rate", "REAL")],
}


def migrate(db):
    for table, cols in WALLET_MIGRATIONS.items():
        have = {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
        for name, typ in cols:
            if name not in have:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
    db.commit()


class _UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def cluster_holders(holders, chains, creator, cfg_w, created_ts=None):
    uf = _UF()
    win = cfg_w["cex_timing_window_seconds"]
    hub_fundings = {}
    creator_linked = set()
    for h in holders:
        node = h["owner"]
        uf.find(node)
        if creator and node == creator:
            creator_linked.add(node)
        prev = node
        for hop in chains.get(node, []):
            f = hop.get("funder")
            if not f:
                break
            if creator and f == creator:
                creator_linked.add(node)
                uf.union("CREATOR", prev)
            if hop.get("hub"):
                hub_fundings.setdefault(f, []).append((prev, hop.get("ts")))
                break
            uf.union(f, prev)
            prev = f
    for hub, lst in hub_fundings.items():
        lst = sorted([x for x in lst if x[1] is not None], key=lambda x: x[1])
        for i in range(1, len(lst)):
            if lst[i][1] - lst[i - 1][1] <= win:
                uf.union(lst[i - 1][0], lst[i][0])
    for w in creator_linked:
        uf.union("CREATOR", w)

    fresh = []
    if created_ts and cfg_w.get("fresh_funding_window_minutes"):
        lo = created_ts - cfg_w["fresh_funding_window_minutes"] * 60
        for h in holders:
            ch = chains.get(h["owner"]) or []
            if ch and ch[0].get("ts") and lo <= ch[0]["ts"] <= created_ts + 3600:
                fresh.append((h["owner"], ch[0]["ts"]))
        fresh.sort(key=lambda x: x[1])
        for i in range(1, len(fresh)):
            if fresh[i][1] - fresh[i - 1][1] <= win:
                uf.union(fresh[i - 1][0], fresh[i][0])

    if not holders:
        return {"bundled": False, "reason": "no wallet holders found (all supply in program accounts or sold)",
                "n_holders": 0, "n_clusters": 0, "largest_cluster_share": 0.0, "creator_share": 0.0,
                "fresh_funded_holders": 0, "clusters": [], "holder_cluster": {}}
    total = sum(h["amount"] for h in holders) or 0.0
    clusters = {}
    for h in holders:
        r = uf.find(h["owner"])
        c = clusters.setdefault(r, {"wallets": [], "amount": 0.0})
        c["wallets"].append(h["owner"])
        c["amount"] += h["amount"]
    creator_root = uf.find("CREATOR") if "CREATOR" in uf.p else None
    cl = sorted(clusters.items(), key=lambda kv: -kv[1]["amount"])
    largest = cl[0][1]["amount"] / total if cl and total else 0.0
    creator_share = clusters.get(creator_root, {"amount": 0})["amount"] / total if creator_root and total else 0.0
    n_distinct = len(clusters)
    reasons = []
    if total and largest >= cfg_w["max_cluster_supply_share"] and len(cl[0][1]["wallets"]) > 1:
        reasons.append(f"largest funding cluster ({len(cl[0][1]['wallets'])} wallets) holds {largest:.0%} of top-holder supply")
    if len(holders) >= cfg_w["min_distinct_clusters"] and n_distinct < cfg_w["min_distinct_clusters"]:
        reasons.append(f"top {len(holders)} holders collapse into {n_distinct} funding clusters")
    if creator_share >= cfg_w["max_creator_cluster_share"]:
        reasons.append(f"creator-funded wallets hold {creator_share:.0%} of top-holder supply")
    return {
        "bundled": bool(reasons),
        "reason": "; ".join(reasons) if reasons else f"{n_distinct} clusters across {len(holders)} holders, largest {largest:.0%}",
        "n_holders": len(holders), "n_clusters": n_distinct,
        "largest_cluster_share": round(largest, 4), "creator_share": round(creator_share, 4),
        "fresh_funded_holders": len(fresh),
        "holder_cluster": {h["owner"]: {"root": uf.find(h["owner"]), "size": len(clusters[uf.find(h["owner"])]["wallets"]),
                                        "share": round(h["amount"] / total, 4) if total else 0,
                                        "creator_linked": h["owner"] in creator_linked} for h in holders},
        "clusters": [{"size": len(c["wallets"]), "share": round(c["amount"] / total, 4) if total else 0,
                      "wallets": c["wallets"][:10]} for _, c in cl[:8]],
    }


class WalletIntel:
    def __init__(self, cfg, rpc, log=print):
        self.cfg = cfg
        self.w = cfg["wallets"]
        self.rpc = rpc
        self.log = log
        from feed import helius_key
        self.key = helius_key(cfg)
        self.db = sqlite3.connect(WALLETS_DB, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        migrate(self.db)
        self.http = requests.Session()
        if self.key:
            self.http.headers["X-Api-Key"] = self.key
        self._funded_by_forbidden = False
        self._rest_last = 0.0

    def _rest_get(self, path, params=None):
        if not self.key:
            return None, None
        abort = getattr(self.rpc, "abort_check", None)
        if abort:
            abort()
        url = f"{self.w['helius_api_base'].rstrip('/')}{path}"
        for attempt in range(3):
            gap = self.w["helius_rest_min_gap_seconds"] - (time.time() - self._rest_last)
            if gap > 0:
                time.sleep(gap)
            self._rest_last = time.time()
            try:
                r = nethttp.request(self.http, "GET", url, read_timeout=20, params=params or {})
            except requests.RequestException as e:
                if isinstance(e, nethttp.HardTimeout):
                    self.http = nethttp.fresh_session(self.http)
                self.log(f"[wallets] Helius REST {path.split('/')[1]} error: {type(e).__name__}")
                return None, None
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            try:
                return r.status_code, r.json()
            except ValueError:
                return r.status_code, None
        return 429, None


    @property
    def bundle_check_available(self):
        return bool(self.key) or bool(self.w["enabled_without_key"])

    @property
    def wallet_confirm_available(self):
        return bool(self.key)

    def status_line(self):
        if self.key:
            return "Helius key present: funding-cluster bundle check + wallet book active (signals can be wallet_confirm)"
        if self.w["enabled_without_key"]:
            return "no Helius key: bundle check runs on public RPC (slow); wallet book off; signals filter_only"
        return "no HELIUS_API_KEY: bundle check and wallet book OFF; signals are filter_only"


    def _cached_funding(self, wallet):
        r = self.db.execute("SELECT * FROM funding WHERE wallet=?", (wallet,)).fetchone()
        if r and time.time() - r["checked_ts"] < self.w["cache_hours"] * 3600:
            return dict(r)
        return None

    def _helius_funded_by(self, wallet):
        if not self.key or self._funded_by_forbidden:
            return None
        status, d = self._rest_get(f"/v1/wallet/{wallet}/funded-by")
        if status == 403:
            self._funded_by_forbidden = True
            self.log("[wallets] Helius funded-by returned 403 (needs paid plan); using RPC tracing instead")
            return None
        if status == 404:
            return {"funder": None, "funder_type": None, "funded_ts": None, "amount_sol": None, "source": "helius"}
        if status != 200 or not d:
            return None
        return {"funder": d.get("funder"), "funder_type": d.get("funderType"), "funded_ts": d.get("timestamp"),
                "amount_sol": d.get("amount"), "source": "helius"}

    def _rpc_first_funding(self, wallet):
        before, oldest, pages = None, None, 0
        while pages < self.w["max_signature_pages"]:
            opts = {"limit": 1000}
            if before:
                opts["before"] = before
            sigs = self.rpc.call("getSignaturesForAddress", [wallet, opts])
            if sigs is None:
                return None
            pages += 1
            if not sigs:
                break
            oldest = sigs
            before = sigs[-1]["signature"]
            if len(sigs) < 1000:
                break
        else:
            return {"funder": None, "funder_type": "busy", "funded_ts": None, "amount_sol": None,
                    "source": "rpc", "busy": 1}
        if not oldest:
            return {"funder": None, "funder_type": None, "funded_ts": None, "amount_sol": None, "source": "rpc"}
        fetched = 0
        for s in reversed(oldest[-self.w["funding_scan_txs"]:]):
            tx = self.rpc.call("getTransaction", [s["signature"], {"encoding": "jsonParsed",
                                                                    "maxSupportedTransactionVersion": 1}])
            if not tx:
                continue
            fetched += 1
            for ins in _all_instructions(tx):
                p = ins.get("parsed")
                if not isinstance(p, dict) or ins.get("program") != "system":
                    continue
                info = p.get("info", {})
                dest = info.get("destination") or info.get("newAccount")
                if dest == wallet and info.get("source") and info.get("source") != wallet:
                    return {"funder": info["source"], "funder_type": None, "funded_ts": tx.get("blockTime"),
                            "amount_sol": (info.get("lamports") or 0) / 1e9, "source": "rpc"}
        if not fetched:
            return None
        return {"funder": None, "funder_type": None, "funded_ts": None, "amount_sol": None, "source": "rpc"}

    def funding(self, wallet):
        c = self._cached_funding(wallet)
        if c:
            return c
        f = self._helius_funded_by(wallet) or self._rpc_first_funding(wallet)
        if f is None:
            return None
        self.db.execute("""INSERT OR REPLACE INTO funding(wallet,funder,funder_type,funded_ts,amount_sol,source,busy,checked_ts)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (wallet, f["funder"], f["funder_type"], f["funded_ts"], f["amount_sol"], f["source"],
                         f.get("busy", 0), time.time()))
        self.db.commit()
        return f

    def is_hub(self, addr, funder_type=None):
        if funder_type in ("exchange", "busy"):
            return True
        r = self.db.execute("SELECT * FROM hubs WHERE address=?", (addr,)).fetchone()
        if r and time.time() - r["checked_ts"] < self.w["cache_hours"] * 3600:
            return bool(r["is_hub"])
        sigs = self.rpc.call("getSignaturesForAddress", [addr, {"limit": self.w["hub_min_signatures"]}])
        if sigs is None:
            return False
        hub = len(sigs) >= self.w["hub_min_signatures"]
        self.db.execute("INSERT OR REPLACE INTO hubs VALUES(?,?,?,?)", (addr, int(hub), len(sigs), time.time()))
        self.db.commit()
        return hub

    def chain(self, wallet):
        out, cur = [], wallet
        for _ in range(self.w["funding_hops"]):
            f = self.funding(cur)
            if not f or not f.get("funder"):
                break
            hub = self.is_hub(f["funder"], f.get("funder_type"))
            out.append({"funder": f["funder"], "ts": f.get("funded_ts"), "hub": hub, "type": f.get("funder_type")})
            if hub:
                break
            cur = f["funder"]
        return out


    def top_holders(self, mint, bonding_curve):
        res = self.rpc.call("getTokenLargestAccounts", [mint])
        if not res:
            return None
        accs = res.get("value") or []
        addrs = [a["address"] for a in accs]
        info = self.rpc.call("getMultipleAccounts", [addrs, {"encoding": "jsonParsed"}]) if addrs else None
        if not info:
            return None
        holders = []
        for a, acc in zip(accs, info.get("value") or []):
            try:
                owner = acc["data"]["parsed"]["info"]["owner"]
            except (TypeError, KeyError):
                continue
            if owner == bonding_curve:
                continue
            amt = float(a.get("uiAmount") or 0)
            if amt > 0:
                holders.append({"owner": owner, "amount": amt})

        owners = [h["owner"] for h in holders]
        oinfo = self.rpc.call("getMultipleAccounts", [owners, {"encoding": "base64", "dataSlice": {"offset": 0, "length": 0}}]) if owners else None
        if oinfo:
            sysprog = "11111111111111111111111111111111"
            keep = []
            for h, acc in zip(holders, oinfo.get("value") or []):
                if acc is None or acc.get("owner") == sysprog:
                    keep.append(h)
            holders = keep
        return holders[: self.w["top_holders"]]

    def bundle_check(self, coin):
        if not self.bundle_check_available:
            return None
        holders = self.top_holders(coin["mint"], coin.get("bonding_curve"))
        if holders is None:
            return None
        chains = {h["owner"]: self.chain(h["owner"]) for h in holders}
        res = cluster_holders(holders, chains, coin.get("creator"), self.w, coin.get("created_ts"))
        supply_tokens = 1e9
        res["top10_holder_share"] = round(sum(h["amount"] for h in holders[:10]) / supply_tokens, 4)
        if self.key:
            row = self.db.execute("SELECT * FROM trade_profiles WHERE mint=?", (coin["mint"],)).fetchone()
            early = dict(row) if row else self.trade_profile(coin, store_it=False)
            res["early"] = early
            if early and early["multi_wallet_supply_share"] >= self.w["max_multi_wallet_sig_supply_share"]:
                res["bundled"] = True
                res["reason"] += (f"; {early['multi_wallet_sigs']} single-signature multi-wallet buys took "
                                  f"{early['multi_wallet_supply_share']:.0%} of supply")
        if self.key:
            fs = [b for b in (self.early_buyers(coin["mint"], coin.get("bonding_curve")) or []) if b["first_slot"]]
            held = {h["owner"] for h in holders}
            fs_held = [b["wallet"] for b in fs if b["wallet"] in held]
            if fs_held:
                share = sum(h["amount"] for h in holders if h["owner"] in fs_held) / (sum(h["amount"] for h in holders) or 1)
                res["first_slot_holder_share"] = round(share, 4)
                if share >= self.w["max_cluster_supply_share"]:
                    res["bundled"] = True
                    res["reason"] += f"; first-{self.w['first_slots']}-slot buyers still hold {share:.0%}"
        self.db.execute("INSERT OR REPLACE INTO coin_bundle VALUES(?,?,?,?,?)",
                        (coin["mint"], time.time(), int(res["bundled"]), res["reason"], json.dumps(res)))
        self.db.commit()
        return res


    def _oldest_signatures(self, address, max_pages):
        before, pages, collected = None, 0, []
        while pages < max_pages:
            opts = {"limit": 1000}
            if before:
                opts["before"] = before
            sigs = self.rpc.call("getSignaturesForAddress", [address, opts])
            if sigs is None:
                return None, False
            pages += 1
            if not sigs:
                return list(reversed(collected)), True
            collected = sigs
            before = sigs[-1]["signature"]
            if len(sigs) < 1000:
                return list(reversed(collected)), True
        return list(reversed(collected)), False

    def _early_txs(self, mint, bonding_curve, window_s=None, cap=None):
        sigs, complete = self._oldest_signatures(bonding_curve, self.w["max_signature_pages"] * 10)
        if not sigs or not complete:
            return None
        t0 = next((s.get("blockTime") for s in sigs if s.get("blockTime")), None)
        slot0 = sigs[0]["slot"]
        chosen = [s for s in sigs if not s.get("err") and (window_s is None or t0 is None
                                                           or (s.get("blockTime") or t0) <= t0 + window_s)]
        n_window = len(chosen)
        capped = cap is not None and n_window > cap
        chosen = chosen[:cap] if cap else chosen
        txs = []
        for s in chosen:
            tx = self.rpc.call("getTransaction", [s["signature"], {"encoding": "jsonParsed",
                                                                    "maxSupportedTransactionVersion": 1}])
            if not tx:
                continue
            deltas = {o: d for o, d in _token_deltas(tx, mint).items() if o != bonding_curve and o}
            txs.append({"slot": s["slot"], "block_time": s.get("blockTime"), "deltas": deltas})
        return {"t0": t0, "slot0": slot0, "txs": txs, "n_window": n_window, "capped": capped}

    def early_buyers(self, mint, bonding_curve, limit=None):
        if not bonding_curve:
            return None
        e = self._early_txs(mint, bonding_curve, cap=limit or self.w["early_buyers"])
        if not e:
            return None
        buyers = {}
        for t in e["txs"]:
            for owner, d in t["deltas"].items():
                if d > 0 and owner not in buyers:
                    buyers[owner] = {"wallet": owner, "slot": t["slot"], "block_time": t["block_time"],
                                     "first_slot": t["slot"] < e["slot0"] + self.w["first_slots"]}
        return list(buyers.values())

    def trade_profile(self, coin, total_supply_raw=1e15, store_it=True, window_minutes=None):
        if store_it:
            r = self.db.execute("SELECT * FROM trade_profiles WHERE mint=?", (coin["mint"],)).fetchone()
            if r:
                return dict(r)
        wm = window_minutes or self.w["trade_profile_minutes"]
        e = self._early_txs(coin["mint"], coin["bonding_curve"], window_s=wm * 60, cap=self.w["trade_profile_max_txs"])
        if not e or not e["txs"]:
            return None
        first = [t for t in e["txs"] if t["slot"] < e["slot0"] + self.w["first_slots"]]
        bought = {}
        for t in first:
            for o, d in t["deltas"].items():
                if d > 0:
                    bought[o] = bought.get(o, 0) + d
        multi = [t for t in e["txs"] if sum(1 for d in t["deltas"].values() if d > 0) >= 2]
        multi_supply = sum(d for t in multi for d in t["deltas"].values() if d > 0)
        buyers = {o for t in e["txs"] for o, d in t["deltas"].items() if d > 0}
        prof = {"mint": coin["mint"], "ts": time.time(), "created_ts": coin.get("created_ts"),
                "first_slot_supply_share": round(sum(bought.values()) / total_supply_raw, 4),
                "first_slot_supply_share_ex_creator": round(sum(v for o, v in bought.items() if o != coin.get("creator")) / total_supply_raw, 4),
                "first_slot_buyers": len(bought), "unique_buyers_window": len(buyers), "txs_parsed": len(e["txs"]),
                "txs_in_window": e["n_window"], "capped": int(e["capped"]), "trade_count": len(e["txs"]),
                "multi_wallet_sigs": len(multi), "multi_wallet_supply_share": round(multi_supply / total_supply_raw, 4)}
        if store_it:
            cols = list(prof)
            self.db.execute(f"INSERT OR REPLACE INTO trade_profiles({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                            [prof[c] for c in cols])
            self.db.commit()
        return prof

    def _helius_swaps(self, address, limit=100):
        status, d = self._rest_get(f"/v0/addresses/{address}/transactions", {"limit": limit})
        return d if status == 200 and isinstance(d, list) else None

    def wallet_confirm(self, coin, now=None):
        if not self.wallet_confirm_available:
            return None
        book = {r["wallet"] for r in self.db.execute("SELECT wallet FROM wallet_stats WHERE in_book=1")}
        book |= {r["wallet"] for r in self.db.execute("SELECT wallet FROM wallet_tiers WHERE tier='follow'")}
        if not book:
            return False
        now = now or time.time()
        txs = self._helius_swaps(coin["bonding_curve"]) or []
        win = self.cfg["signal"]["wallet_window_seconds"]
        hits = {}
        for t in txs:
            ts = t.get("timestamp") or 0
            if now - ts > win:
                continue
            for tt in t.get("tokenTransfers") or []:
                if tt.get("mint") == coin["mint"] and tt.get("toUserAccount") in book:
                    hits.setdefault(tt["toUserAccount"], ts)
        if len(hits) < self.cfg["signal"]["min_confirm_wallets"]:
            return False
        times = sorted(hits.values())
        return times[-1] - times[0] <= win


    def build_book(self, store):
        if not self.key:
            return {"ran": False, "reason": "no HELIUS_API_KEY"}
        mult = self.w["real_winner_multiple"]

        winners = store.q("""SELECT * FROM coins WHERE band_ts IS NOT NULL AND band_mcap_sol>0
                             AND peak_mcap_sol_after_band >= band_mcap_sol*?""", (mult,))
        done = {r["mint"] for r in self.db.execute("SELECT mint FROM processed_winners")}
        new = 0
        for c in winners:
            if c["mint"] in done:
                continue
            if new >= self.w["book_winners_per_refresh"]:
                break
            buyers = self.early_buyers(c["mint"], c["bonding_curve"]) or []
            for b in buyers:
                ch = self.chain(b["wallet"])
                creator_funded = any(h["funder"] == c["creator"] for h in ch)
                root = ch[-1]["funder"] if ch else b["wallet"]
                self.db.execute("INSERT OR REPLACE INTO early_buyers VALUES(?,?,?,?,?,?,?)",
                                (c["mint"], b["wallet"], b["slot"], b["block_time"], int(b["first_slot"]),
                                 int(creator_funded), root))
            self.db.execute("INSERT OR REPLACE INTO processed_winners VALUES(?,?,?)", (c["mint"], time.time(), len(buyers)))
            new += 1
        self.db.commit()
        self._score_wallets(store)
        n_book = self.db.execute("SELECT COUNT(*) FROM wallet_stats WHERE in_book=1").fetchone()[0]
        return {"ran": True, "winners_found": len(winners), "new_winners_processed": new, "wallets_in_book": n_book}

    def _score_wallets(self, store):
        rows = self.db.execute("""SELECT eb.*, cb.bundled FROM early_buyers eb LEFT JOIN
                                  (SELECT mint, MAX(bundled) bundled FROM coin_bundle GROUP BY mint) cb
                                  ON cb.mint=eb.mint""").fetchall()
        per = {}
        for r in rows:
            per.setdefault(r["wallet"], []).append(r)
        for wallet, lst in per.items():
            if any(r["first_slot_buyer"] or r["creator_funded"] for r in lst):
                in_book, why = 0, "first-slot or creator-funded on some coin"
            else:
                bundled = sum(1 for r in lst if r["bundled"])
                creators, roots = set(), set()
                for r in lst:
                    c = store.get_coin(r["mint"])
                    creators.add(c["creator"] if c else None)
                    roots.add(r["root"])
                unrelated = min(len(creators), len(lst))
                if unrelated < self.w["min_winners_per_wallet"]:

                    self.db.execute("""INSERT OR REPLACE INTO wallet_stats VALUES(?,?,?,?,?,?,?,?,?)""",
                                    (wallet, len(lst), unrelated, bundled, 0, 0.0, 0,
                                     f"unrelated winners {unrelated} (< {self.w['min_winners_per_wallet']}): "
                                     f"sell behaviour not checked", time.time()))
                    continue
                strength, profit = self._sold_into_strength(wallet, [r["mint"] for r in lst])
                in_book = int(unrelated >= self.w["min_winners_per_wallet"] and strength >= self.w["min_winners_per_wallet"]
                              and not (bundled >= 1 and len(lst) == 1))
                why = f"unrelated winners {unrelated}, sold into strength on {strength}, bundled coins {bundled}"
                self.db.execute("""INSERT OR REPLACE INTO wallet_stats VALUES(?,?,?,?,?,?,?,?,?)""",
                                (wallet, len(lst), unrelated, bundled, strength, profit, in_book, why, time.time()))
                continue
            self.db.execute("""INSERT OR REPLACE INTO wallet_stats VALUES(?,?,?,?,?,?,?,?,?)""",
                            (wallet, len(lst), 0, 0, 0, 0.0, in_book, why, time.time()))
        self.db.commit()

    def _sold_into_strength(self, wallet, mints):
        txs = self._helius_swaps(wallet) or []
        count, profit = 0, 0.0
        for m in mints:
            sol_in = sol_out = tok_in = tok_out = 0.0
            for t in txs:
                for tt in t.get("tokenTransfers") or []:
                    if tt.get("mint") != m:
                        continue
                    amt = float(tt.get("tokenAmount") or 0)
                    sol = sum(abs(n.get("amount", 0)) for n in t.get("nativeTransfers") or []
                              if wallet in (n.get("fromUserAccount"), n.get("toUserAccount"))) / 1e9
                    if tt.get("toUserAccount") == wallet:
                        tok_in += amt
                        sol_in += sol
                    elif tt.get("fromUserAccount") == wallet:
                        tok_out += amt
                        sol_out += sol
            if tok_in and tok_out:
                if (sol_out / tok_out) >= self.w["sold_into_strength_ratio"] * (sol_in / tok_in):
                    count += 1
                profit += sol_out - sol_in
        return count, round(profit, 4)


def _all_instructions(tx):
    msg = tx.get("transaction", {}).get("message", {})
    out = list(msg.get("instructions", []))
    for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
        out.extend(inner.get("instructions", []))
    return out


def _token_deltas(tx, mint):
    meta = tx.get("meta") or {}
    pre = {(b.get("owner"), b["accountIndex"]): float(b["uiTokenAmount"].get("amount") or 0)
           for b in meta.get("preTokenBalances") or [] if b.get("mint") == mint}
    post = {(b.get("owner"), b["accountIndex"]): float(b["uiTokenAmount"].get("amount") or 0)
            for b in meta.get("postTokenBalances") or [] if b.get("mint") == mint}
    out = {}
    for k in set(pre) | set(post):
        out[k[0]] = out.get(k[0], 0) + post.get(k, 0) - pre.get(k, 0)
    return out




def classify_wallet(st_, L):
    if st_["bundle_flags"] >= L["bundler_min_coins"] or st_["first_slot_count"] >= L["bundler_min_coins"]:
        return "bundler", (f"in a bundled funding cluster on {st_['bundle_flags']} coins, "
                           f"first-slot buyer on {st_['first_slot_count']}")
    if st_["dev_rugs"] >= L["risky_dev_rugs"]:
        return "risky", f"dev of {st_['dev_rugs']} coin(s) that dumped"
    if st_["creator_linked_count"] >= L["risky_creator_linked"]:
        return "risky", f"funded by the coin's creator on {st_['creator_linked_count']} coin(s)"
    if st_["bundle_flags"] >= 1 or st_["first_slot_count"] >= 1:
        return "risky", "seen once in a bundle cluster / first slots"
    if st_["rugs"] >= L["risky_min_rugs"] and st_["winners"] == 0:
        return "risky", f"held {st_['rugs']} coins that dumped, no winners"
    if (st_.get("resolved_coins", 0) >= L["skill_min_tokens"] and (st_.get("success_rate") or 0) >= L["skill_min_success_rate"]
            and st_["unrelated_winners"] >= L["follow_min_winners"] and st_["creator_linked_count"] == 0
            and st_["bundle_flags"] == 0 and st_["first_slot_count"] == 0
            and (not L["follow_require_profit"] or st_["profitable_winners"] >= L["follow_min_winners"])):
        return "follow", (f"{st_.get('resolved_coins')} prior coins, {st_.get('success_rate', 0):.0%} success; "
                          f"early in {st_['unrelated_winners']} unrelated winners, profitable on "
                          f"{st_['profitable_winners']}, never dev-funded or clustered")
    if st_["coins"] >= L["clean_min_coins"]:
        busy = " (busy, not skilled)" if st_["coins"] >= L["skill_min_tokens"] else ""
        return "clean", f"seen on {st_['coins']} coins with no red flags{busy}"
    return "unknown", f"seen on {st_['coins']} coin(s); not enough history"


class RiskBook:

    def __init__(self, wi, cfg):
        self.wi, self.cfg, self.L = wi, cfg, cfg["learn"]
        self.db = wi.db

    @property
    def available(self):
        return self.wi.bundle_check_available and self.L["wallet_risk_book"]

    def record_coin(self, coin, reason, bundle=None):
        if not self.available:
            return None
        bundle = bundle or self.wi.bundle_check(coin)
        if not bundle:
            return None
        hc = bundle.get("holder_cluster") or {}
        ranked = sorted(hc.items(), key=lambda kv: -kv[1]["share"])[: self.L["top_holders"]]
        first_slot = set()
        if self.L["record_first_slot_buyers"]:
            fs = self.wi.early_buyers(coin["mint"], coin.get("bonding_curve"), limit=self.L["first_slot_scan_txs"]) or []
            first_slot = {b["wallet"] for b in fs if b["first_slot"]}
        now = time.time()
        rows = [(coin["mint"], coin.get("creator"), "dev", 0, 0.0, 0, int(bundle["bundled"]), 0,
                 int(coin.get("creator") in first_slot), coin.get("creator"), reason, now)] if coin.get("creator") else []
        for i, (wal, info) in enumerate(ranked, 1):
            rows.append((coin["mint"], wal, "holder", i, info["share"], info["size"], int(bundle["bundled"]),
                         int(info["creator_linked"]), int(wal in first_slot), coin.get("creator"), reason, now))
        self.db.executemany("INSERT OR REPLACE INTO coin_wallets VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        self.db.commit()
        return len(rows)

    def resolve_outcomes(self, store):
        L, now = self.L, time.time()
        mints = [r[0] for r in self.db.execute("""SELECT DISTINCT mint FROM coin_wallets WHERE mint NOT IN
                                                  (SELECT mint FROM coin_outcomes WHERE outcome<>'open')""")]
        for m in mints:
            c = store.get_coin(m)
            if not c:
                continue
            base = c["band_mcap_sol"]
            peak = (c["peak_mcap_sol_after_band"] or 0) / base if base else None
            ath_mult = (c["ath_mcap_usd"] / c["band_mcap_usd"]) if c["ath_mcap_usd"] and c["band_mcap_usd"] else None
            pm = max(x for x in (peak or 0, ath_mult or 0))
            cur = (c["mcap_sol"] / base) if base and c["mcap_sol"] else None
            age_h = (now - (c["band_ts"] or c["first_seen_ts"])) / 3600
            if pm >= self.cfg["wallets"]["real_winner_multiple"]:
                out = "winner"
            elif cur is not None and cur <= L["dump_max_mult"] and age_h * 60 >= L["outcome_min_minutes"]:
                out = "dumped"
            elif age_h >= L["outcome_max_hours"]:
                out = "faded"
            else:
                out = "open"
            self.db.execute("INSERT OR REPLACE INTO coin_outcomes VALUES(?,?,?,?)", (m, out, round(pm, 3), now))
        self.db.commit()

    def recompute_tiers(self):
        L = self.L
        cutoff = time.time() - L["skill_test_hours"] * 3600


        rows = self.db.execute("""SELECT cw.*, co.outcome FROM coin_wallets cw LEFT JOIN coin_outcomes co
                                  ON co.mint=cw.mint""").fetchall()
        per = {}
        for r in rows:
            per.setdefault(r["wallet"], []).append(r)
        now = time.time()
        for wal, lst in per.items():
            holder = [r for r in lst if r["role"] == "holder"]
            dev = [r for r in lst if r["role"] == "dev"]
            pre = [r for r in holder if (r["recorded_ts"] or 0) < cutoff]
            win = [r for r in pre if r["outcome"] == "winner"]
            st_ = {"coins": len({r["mint"] for r in lst}), "winners": len(win),
                   "unrelated_winners": len({r["creator"] for r in win}),
                   "rugs": sum(1 for r in holder if r["outcome"] == "dumped"),
                   "bundle_flags": sum(1 for r in holder if r["coin_bundled"] and r["cluster_size"] >= 2),

                   "first_slot_count": sum(1 for r in holder if r["first_slot"]),
                   "creator_linked_count": sum(1 for r in holder if r["creator_linked"]),
                   "dev_coins": len(dev), "dev_rugs": sum(1 for r in dev if r["outcome"] == "dumped"),
                   "profitable_winners": 0}
            resolved = [r for r in pre if r["outcome"] in ("winner", "dumped", "faded")]
            st_["resolved_coins"] = len(resolved)
            st_["success_rate"] = (sum(1 for r in resolved if r["outcome"] == "winner") / len(resolved)) if resolved else None
            if L["follow_require_profit"] and st_["unrelated_winners"] >= L["follow_min_winners"]:
                st_["profitable_winners"], _ = self.wi._sold_into_strength(wal, [r["mint"] for r in win])
            tier, why = classify_wallet(st_, L)
            self.db.execute("""INSERT OR REPLACE INTO wallet_tiers(wallet,tier,coins,winners,unrelated_winners,rugs,
                bundle_flags,first_slot_count,creator_linked_count,dev_coins,dev_rugs,profitable_winners,reason,updated_ts,
                resolved_coins,success_rate) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (wal, tier, st_["coins"], st_["winners"], st_["unrelated_winners"], st_["rugs"],
                             st_["bundle_flags"], st_["first_slot_count"], st_["creator_linked_count"],
                             st_["dev_coins"], st_["dev_rugs"], st_["profitable_winners"], why, now,
                             st_["resolved_coins"], st_["success_rate"]))
        self.db.commit()

    def skill_test(self):
        cutoff = time.time() - self.L["skill_test_hours"] * 3600
        follow = {r[0] for r in self.db.execute("SELECT wallet FROM wallet_tiers WHERE tier='follow'")}
        rows = self.db.execute("""SELECT cw.mint, cw.wallet, co.outcome FROM coin_wallets cw JOIN coin_outcomes co
                                  ON co.mint=cw.mint WHERE cw.recorded_ts >= ? AND cw.role='holder'
                                  AND co.outcome IN ('winner','dumped','faded')""", (cutoff,)).fetchall()
        coins = {}
        for r in rows:
            c = coins.setdefault(r["mint"], {"outcome": r["outcome"], "follow": 0})
            c["follow"] += r["wallet"] in follow
        def rate(sel):
            return (sum(1 for c in sel if c["outcome"] == "winner") / len(sel)) if sel else None
        w_f = [c for c in coins.values() if c["follow"]]
        n_f = [c for c in coins.values() if not c["follow"]]
        base = rate(list(coins.values()))
        return {"test_coins": len(coins), "with_follow": len(w_f), "success_with_follow": rate(w_f),
                "success_without": rate(n_f), "base_rate": base,
                "lift": (rate(w_f) / base) if (w_f and base) else None}

    def tiers_for(self, wallets_):
        wallets_ = [w for w in wallets_ if w]
        if not wallets_:
            return {}
        q = ",".join("?" * len(wallets_))
        return {r["wallet"]: r["tier"] for r in self.db.execute(
            f"SELECT wallet, tier FROM wallet_tiers WHERE wallet IN ({q})", wallets_)}

    def hits(self, bundle, creator):
        holders = list((bundle or {}).get("holder_cluster", {}).keys())[: self.L["top_holders"]]
        tiers = self.tiers_for(holders + [creator])
        h = {"follow": 0, "risky": 0, "bundler": 0, "clean": 0, "dev_tier": tiers.get(creator, "unknown")}
        for w in holders:
            t = tiers.get(w)
            if t in h:
                h[t] += 1
        return h
