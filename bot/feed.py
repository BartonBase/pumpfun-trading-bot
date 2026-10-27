import base64
import os
import statistics
import struct
import threading
import time

import requests

import nethttp

CURVE_DISCRIMINATOR = bytes.fromhex("17b7f83760d8ac60")




USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDC_MINT_BYTES = bytes.fromhex("c6fa7af3bedbad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61")
SOL_USD_FN = None


def sol_usd():
    try:
        v = SOL_USD_FN() if SOL_USD_FN else None
        return float(v) if v and float(v) > 0 else None
    except Exception:
        return None


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def normalize_coin(raw, cfg):
    sol_quote = cfg["feed"]["sol_quote_mint"]
    init_tokens = cfg["curve"]["initial_real_token_reserves"]
    quote = raw.get("quote_mint") or sol_quote
    rtr = _f(raw.get("real_token_reserves"))
    curve_pct = None
    if rtr is not None and init_tokens:
        curve_pct = max(0.0, min(100.0, (1 - rtr / init_tokens) * 100))
    if raw.get("complete"):
        curve_pct = 100.0
    real_sol = None
    usdc = quote == USDC_MINT
    if quote == sol_quote and raw.get("real_sol_reserves") is not None:
        real_sol = _f(raw.get("real_sol_reserves")) / 1e9
    mcap_usd = _f(raw.get("usd_market_cap"))
    if mcap_usd is None:
        mcap_usd = _f(raw.get("market_cap_usd"))
    mcap_sol_usdc = None
    if usdc:

        sp = sol_usd()
        mcap_sol_usdc = (mcap_usd / sp) if (mcap_usd and sp) else None
    ms = lambda k: (_f(raw.get(k)) / 1000.0) if raw.get(k) else None
    mayhem = raw.get("mayhem_state")
    return {
        "mint": raw.get("mint"),
        "symbol": (raw.get("symbol") or "").strip(),
        "name": (raw.get("name") or "").strip(),
        "creator": raw.get("creator"),
        "bonding_curve": raw.get("bonding_curve"),
        "created_ts": ms("created_timestamp"),
        "quote_mint": quote,
        "mayhem_state": mayhem if mayhem else None,
        "nsfw": 1 if raw.get("nsfw") else 0,
        "has_socials": 1 if (raw.get("twitter") or raw.get("telegram") or raw.get("website")) else 0,
        "reply_count": int(raw.get("reply_count") or 0),
        "complete": 1 if raw.get("complete") else 0,

        "mcap_sol": _f(raw.get("market_cap")) if quote == sol_quote else mcap_sol_usdc,
        "mcap_usd": mcap_usd,
        "curve_pct": curve_pct,
        "real_sol": real_sol,
        "ath_mcap_usd": _f(raw.get("ath_market_cap")),
        "ath_ts": ms("ath_market_cap_timestamp"),
        "last_trade_ts": ms("last_trade_timestamp"),
    }


def sol_usd_from_coins(raws, cfg):
    sol_quote = cfg["feed"]["sol_quote_mint"]
    ratios = []
    for r in raws:
        if (r.get("quote_mint") or sol_quote) != sol_quote:
            continue
        u, s = _f(r.get("usd_market_cap")), _f(r.get("market_cap"))
        if u and s and s > 0:
            ratios.append(u / s)
    return statistics.median(ratios) if ratios else None


class PumpFeed:
    def __init__(self, cfg, store=None, log=print):
        self.cfg = cfg["feed"]
        self.full_cfg = cfg
        self.store = store
        self.log = log
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": self.cfg["user_agent"],
            "Origin": "https://pump.fun",
            "Referer": "https://pump.fun/",
        })
        self._last = 0.0
        self.stats = {"ok": 0, "rate_limited": 0, "error": 0}

    def _wait_gap(self):
        gap = self.cfg["min_request_gap_seconds"] - (time.time() - self._last)
        if gap > 0:
            time.sleep(gap)

    def get_coins(self, label, **params):
        url = self.cfg["base_url"].rstrip("/") + "/coins"
        params.setdefault("limit", self.cfg["page_limit"])
        params.setdefault("includeNsfw", "true" if self.cfg["include_nsfw"] else "false")
        backoff = self.cfg["backoff_seconds"]
        for attempt in range(self.cfg["max_retries"] + 1):
            self._wait_gap()
            self._last = time.time()
            try:
                r = nethttp.request(self.session, "GET", url, read_timeout=self.cfg["timeout_seconds"], params=params)
            except requests.RequestException as e:
                if isinstance(e, nethttp.HardTimeout):
                    self.session = nethttp.fresh_session(self.session)
                self.stats["error"] += 1
                self._log(label, f"error:{type(e).__name__}")
                return None
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    self.stats["error"] += 1
                    self._log(label, "error:bad_json")
                    return None
                if not isinstance(data, list):
                    self.stats["error"] += 1
                    self._log(label, "error:unexpected_shape")
                    return None
                self.stats["ok"] += 1
                self._log(label, "200", len(data))
                return data
            if r.status_code == 429:
                self.stats["rate_limited"] += 1
                self._log(label, "429")
                if attempt < self.cfg["max_retries"]:
                    wait = min(backoff, self.cfg["backoff_max_seconds"])
                    self.log(f"  feed rate-limited on {label}; waiting {wait:.0f}s (retry {attempt + 1}/{self.cfg['max_retries']})")
                    time.sleep(wait)
                    backoff *= 2
                    continue
                return None
            self.stats["error"] += 1
            self._log(label, f"http_{r.status_code}")
            return None
        return None

    def _log(self, label, status, n=0):
        if self.store:
            self.store.log_feed("pumpfun", label, status, n)


    def newest(self, offset=0):
        return self.get_coins(f"new@{offset}", offset=offset, sort="created_timestamp", order="DESC")

    def active(self):
        return self.get_coins("active", offset=0, sort="last_trade_timestamp", order="DESC", complete="false")

    def graduated(self, offset=0):
        return self.get_coins(f"graduated@{offset}", offset=offset, sort="created_timestamp", order="DESC",
                              complete="true")


def decode_curve(b64data, init_tokens):
    raw = base64.b64decode(b64data)
    if len(raw) < 49 or raw[:8] != CURVE_DISCRIMINATOR:
        return None
    vtok, vsol, rtok, rsol, supply = struct.unpack_from("<5Q", raw, 8)
    complete = bool(raw[48])
    if vtok == 0:
        return None
    qb = raw[83:115] if len(raw) >= 115 else bytes(32)
    curve_pct = 100.0 if complete else max(0.0, min(100.0, (1 - rtok / init_tokens) * 100))
    if qb == bytes(32):
        mcap_sol = (vsol / 1e9) * (supply / vtok)
        return {"mcap_sol": mcap_sol, "curve_pct": curve_pct, "real_sol": rsol / 1e9, "complete": complete,
                "virtual_sol": vsol, "virtual_tokens": vtok, "quote": "SOL"}
    if qb == USDC_MINT_BYTES:
        sp = sol_usd()
        if not sp:
            return None
        mcap_usdc = (vsol / 1e6) * (supply / vtok)
        return {"mcap_sol": mcap_usdc / sp, "curve_pct": curve_pct, "real_sol": None, "complete": complete,
                "virtual_sol": vsol, "virtual_tokens": vtok, "quote": "USDC", "mcap_usdc": mcap_usdc}
    return None


def decode_curve_complete(b64data):
    raw = base64.b64decode(b64data)
    if len(raw) < 49 or raw[:8] != CURVE_DISCRIMINATOR:
        return None
    return bool(raw[48])


def helius_key(cfg):
    if not (cfg.get("data_sources") or {}).get("helius_enabled", False):
        return None
    return os.environ.get("HELIUS_API_KEY") or None


class SolanaRPC:
    _lock = threading.Lock()
    _shared_last = {}
    """Read-only JSON-RPC client. Only 'get*' methods are allowed."""

    def __init__(self, cfg, store=None):
        self.cfg = cfg["rpc"]
        self.init_tokens = cfg["curve"]["initial_real_token_reserves"]
        self.store = store
        key = helius_key(cfg)
        self.using_helius = bool(key)
        self.url = self.cfg["helius_url_template"].format(key=key) if key else self.cfg["public_url"]
        self.session = requests.Session()
        self.gap = self.cfg["helius_min_request_gap_seconds"] if key else self.cfg["min_request_gap_seconds"]
        self._last = 0.0
        self.abort_check = None
        self._id = 0

    @property
    def label(self):
        return "helius-rpc" if self.using_helius else "public-rpc"

    def call(self, method, params):
        if not method.startswith("get"):
            raise PermissionError("read-only RPC client: only get* methods are allowed")
        if self.abort_check:
            self.abort_check()
        with SolanaRPC._lock:
            gap = self.gap - (time.time() - SolanaRPC._shared_last.get(self.url, 0.0))
            if gap > 0:
                time.sleep(gap)
            SolanaRPC._shared_last[self.url] = self._last = time.time()
        self._id += 1
        for attempt in range(3):
            try:
                r = nethttp.request(self.session, "POST", self.url, read_timeout=self.cfg["timeout_seconds"],
                                    json={"jsonrpc": "2.0", "id": self._id, "method": method, "params": params})
            except requests.RequestException as e:
                if isinstance(e, nethttp.HardTimeout):
                    self.session = nethttp.fresh_session(self.session)
                self._log(method, f"error:{type(e).__name__}")
                return None
            if r.status_code == 429:
                self._log(method, "429")
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code != 200:
                self._log(method, f"http_{r.status_code}")
                return None
            try:
                body = r.json()
            except ValueError:
                self._log(method, "error:bad_json")
                return None
            if "error" in body:
                self._log(method, f"rpc_error:{body['error'].get('code')}")
                return None
            return body.get("result")
        return None

    def _log(self, method, status, n=0):
        if self.store:
            self.store.log_feed(self.label, method, status, n)

    def curve_states(self, curve_addresses):
        return self.curve_states_ex(curve_addresses)[0]

    def curve_states_ex(self, curve_addresses):
        out = {}
        completed = set()
        addrs = [a for a in curve_addresses if a]
        bs = self.cfg["batch_size"]
        for i in range(0, len(addrs), bs):
            chunk = addrs[i:i + bs]
            res = self.call("getMultipleAccounts", [chunk, {"encoding": "base64"}])
            if not res:
                continue
            for addr, acc in zip(chunk, res.get("value") or []):
                if acc and acc.get("data"):
                    st = decode_curve(acc["data"][0], self.init_tokens)
                    if st:
                        out[addr] = st
                    if (st and st["complete"]) or decode_curve_complete(acc["data"][0]):
                        completed.add(addr)
            self._log("getMultipleAccounts", "200", len(chunk))
        return out, completed
