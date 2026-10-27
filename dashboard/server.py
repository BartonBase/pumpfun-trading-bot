
from __future__ import annotations
import json, os, re, secrets, sqlite3, time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
PAPER = Path(os.environ.get("PAPER_ROOT", str(Path(__file__).resolve().parent.parent / "bot")))
PAPERB = Path(os.environ.get("PAPERB_ROOT", str(PAPER.parent / "paperb2")))
PAPERE = Path(os.environ.get("PAPERE_ROOT", str(PAPER.parent / "papere")))
PAPERA = Path(os.environ.get("PAPERA_ROOT", str(PAPER.parent / "papera")))
PAPERG = Path(os.environ.get("PAPERG_ROOT", str(PAPER.parent / "paperg")))
import auditcost
LIVE = Path(os.environ.get("LIVE_ROOT", str(Path(__file__).resolve().parent.parent / "live_disabled")))
HOST, PORT = os.environ.get("HOST", "127.0.0.1"), int(os.environ.get("PORT", "8792"))
TZ = ZoneInfo("America/Boise")
TOKEN = (HERE / ".token").read_text().strip() if (HERE / ".token").exists() else os.environ.get("DASH_TOKEN", "")
PAPER_RECENT, LIVE_RECENT = 40, 25
WALLET = os.environ.get("WALLET_ADDRESS", "")


REASONS = {"scale1": "take profit 1", "scale2": "take profit 2", "scale3": "take profit 3", "trail": "trailing stop",
           "hard_stop": "hard stop", "stall": "stall exit", "stall_failed": "stall (retry fail)", "stall_retry": "stall retry",
           "time_stop": "time stop", "graduated_exit": "graduated", "graduated_unsold": "graduated (sell by hand)",
           "early_stop": "early stop", "early_check": "early check", "stale_price": "stale price", "stale": "stale price", "no_tokens": "no tokens in wallet", "manual": "manual"}
HDR_RE = re.compile(r"^=== (\d\d:\d\d:\d\d \w+) \| poll (\d+)(?: \| REGIME: (\S+))?(?: \| open (\d+)/(\d+))?.*?\| day PnL ([+-]?[\d.]+) SOL(.*?)===\s*$")
BAL_RE = re.compile(r"LIVE wallet ([\d.]+) SOL")


def ro(path):
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    return con


def q(path, sql, args=(), tries=4):
    for i in range(tries):
        try:
            con = ro(path)
            try:
                return [dict(r) for r in con.execute(sql, args)]
            finally:
                con.close()
        except sqlite3.OperationalError as e:
            if i == tries - 1 or not any(w in str(e) for w in ("locked", "busy")):
                raise
            time.sleep(0.3 * (i + 1))


def local(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%-I:%M %p") if ts else None


def local_day(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%b %-d") if ts else None


def jload(p, default=None):
    try:
        with open(p) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def log_header(root):
    out = {"log_at": None, "poll": None, "regime": None, "open_slots": None, "max_open": None, "day_pnl_sol": None,
           "balance_sol": None, "balance_at": None, "balance_note": None}
    try:
        with open(root / "data" / "bot.log", "rb") as f:
            f.seek(0, 2); f.seek(max(0, f.tell() - 200_000))
            lines = f.read().decode("utf-8", "replace").splitlines()
    except OSError:
        return out
    for line in reversed(lines):
        m = HDR_RE.match(line)
        if not m:
            continue
        if out["log_at"] is None:
            out.update(log_at=m.group(1), poll=int(m.group(2)), regime=None,
                       open_slots=int(m.group(4)) if m.group(4) else None, max_open=int(m.group(5)) if m.group(5) else None,
                       day_pnl_sol=float(m.group(6)))
        tail = m.group(7)
        if "LIVE" not in tail:
            break
        b = BAL_RE.search(tail)
        if b:
            out["balance_sol"], out["balance_at"] = float(b.group(1)), m.group(1)
            break
        if out["balance_note"] is None:
            out["balance_note"] = tail.strip(" |:")
    return out


def heartbeat_age(root, use_json_ts):
    hb = root / "data" / "heartbeat"
    if use_json_ts:
        d = jload(hb)
        if d and d.get("ts"):
            return time.time() - float(d["ts"])
    try:
        return time.time() - hb.stat().st_mtime
    except OSError:
        return None


def series(closed):
    pts, run = [], 0.0
    for c in sorted(closed, key=lambda c: (c["close_ts"] or 0, c["id"])):
        run += c["pnl_sol"] or 0.0
        pts.append({"id": c["id"], "t": c["close_ts"], "v": round(run, 6), "d": round(c["pnl_sol"] or 0.0, 6),
                    "s": c["symbol"] or "?"})
    return pts


def rlabel(reason, default="closed"):
    if not reason:
        return default
    for suf in ("_retry", "_failed"):
        if reason.endswith(suf) and reason[:-len(suf)] in REASONS:
            return f"{REASONS[reason[:-len(suf)]]} ({suf[1:]})"
    return REASONS.get(reason, reason)


def sell_row(reason, frac, mult, sol, ts):
    return {"reason": rlabel(reason, "sell"), "frac_pct": None if frac is None else round(frac * 100),
            "mult": None if mult is None else round(mult, 2), "sol": None if sol is None else round(sol, 6), "time": local(ts)}



MAIN_LABEL = "main paper bot (rules in config.json)"


def strip_no_replies(text):
    if not isinstance(text, str):
        return text
    t = re.sub(r"\s*\(?\bno[- ]repl(?:y|ies)(?: scor(?:e|ing))?\)?\s*,?", " ", text, flags=re.I)
    t = re.sub(r"\(\s*,\s*", "(", t); t = re.sub(r"\s*,\s*,", ",", t); t = re.sub(r"\+\s*\+", "+", t)
    t = re.sub(r"\s{2,}", " ", t); t = re.sub(r"\s+,", ",", t); t = re.sub(r",\s*\)", ")", t)
    return t.strip().strip(",").strip()


def paper_cfg(root=None):
    return jload((root or PAPER) / "config.json", {}) or {}


def paper_baseline(root=None):
    base = jload((root or PAPER) / "data" / "pnl_baseline.json")
    return base, (base or {}).get("ts") or 0


def paper_exits(db, ids):
    by = {}
    if ids:
        for e in q(db, f"SELECT * FROM exits WHERE paper_id IN ({','.join('?' * len(ids))}) ORDER BY ts, id", ids):
            by.setdefault(e["paper_id"], []).append(e)
    return by


def paper_closed_row(r, ex):
    cost = r["sol_in"] if r["sol_in"] not in (None, 0) else (r["size_sol"] or 0.15)
    p = r["pnl_sol"]
    sol_out = r["sol_out"] if r["sol_out"] is not None else r["proceeds_sol"]
    em = r["entry_mcap_usd"]
    return {"id": r["id"], "symbol": r["symbol"] or "?", "mint": r["mint"], "entry_mcap_usd": em, "pnl_sol": p,
            "pnl_pct": p / cost * 100 if (p is not None and cost) else None,
            "reason": rlabel(r["exit_reason_final"]),
            "time": local(r["close_ts"]), "day": local_day(r["close_ts"]), "opened": local(r["entry_ts"]),
            "sol_in": r["sol_in"] if r["sol_in"] is not None else r["size_sol"], "sol_out": sol_out,
            "peak_mult": r["peak_mult"], "score": r["score"], "score_v2": r.get("score_v2"),
            "sells": [dict(sell_row(e["reason"], e["frac_sold"], e["multiple"], e["proceeds_sol"], e["ts"]),
                           mcap_usd=(em * e["multiple"]) if (em is not None and e["multiple"] is not None) else None)
                      for e in ex.get(r["id"], [])]}


PAPER_CLOSED_SQL = "FROM paper WHERE status='closed' AND entry_ts>=?"


def paper_trades(offset=0, limit=50, db=None, root=None):
    root = root or PAPER
    """Every closed trade since the baseline (same filter as the summary), newest close first, paged."""
    db = db or root / "data" / "trench.db"
    _, since = paper_baseline(root)
    limit, offset = max(1, min(int(limit), 500)), max(0, int(offset))
    total = q(db, f"SELECT COUNT(*) n {PAPER_CLOSED_SQL}", (since,))[0]["n"]
    rows = q(db, f"SELECT * {PAPER_CLOSED_SQL} ORDER BY close_ts DESC, id DESC LIMIT ? OFFSET ?", (since, limit, offset))
    ex = paper_exits(db, [r["id"] for r in rows])
    out = [paper_closed_row(r, ex) for r in rows]

    pe = {}
    ids = [r["id"] for r in rows]
    if ids:
        try:
            pe = {x["paper_id"]: x for x in q(db, f"SELECT * FROM post_exit WHERE paper_id IN ({','.join('?' * len(ids))})", ids)}
        except Exception:
            pe = {}
    for o, r in zip(out, rows):
        x = pe.get(r["id"])
        hp = (x["held_peak_mult"] if x else None) or r["peak_mult"]
        hts = x["held_peak_ts"] if x else None
        pp_, pts = (x["post_peak_mult"], x["post_peak_ts"]) if x else (None, None)
        best, bts = (pp_, pts) if (pp_ is not None and (hp is None or pp_ > hp)) else (hp, hts)
        o.update(exit_mult=(x["exit_mult"] if x else None), held_peak_mult=hp, post_peak_mult=pp_,
                 overall_peak_mult=best, overall_peak_after_exit=bool(best is not None and best is pp_),
                 time_to_peak_min=((bts - r["entry_ts"]) / 60) if (bts and r["entry_ts"]) else None,
                 post_status=(x["status"] + (":" + x["done_reason"] if x["done_reason"] else "")) if x else None)
    if root in (PAPERE, PAPERA, PAPERG):
        for o, r in zip(out, rows):
            o["audited_pnl_sol"] = auditcost.audited_pnl(r["size_sol"], r["entry_mcap_sol"],
                                                         [(e["frac_sold"], e["multiple"]) for e in ex.get(r["id"], [])])
    return {"ok": True, "since_ts": since, "total": total, "offset": offset, "limit": limit, "trades": out}


def paper_snapshot(db=None, root=None):
    root = root or PAPER
    db = db or root / "data" / "trench.db"
    raw = paper_cfg(root)
    cfg = raw.get("paper", {})
    fee, entry_fee = float(cfg.get("fee_pct", 1.25)) / 100.0, float(cfg.get("priority_fee_sol", 0.0005)) + float(cfg.get("token_account_deposit_sol", 0.0))
    dep = float(cfg.get("token_account_deposit_sol", 0.0))
    base, since = paper_baseline(root)

    closed = q(db, f"SELECT id,symbol,close_ts,pnl_sol {PAPER_CLOSED_SQL} ORDER BY close_ts DESC", (since,))
    opens = q(db, "SELECT * FROM paper WHERE status!='closed' ORDER BY entry_ts DESC")
    ex = paper_exits(db, [r["id"] for r in opens])
    wins = sum(1 for r in closed if (r["pnl_sol"] or 0) > 0)
    net = sum(r["pnl_sol"] or 0 for r in closed)
    base_open = [r for r in opens if r["entry_ts"] >= since]
    unreal_hdr = sum((r["proceeds_sol"] or 0) + r["size_sol"] * (r["remaining_frac"] or 0) * (r["last_mult"] or 1.0)
                     - (r["size_sol"] + entry_fee) for r in base_open)
    open_out = []
    for r in opens:
        cost = r["sol_in"] if r["sol_in"] not in (None, 0) else (r["size_sol"] or 0) + entry_fee
        pnl = (r["proceeds_sol"] or 0.0) + (r["size_sol"] or 0) * (r["remaining_frac"] or 0.0) * (r["last_mult"] or 1.0) * (1 - fee) - cost
        em = r["entry_mcap_usd"]
        open_out.append({"symbol": r["symbol"] or "?", "mint": r["mint"], "entry_mcap_usd": em,
                         "mark_mcap_usd": (em * r["last_mult"]) if (em is not None and r["last_mult"] is not None) else None,
                         "mult": r["last_mult"] or 1.0, "held_pct": (r["remaining_frac"] or 0) * 100, "unrealized_sol": pnl,
                         "unrealized_pct": pnl / cost * 100 if cost else None, "opened": local(r["entry_ts"]),
                         "sol_in": r["sol_in"] if r["sol_in"] is not None else r["size_sol"],
                         "minutes": (time.time() - r["entry_ts"]) / 60,
                         "sells": [sell_row(e["reason"], e["frac_sold"], e["multiple"], e["proceeds_sol"], e["ts"])
                                   for e in ex.get(r["id"], [])]})
    hdr = log_header(root)
    audited = None
    if root in (PAPERE, PAPERA, PAPERG):
        cids = [r["id"] for r in closed]
        cr = {r["id"]: r for r in q(db, f"SELECT id,size_sol,entry_mcap_sol FROM paper WHERE id IN ({','.join('?' * len(cids))})", cids)} if cids else {}
        cex = paper_exits(db, cids)
        vals = [auditcost.audited_pnl(cr[i]["size_sol"], cr[i]["entry_mcap_sol"], [(e["frac_sold"], e["multiple"]) for e in cex.get(i, [])]) for i in cids if i in cr]
        vals = [v for v in vals if v is not None]
        aopen = sum((auditcost.audited_open(r["size_sol"], r["entry_mcap_sol"], [(e["frac_sold"], e["multiple"]) for e in ex.get(r["id"], [])],
                                            r["remaining_frac"] or 0, r["last_mult"] or 1.0) or 0) for r in base_open)
        aw = sum(1 for v in vals if v > 0)
        audited = {"net_sol": sum(vals), "n": len(vals), "wins": aw, "win_rate": 100.0 * aw / len(vals) if vals else None,
                   "per_trade_sol": sum(vals) / len(vals) if vals else None, "open_sol": aopen, "total_sol": sum(vals) + aopen,
                   "model": "pump.fun fee each side, curve price impact, priority fee per tx (dashboard/auditcost.py)"}
    return {"ok": True, "kind": "papera" if root == PAPERA else "paperg" if root == PAPERG else "paper" if root == PAPER else ("papere" if root == PAPERE else "paperb"), "generated_at": time.time(),
            "audited": audited,
            "baseline": {"ts": since, "local": (base or {}).get("local")},
            "rules": strip_no_replies(raw.get("bot_label") or MAIN_LABEL),
            "summary": {"closed_n": len(closed), "wins": wins, "losses": len(closed) - wins, "net_sol": net,
                        "win_rate": 100.0 * wins / len(closed) if closed else None,
                        "per_trade_sol": net / len(closed) if closed else None,
                        "open_n": len(base_open), "open_all_n": len(opens), "open_unrealized_sol": unreal_hdr,
                        "buys_n": len(closed) + len(base_open), "total_sol": net + unreal_hdr,
                        "reclaimable_sol": len(closed) * dep, "reclaimable_n": len(closed), "deposit_sol": dep},
            "status": {"heartbeat_age_s": heartbeat_age(root, False), **hdr},
            "today": local_day(time.time()), "series_key": f"{root.name}:{since}", "series": series(closed),
            "open": open_out, "closed": []}



def live_config():
    c = jload(LIVE / "config.json", {}) or {}
    ex, pp = c.get("execution", {}), c.get("paper", {})
    return {"enabled": bool(ex.get("enabled")), "size_sol": pp.get("size_sol"), "daily_loss_cap_sol": pp.get("daily_loss_cap_sol"),
            "min_reserve_sol": ex.get("min_reserve_sol", 0.01), "fee_pct": (ex.get("assumed_fee_bps") or 125) / 100.0}


def live_snapshot(db=None, live_db=None):
    db, live_db = db or LIVE / "data" / "trench.db", live_db or LIVE / "data" / "live.db"
    cfg = live_config()
    fee = cfg["fee_pct"] / 100.0
    lpos, sells, n_orders, n_failed = {}, {}, 0, 0
    if live_db.exists():
        lpos = {r["paper_id"]: r for r in q(live_db, "SELECT * FROM live_positions")}
        r = q(live_db, "SELECT COUNT(*) n, SUM(status!='filled') f FROM live_orders")[0]
        n_orders, n_failed = r["n"] or 0, r["f"] or 0
        raw = q(live_db, "SELECT sig, ts, paper_id, reason, lamports_delta, tokens_delta FROM live_orders "
                         "WHERE side='sell' AND status='filled' AND paper_id IS NOT NULL")
        raw += q(live_db, "SELECT sig, COALESCE(block_time, ts) ts, paper_id, reason, lamports_delta, tokens_delta "
                          "FROM live_hand_sells WHERE paper_id IS NOT NULL")
        seen = set()
        for s in sorted(raw, key=lambda s: s["ts"] or 0):
            if s["sig"] in seen:
                continue
            seen.add(s["sig"])
            sells.setdefault(s["paper_id"], []).append(s)
    rows = q(db, "SELECT id,mint,symbol,entry_ts,entry_mcap_usd,size_sol,remaining_frac,pending_frac,last_mult,status,close_ts,"
                 "proceeds_sol,pnl_sol,sol_in,exit_reason_final FROM paper ORDER BY entry_ts DESC")
    exits = {}
    for e in q(db, "SELECT paper_id, reason, multiple FROM exits ORDER BY ts, id"):
        exits.setdefault(e["paper_id"], []).append(e)

    def sell_list(pid):
        lp, out, used = lpos.get(pid) or {}, [], set()
        bought = lp.get("tokens_bought") or 0
        for s in sells.get(pid, []):
            mult = None
            for i, e in enumerate(exits.get(pid, [])):
                if i not in used and e["reason"] == s["reason"]:
                    used.add(i); mult = e["multiple"]; break
            frac = (-(s["tokens_delta"] or 0) / bought) if bought else None
            out.append(sell_row(s["reason"], frac, mult, (s["lamports_delta"] or 0) / 1e9, s["ts"]))
        return out

    opens, closed, now = [], [], time.time()
    for r in rows:
        lp = lpos.get(r["id"]) or {}
        sol_in = lp.get("sol_in") or r["sol_in"] or r["size_sol"]
        if r["status"] == "closed":
            p = r["pnl_sol"] or 0.0
            closed.append({"id": r["id"], "symbol": r["symbol"] or "?", "mint": r["mint"], "pnl_sol": p,
                           "pnl_pct": p / sol_in * 100 if sol_in else None, "close_ts": r["close_ts"],
                           "reason": REASONS.get(r["exit_reason_final"], r["exit_reason_final"] or "closed"),
                           "time": local(r["close_ts"]), "day": local_day(r["close_ts"])})
        else:
            held = (r["remaining_frac"] or 0) + (r["pending_frac"] or 0)
            mult = r["last_mult"] or 1.0
            sold = lp.get("sol_out") if lp else r["proceeds_sol"]
            unreal = (sold or 0.0) + r["size_sol"] * held * mult * (1 - fee) - sol_in
            opens.append({"symbol": r["symbol"] or "?", "mint": r["mint"], "entry_mcap_usd": r["entry_mcap_usd"], "mult": mult,
                          "mark_value_sol": r["size_sol"] * held * mult * (1 - fee),
                          "held_pct": held * 100, "unrealized_sol": unreal, "unrealized_pct": unreal / sol_in * 100 if sol_in else None,
                          "sell_retrying": (r["pending_frac"] or 0) > 1e-9, "opened": local(r["entry_ts"]),
                          "minutes": (now - r["entry_ts"]) / 60, "sells": sell_list(r["id"])})
    closed.sort(key=lambda x: x["close_ts"] or 0, reverse=True)
    wins = sum(1 for c in closed if c["pnl_sol"] > 0)
    net = sum(c["pnl_sol"] for c in closed)
    unreal = sum(o["unrealized_sol"] for o in opens)
    hdr = log_header(LIVE)
    need = (cfg["size_sol"] or 0) + (cfg["min_reserve_sol"] or 0)
    bal = hdr["balance_sol"]
    notes = []
    if not cfg["enabled"]:
        notes.append("Live switch is off: nothing is sent.")
    if bal is not None and bal < need:
        notes.append(f"Waiting for SOL: balance {bal:.4f}, needs about {need:.2f} to buy." if n_orders == 0 else
                     f"Balance {bal:.4f} SOL is below {need:.2f}: no new buys until sells return SOL or a top-up. Sells and stops still run.")
    grad = [{"symbol": c["symbol"], "mint": c["mint"]} for c in closed if c["reason"].startswith("graduated (")]
    recent = closed[:LIVE_RECENT]
    for c in recent:
        c["sells"] = sell_list(c["id"])
    try:
        wp = wallet_pnl(opens, fee)
    except Exception as e:
        wp = {"error": f"{type(e).__name__}: {e}"}
    return {"ok": True, "kind": "live", "generated_at": now, "wallet": WALLET, "config": cfg, "wallet_pnl": wp,
            "summary": {"closed_n": len(closed), "wins": wins, "losses": len(closed) - wins, "net_sol": net,
                        "win_rate": 100.0 * wins / len(closed) if closed else None,
                        "per_trade_sol": net / len(closed) if closed else None,
                        "open_n": len(opens), "open_unrealized_sol": unreal, "total_sol": net + unreal},
            "status": {"heartbeat_age_s": heartbeat_age(LIVE, True), **hdr}, "orders_sent": n_orders, "orders_failed": n_failed,
            "notes": notes, "graduated_unsold": grad,
            "today": local_day(now), "series_key": "live", "series": series(closed), "open": opens, "closed": recent}



import threading
try:
    import wallet as W
except ImportError:
    W = None
WALLET_REFRESH_S = 300


def wallet_refresher():
    while W is not None:
        try:
            W.refresh(log=lambda *a: None)
        except Exception as e:
            print(f"{time.strftime('%F %T')} wallet refresh failed: {type(e).__name__}: {e}", flush=True)
        time.sleep(WALLET_REFRESH_S)


def wallet_pnl(opens, fee):
    if W is None:
        return None
    c = W.load_cache()
    h = c.get("holdings")
    px = jload(LIVE / "data" / "sol_usd.json") or {}
    sol_usd = px.get("usd")
    if not h or not sol_usd:
        return None
    flows = W.flows(c)
    dep = [f for f in flows if f["dir"] == "in"]
    wd = [f for f in flows if f["dir"] == "out"]
    bal = h["balance_lamports"] / 1e9
    marks = {o["mint"]: o for o in opens}
    tok_usd, tok_sol_rent, tok_rows, unpriced = 0.0, 0.0, [], 0
    reclaim_sol, reclaim_n = 0.0, 0
    for t in h["tokens"]:
        tok_sol_rent += t["rent_lamports"] / 1e9
        if not t["amount"]:
            reclaim_sol += t["rent_lamports"] / 1e9; reclaim_n += 1
        if t["mint"] in marks:
            v = marks[t["mint"]]["mark_value_sol"] * sol_usd
        elif t["value_usd"] is None:
            unpriced += 1; v = 0.0
        else:
            v = t["value_usd"]
        tok_usd += v
        tok_rows.append({"mint": t["mint"], "amount": t["amount"], "value_usd": v})
    held = {t["mint"] for t in h["tokens"]}
    open_mark_sol = sum(o["mark_value_sol"] for o in opens if o["mint"] not in held)

    cur_usd = (bal + open_mark_sol) * sol_usd + tok_usd
    dep_usd = sum(f["usd"] or 0 for f in dep); wd_usd = sum(f["usd"] or 0 for f in wd)
    dep_sol = sum(f["sol"] for f in dep); wd_sol = sum(f["sol"] for f in wd)
    cur_sol = cur_usd / sol_usd
    return {"pnl_usd": cur_usd - dep_usd + wd_usd, "pnl_sol": cur_sol - dep_sol + wd_sol,
            "current_usd": cur_usd, "current_sol": cur_sol, "balance_sol": bal, "token_rent_sol": tok_sol_rent, "reclaimable_sol": reclaim_sol, "reclaimable_n": reclaim_n,
            "tokens_usd": tok_usd, "tokens": tok_rows, "tokens_unpriced": unpriced, "open_mark_sol": open_mark_sol,
            "deposits_usd": dep_usd, "deposits_sol": dep_sol, "deposits_usd_now": dep_sol * sol_usd,
            "withdrawals_usd": wd_usd, "withdrawals_sol": wd_sol, "withdrawals_usd_now": wd_sol * sol_usd,
            "flows": flows, "sol_usd": sol_usd, "sol_usd_at": px.get("fetched_local"),
            "sol_usd_sources": ", ".join(k.title() for k in (px.get("sources") or {})),
            "chain_at": local(h["ts"]), "tx_count": len(c.get("order") or [])}



class Handler(BaseHTTPRequestHandler):
    server_version = "td"
    sys_version = ""

    def log_message(self, fmt, *args):
        if args and str(args[1]).startswith(("2", "4")):
            return
        super().log_message(fmt, *args)

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Robots-Tag", "noindex")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        path = urlparse(self.path).path
        pre = f"/{TOKEN}"
        if not TOKEN or not (path == pre or path.startswith(pre + "/")) or not secrets.compare_digest(path[:len(pre)], pre):
            return self._send(404, b"not found\n", "text/plain")
        sub = path[len(pre):] or "/"
        if sub == "":
            sub = "/"
        if sub in ("/", "/index.html"):
            return self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
        if sub in ("/api/papere/trades", "/api/papera/trades", "/api/paperg/trades"):
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            try:
                data = paper_trades(int(qs.get("offset", ["0"])[0]), int(qs.get("limit", ["50"])[0]), root=PAPERA if "papera/" in sub else PAPERG if "paperg/" in sub else PAPERB if "paperb" in sub else PAPERE if "papere" in sub else PAPER)
            except Exception as e:
                data = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            return self._send(200, json.dumps(data, default=str).encode(), "application/json")
        if sub in ("/api/papere", "/api/papera", "/api/paperg", "/api/live"):
            try:
                data = paper_snapshot() if sub == "/api/paper" else paper_snapshot(root=PAPERB) if sub == "/api/paperb" else paper_snapshot(root=PAPERE) if sub == "/api/papere" else paper_snapshot(root=PAPERA) if sub == "/api/papera" else paper_snapshot(root=PAPERG) if sub == "/api/paperg" else live_snapshot()
                try:
                    import sys as _s; _s.path.insert(0, str(HERE.parent)); import downtime as _dt, compare as _cmp
                    data["downtime_today"] = {b: _dt.today_total(b) for b in ("papere", "papera", "paperg")}
                    if sub == "/api/live":
                        data["compare"] = _cmp.compare()["summary"]
                except Exception as _e:
                    data["downtime_err"] = str(_e)
            except Exception as e:
                data = {"ok": False, "error": f"{type(e).__name__}: {e}", "generated_at": time.time()}
            return self._send(200, json.dumps(data, default=str).encode(), "application/json")
        return self._send(404, b"not found\n", "text/plain")


def main():
    threading.Thread(target=wallet_refresher, daemon=True).start()
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"{time.strftime('%F %T %Z')} trench-dashboard on http://{HOST}:{PORT}/<token>/", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
