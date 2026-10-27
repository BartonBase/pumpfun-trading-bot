import hashlib
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")
CONFIG_PATH = os.path.join(ROOT, "config.json")
HISTORY_DIR = os.path.join(ROOT, "config", "history")
DB_PATH = os.path.join(DATA_DIR, "trench.db")




def _strip_comments(obj):
    if isinstance(obj, dict):
        return {k: _strip_comments(v) for k, v in obj.items() if not k.startswith("_")}
    return obj


def load_raw_config(path=CONFIG_PATH):
    with open(path) as f:
        return json.load(f)


def load_config(path=CONFIG_PATH):
    return _strip_comments(load_raw_config(path))


def _file_hash(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def snapshot_config_if_changed(path=CONFIG_PATH, history_dir=HISTORY_DIR):
    os.makedirs(history_dir, exist_ok=True)
    snaps = sorted(f for f in os.listdir(history_dir) if f.endswith(".json"))
    cur = _file_hash(path)
    if snaps and _file_hash(os.path.join(history_dir, snaps[-1])) == cur:
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = os.path.join(history_dir, f"config-{stamp}.json")
    n = 1
    while os.path.exists(dest):
        dest = os.path.join(history_dir, f"config-{stamp}-{n}.json")
        n += 1
    shutil.copy2(path, dest)
    return dest


def set_config_value(raw, dotted_key, value):
    parts = dotted_key.split(".")
    node = raw
    for p in parts[:-1]:
        node = node[p]
    if parts[-1] not in node:
        raise KeyError(dotted_key)
    node[parts[-1]] = value


def write_config(raw, path=CONFIG_PATH):
    snapshot_config_if_changed(path)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(raw, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    return snapshot_config_if_changed(path)




SCHEMA = """
CREATE TABLE IF NOT EXISTS coins (
  mint TEXT PRIMARY KEY,
  symbol TEXT, name TEXT, creator TEXT, bonding_curve TEXT,
  created_ts REAL,            -- unix seconds
  first_seen_ts REAL, last_seen_ts REAL,
  seen_in_new_feed INTEGER DEFAULT 0,
  quote_mint TEXT, mayhem_state TEXT, nsfw INTEGER,
  has_socials INTEGER, reply_count INTEGER,
  complete INTEGER DEFAULT 0, complete_seen_ts REAL,
  mcap_sol REAL, mcap_usd REAL, curve_pct REAL, real_sol REAL,
  ath_mcap_usd REAL, ath_ts REAL, last_trade_ts REAL,
  price_source TEXT,
  prev_mcap_sol REAL, prev_seen_ts REAL,
  band_ts REAL, band_mcap_usd REAL, band_mcap_sol REAL, band_curve_pct REAL,
  band_reply_count INTEGER, band_age_min REAL,
  peak_mcap_sol_after_band REAL, peak_ts_after_band REAL
);
CREATE INDEX IF NOT EXISTS coins_created ON coins(created_ts);
CREATE INDEX IF NOT EXISTS coins_symbol ON coins(symbol);
CREATE INDEX IF NOT EXISTS coins_band ON coins(band_ts);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL, mint TEXT, symbol TEXT,
  score REAL, min_score REAL, label TEXT,
  breakdown TEXT,             -- JSON list of [reason, points]
  regime_verdict TEXT, action TEXT,
  mcap_usd REAL, curve_pct REAL, age_min REAL, reply_count INTEGER
);
CREATE INDEX IF NOT EXISTS signals_mint ON signals(mint, ts);

CREATE TABLE IF NOT EXISTS paper (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mint TEXT, symbol TEXT, signal_id INTEGER,
  entry_ts REAL, entry_mcap_sol REAL, entry_mcap_usd REAL, size_sol REAL,
  remaining_frac REAL, peak_mult REAL, tp1_done INTEGER, tp2_done INTEGER,
  status TEXT, close_ts REAL, proceeds_sol REAL, pnl_sol REAL,
  last_mult REAL, last_price_ts REAL,
  score REAL, score_reasons TEXT, label TEXT,
  regime_verdict_entry TEXT, exit_reason_final TEXT
);

CREATE TABLE IF NOT EXISTS exits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  paper_id INTEGER, ts REAL, reason TEXT,
  frac_sold REAL, multiple REAL, proceeds_sol REAL,
  minutes_in REAL, regime_verdict TEXT, score_reasons TEXT
);

CREATE TABLE IF NOT EXISTS regime_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL, verdict TEXT, verdict_24h TEXT, report TEXT
);

CREATE TABLE IF NOT EXISTS tune_trials (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT, ts REAL, kind TEXT,          -- kind: grid (one config on all trades) | summary
  params TEXT, n_trades INTEGER, pnl_sol REAL, expectancy_sol REAL, expectancy_r REAL,
  sharpe_per_trade REAL, selected INTEGER DEFAULT 0, note TEXT
);

CREATE TABLE IF NOT EXISTS paper_ticks (
  paper_id INTEGER, ts REAL, mult REAL, source TEXT
);
CREATE INDEX IF NOT EXISTS paper_ticks_pid ON paper_ticks(paper_id, ts);

CREATE TABLE IF NOT EXISTS feed_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL, source TEXT, endpoint TEXT, status TEXT, n INTEGER
);
CREATE INDEX IF NOT EXISTS feed_log_ts ON feed_log(ts);
"""



MIGRATIONS = {
    "signals": [("features", "TEXT"), ("real_sol", "REAL"), ("mayhem", "INTEGER"), ("ath_retained", "REAL"),
                ("dup_ticker", "INTEGER"), ("unique_buyers", "INTEGER"), ("trade_count", "INTEGER"),
                ("same_block_supply_share", "REAL"), ("multi_wallet_sig_supply_share", "REAL"),
                ("creator_funded_share", "REAL"), ("top10_holder_share", "REAL"),
                ("qualified_wallets", "INTEGER"), ("creator_prior_grad_rate", "REAL"), ("creator_prior_coins", "INTEGER"),
                ("filter_tags", "TEXT"),
                ("score_v2", "REAL"), ("score_v2_reasons", "TEXT")],
    "paper": [("sol_in", "REAL"), ("sol_out", "REAL"), ("fees_sol", "REAL"), ("slippage_sol", "REAL"),
              ("time_to_2x_min", "REAL"), ("time_to_stop_min", "REAL"), ("scale1_sold", "INTEGER"),
              ("pending_frac", "REAL DEFAULT 0"), ("pending_reason", "TEXT"), ("failed_sells", "INTEGER DEFAULT 0"),
              ("mcap_1h_sol", "REAL"), ("mcap_6h_sol", "REAL"), ("mult_1h", "REAL"), ("mult_6h", "REAL"),
              ("later_graduated", "INTEGER"), ("mark_value_sol", "REAL"),
              ("filter_tags", "TEXT"),
              ("entry_path", "TEXT"), ("first_seen_mcap_usd", "REAL"),
              ("score_v2", "REAL"), ("score_v2_reasons", "TEXT")],
}


class Store:
    def __init__(self, path=DB_PATH, autocommit=False):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path

        self.db = sqlite3.connect(path, timeout=60, isolation_level=None if autocommit else "")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        for table, cols in MIGRATIONS.items():
            have = {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}
            for name, typ in cols:
                if name not in have:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
        self.db.commit()


    def q(self, sql, args=()):
        return self.db.execute(sql, args).fetchall()

    def one(self, sql, args=()):
        return self.db.execute(sql, args).fetchone()

    def commit(self):
        self.db.commit()


    def log_feed(self, source, endpoint, status, n=0):
        self.db.execute("INSERT INTO feed_log(ts,source,endpoint,status,n) VALUES(?,?,?,?,?)",
                        (time.time(), source, endpoint, str(status), n))
        self.db.commit()


    def get_coin(self, mint):
        return self.one("SELECT * FROM coins WHERE mint=?", (mint,))

    def upsert_coin(self, c, now, from_new_feed=False, source="feed"):
        old = self.get_coin(c["mint"])
        if old is None:
            self.db.execute(
                """INSERT INTO coins(mint,symbol,name,creator,bonding_curve,created_ts,first_seen_ts,
                last_seen_ts,seen_in_new_feed,quote_mint,mayhem_state,nsfw,has_socials,reply_count,complete,
                complete_seen_ts,mcap_sol,mcap_usd,curve_pct,real_sol,ath_mcap_usd,ath_ts,last_trade_ts,price_source)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (c["mint"], c["symbol"], c["name"], c["creator"], c["bonding_curve"], c["created_ts"], now, now,
                 1 if from_new_feed else 0, c["quote_mint"], c["mayhem_state"], c["nsfw"], c["has_socials"],
                 c["reply_count"], c["complete"], now if c["complete"] else None, c["mcap_sol"], c["mcap_usd"],
                 c["curve_pct"], c["real_sol"], c["ath_mcap_usd"], c["ath_ts"], c["last_trade_ts"], source))
            return None
        self.db.execute(
            """UPDATE coins SET symbol=?, name=?, last_seen_ts=?, seen_in_new_feed=MAX(seen_in_new_feed,?),
            mayhem_state=?, reply_count=?, has_socials=?, complete=MAX(complete,?),
            complete_seen_ts=COALESCE(complete_seen_ts, CASE WHEN ?=1 THEN ? END),
            prev_mcap_sol=mcap_sol, prev_seen_ts=last_seen_ts,
            mcap_sol=?, mcap_usd=?, curve_pct=?, real_sol=?,
            ath_mcap_usd=MAX(COALESCE(ath_mcap_usd,0),COALESCE(?,0)), ath_ts=COALESCE(?,ath_ts),
            last_trade_ts=MAX(COALESCE(last_trade_ts,0),COALESCE(?,0)), price_source=? WHERE mint=?""",
            (c["symbol"], c["name"], now, 1 if from_new_feed else 0, c["mayhem_state"], c["reply_count"],
             c["has_socials"], c["complete"], c["complete"], now, c["mcap_sol"], c["mcap_usd"], c["curve_pct"],
             c["real_sol"], c["ath_mcap_usd"], c["ath_ts"], c["last_trade_ts"], source, c["mint"]))
        return old

    def update_from_rpc(self, mint, state, sol_usd, now):
        mcap_usd = state["mcap_sol"] * sol_usd if sol_usd else None
        self.db.execute(
            """UPDATE coins SET prev_mcap_sol=mcap_sol, prev_seen_ts=last_seen_ts, last_seen_ts=?,
            mcap_sol=?, mcap_usd=COALESCE(?,mcap_usd), curve_pct=?, real_sol=?, complete=MAX(complete,?),
            ath_ts=CASE WHEN COALESCE(?,0) > COALESCE(ath_mcap_usd,0) THEN ? ELSE ath_ts END,
            ath_mcap_usd=MAX(COALESCE(ath_mcap_usd,0), COALESCE(?,0)),
            complete_seen_ts=COALESCE(complete_seen_ts, CASE WHEN ?=1 THEN ? END), price_source='rpc'
            WHERE mint=?""",
            (now, state["mcap_sol"], mcap_usd, state["curve_pct"], state["real_sol"], int(state["complete"]),
             mcap_usd, now, mcap_usd, int(state["complete"]), now, mint))
        self.bump_peak(mint, state["mcap_sol"], now)

    def bump_peak(self, mint, mcap_sol, now):
        self.db.execute(
            """UPDATE coins SET peak_mcap_sol_after_band=?, peak_ts_after_band=?
            WHERE mint=? AND band_ts IS NOT NULL AND (peak_mcap_sol_after_band IS NULL OR peak_mcap_sol_after_band < ?)""",
            (mcap_sol, now, mint, mcap_sol))

    def mark_band(self, mint, now, mcap_usd, mcap_sol, curve_pct, replies, age_min):
        self.db.execute(
            """UPDATE coins SET band_ts=?, band_mcap_usd=?, band_mcap_sol=?, band_curve_pct=?, band_reply_count=?,
            band_age_min=?, peak_mcap_sol_after_band=?, peak_ts_after_band=? WHERE mint=? AND band_ts IS NULL""",
            (now, mcap_usd, mcap_sol, curve_pct, replies, age_min, mcap_sol, now, mint))

    def dup_ticker_exists(self, mint, symbol, created_ts, window_s):
        r = self.one("""SELECT COUNT(*) n FROM coins WHERE mint<>? AND lower(symbol)=lower(?)
                        AND created_ts < ? AND created_ts >= ?""",
                     (mint, symbol or "", created_ts, created_ts - window_s))
        return r["n"] > 0


    SIGNAL_FEATURES = ("real_sol", "mayhem", "ath_retained", "dup_ticker", "unique_buyers", "trade_count",
                       "same_block_supply_share", "multi_wallet_sig_supply_share", "creator_funded_share",
                       "top10_holder_share", "qualified_wallets", "creator_prior_grad_rate", "creator_prior_coins")

    def add_signal(self, s):
        f = s.get("features") or {}
        cols = ["ts", "mint", "symbol", "score", "min_score", "label", "breakdown", "regime_verdict", "action",
                "mcap_usd", "curve_pct", "age_min", "reply_count", "features", "filter_tags", "score_v2", "score_v2_reasons"] \
            + list(self.SIGNAL_FEATURES)
        v2r = s.get("score_v2_reasons")
        vals = [s["ts"], s["mint"], s["symbol"], s["score"], s["min_score"], s["label"], json.dumps(s["breakdown"]),
                s["regime_verdict"], s["action"], s["mcap_usd"], s["curve_pct"], s["age_min"], s["reply_count"],
                json.dumps(f), s.get("filter_tags"), s.get("score_v2"), json.dumps(v2r) if v2r is not None else None] \
            + [f.get(k) for k in self.SIGNAL_FEATURES]
        cur = self.db.execute(f"INSERT INTO signals({','.join(cols)}) VALUES({','.join('?' * len(cols))})", vals)
        return cur.lastrowid

    def creator_prior(self, creator, before_ts):
        if not creator:
            return 0, None
        r = self.one("""SELECT COUNT(*) n, SUM(complete) g FROM coins WHERE creator=? AND created_ts < ?""",
                     (creator, before_ts or 0))
        n = r["n"] or 0
        return n, ((r["g"] or 0) / n if n else None)

    def add_trial(self, t):
        self.db.execute("""INSERT INTO tune_trials(run_id,ts,kind,params,n_trades,pnl_sol,expectancy_sol,expectancy_r,
                           sharpe_per_trade,selected,note) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (t["run_id"], time.time(), t["kind"], json.dumps(t.get("params")), t.get("n_trades"),
                         t.get("pnl_sol"), t.get("expectancy_sol"), t.get("expectancy_r"), t.get("sharpe"),
                         int(t.get("selected", 0)), t.get("note")))

    def last_signal_ts(self, mint):
        r = self.one("SELECT MAX(ts) t FROM signals WHERE mint=?", (mint,))
        return r["t"] if r else None

    def open_paper(self):
        return self.q("SELECT * FROM paper WHERE status='open' ORDER BY entry_ts")

    def ever_traded(self, mint):
        return self.one("SELECT 1 FROM paper WHERE mint=?", (mint,)) is not None

    def add_paper(self, p):
        cur = self.db.execute(
            """INSERT INTO paper(mint,symbol,signal_id,entry_ts,entry_mcap_sol,entry_mcap_usd,size_sol,remaining_frac,
            peak_mult,tp1_done,tp2_done,status,proceeds_sol,last_mult,last_price_ts,score,score_reasons,label,
            regime_verdict_entry,filter_tags,entry_path,first_seen_mcap_usd,score_v2,score_v2_reasons)
            VALUES(?,?,?,?,?,?,?,1.0,1.0,0,0,'open',0,1.0,?,?,?,?,?,?,?,?,?,?)""",
            (p["mint"], p["symbol"], p["signal_id"], p["entry_ts"], p["entry_mcap_sol"], p["entry_mcap_usd"],
             p["size_sol"], p["entry_ts"], p["score"], json.dumps(p["score_reasons"]), p["label"],
             p["regime_verdict"], p.get("filter_tags"), p.get("entry_path"), p.get("first_seen_mcap_usd"),
             p.get("score_v2"), json.dumps(p["score_v2_reasons"]) if p.get("score_v2_reasons") is not None else None))
        return cur.lastrowid

    def update_paper(self, pid, **kw):
        cols = ", ".join(f"{k}=?" for k in kw)
        self.db.execute(f"UPDATE paper SET {cols} WHERE id=?", (*kw.values(), pid))

    def add_tick(self, paper_id, ts, mult, source):
        self.db.execute("INSERT INTO paper_ticks(paper_id,ts,mult,source) VALUES(?,?,?,?)", (paper_id, ts, mult, source))

    def ticks(self, paper_id):
        return [(r["ts"], r["mult"]) for r in self.q("SELECT ts, mult FROM paper_ticks WHERE paper_id=? ORDER BY ts", (paper_id,))]

    def add_exit(self, e):
        self.db.execute(
            """INSERT INTO exits(paper_id,ts,reason,frac_sold,multiple,proceeds_sol,minutes_in,regime_verdict,score_reasons)
            VALUES(?,?,?,?,?,?,?,?,?)""",
            (e["paper_id"], e["ts"], e["reason"], e["frac_sold"], e["multiple"], e["proceeds_sol"],
             e["minutes_in"], e["regime_verdict"], e["score_reasons"]))

    def closed_pnl_since(self, ts):
        r = self.one("SELECT COALESCE(SUM(pnl_sol),0) s FROM paper WHERE status='closed' AND close_ts>=?", (ts,))
        return r["s"]


    def add_regime(self, verdict, verdict_24h, report):
        self.db.execute("INSERT INTO regime_snapshots(ts,verdict,verdict_24h,report) VALUES(?,?,?,?)",
                        (time.time(), verdict, verdict_24h, json.dumps(report)))

    def last_regimes(self, n):
        return self.q("SELECT * FROM regime_snapshots ORDER BY id DESC LIMIT ?", (n,))


BASELINE_PATH = os.path.join(ROOT, "data", "pnl_baseline.json")


def load_baseline():
    try:
        with open(BASELINE_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def set_baseline(note):
    import time as _t
    ts = int(_t.time())
    b = {"ts": ts, "local": _t.strftime("%Y-%m-%d %H:%M:%S %Z", _t.localtime(ts)), "note": note}
    with open(BASELINE_PATH, "w") as f:
        json.dump(b, f, indent=2)
    return b


FILTER_TAGS = ("spike", "vertical", "serial_creator", "too_few_holders", "holder_count_missing", "dup_ticker",
               "top10_below_min", "holder_data_missing", "bundled")


def tag_breakdown(con, since_ts, entry_fee_sol=0.0005):
    have = {r[1] for r in con.execute("PRAGMA table_info(paper)")}
    col = "filter_tags" if "filter_tags" in have else "NULL"
    rows = con.execute(f"SELECT status, pnl_sol, proceeds_sol, size_sol, remaining_frac, last_mult, {col} "
                       "FROM paper WHERE entry_ts>=?", (since_ts,)).fetchall()
    out = {}
    for st_, pnl, proc, size, rem, lm, tg in rows:
        keys = [t for t in (tg or "").split(",") if t] or (["none"] if tg is not None else ["untagged"])
        for k in keys:
            d = out.setdefault(k, {"tag": k, "n": 0, "closed_n": 0, "wins": 0, "losses": 0, "closed_net_sol": 0.0,
                                   "open_n": 0, "open_unrealized_sol": 0.0})
            d["n"] += 1
            if st_ == "closed":
                d["closed_n"] += 1
                d["closed_net_sol"] += pnl or 0
                d["wins" if (pnl or 0) > 0 else "losses"] += 1
            else:
                d["open_n"] += 1
                d["open_unrealized_sol"] += (proc or 0) + size * (rem or 0) * (lm or 1.0) - (size + entry_fee_sol)
    order = list(FILTER_TAGS) + ["none", "untagged"]
    return sorted(out.values(), key=lambda d: order.index(d["tag"]) if d["tag"] in order else 99)


def baseline_summary(con, since_ts, entry_fee_sol=0.0005):
    con.row_factory = sqlite3.Row
    closed = list(con.execute("SELECT pnl_sol FROM paper WHERE status='closed' AND entry_ts>=?", (since_ts,)))
    opens = list(con.execute("SELECT * FROM paper WHERE status!='closed' AND entry_ts>=?", (since_ts,)))
    net = sum((r["pnl_sol"] or 0) for r in closed)
    wins = sum(1 for r in closed if (r["pnl_sol"] or 0) > 0)
    unreal = sum((r["proceeds_sol"] or 0) + r["size_sol"] * (r["remaining_frac"] or 0) * (r["last_mult"] or 1.0)
                 - (r["size_sol"] + entry_fee_sol) for r in opens)
    return {"since_ts": since_ts, "closed_n": len(closed), "wins": wins, "losses": len(closed) - wins,
            "closed_net_sol": net, "open_n": len(opens), "open_unrealized_sol": unreal}
