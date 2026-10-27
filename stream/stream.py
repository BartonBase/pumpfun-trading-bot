
import asyncio, json, os, sqlite3, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("TRENCH_STREAM_DB", os.path.join(HERE, "stream.db"))
URL = "wss://stream.pumpapi.io/"
SILENCE_S = 60
FLUSH_S = 0.25
PRUNE_AGE_S = 3 * 3600
CROSS_SOL = float(os.environ.get("TRENCH_STREAM_CROSS_SOL", "60"))


def open_db(path=DB):
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("""CREATE TABLE IF NOT EXISTS coins(mint TEXT PRIMARY KEY, symbol TEXT, name TEXT, creator TEXT,
        bonding_curve TEXT, created_ms INTEGER, mayhem INTEGER, first_ms INTEGER, last_ms INTEGER, recv_ms INTEGER,
        mcap_sol REAL, tokens_in_pool REAL, n_trades INTEGER DEFAULT 0, cross_ms INTEGER)""")
    db.execute("CREATE INDEX IF NOT EXISTS coins_recv ON coins(recv_ms)")
    db.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
    return db


def apply(db, evs):
    db.execute("BEGIN")
    for m, recv in evs:
        mint = m.get("mint")
        if not mint or m.get("pool") != "pump":
            continue
        act = m.get("action")
        ts = int(m.get("timestamp") or recv)
        mc = m.get("marketCapQuote")
        tip = m.get("tokensInPool")
        if act == "create":
            db.execute("""INSERT INTO coins(mint,symbol,name,creator,bonding_curve,created_ms,mayhem,first_ms,last_ms,recv_ms,
                mcap_sol,tokens_in_pool,n_trades) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0)
                ON CONFLICT(mint) DO UPDATE SET symbol=excluded.symbol, name=excluded.name, creator=excluded.creator,
                bonding_curve=excluded.bonding_curve, created_ms=excluded.created_ms, mayhem=excluded.mayhem""",
                       (mint, m.get("symbol"), m.get("name"), m.get("txSigner"), m.get("poolId"), ts,
                        1 if m.get("mayhemMode") else 0, ts, ts, recv, mc, tip))
        elif act in ("buy", "sell"):
            cross = ts if (mc or 0) >= CROSS_SOL else None
            db.execute("""INSERT INTO coins(mint,symbol,name,bonding_curve,mayhem,first_ms,last_ms,recv_ms,mcap_sol,tokens_in_pool,
                n_trades,cross_ms) VALUES(?,?,?,?,?,?,?,?,?,?,1,?)
                ON CONFLICT(mint) DO UPDATE SET last_ms=MAX(last_ms,excluded.last_ms), recv_ms=excluded.recv_ms,
                mcap_sol=excluded.mcap_sol, tokens_in_pool=excluded.tokens_in_pool, n_trades=n_trades+1,
                bonding_curve=COALESCE(bonding_curve,excluded.bonding_curve),
                cross_ms=COALESCE(cross_ms,excluded.cross_ms)""",
                       (mint, m.get("symbol"), m.get("name"), m.get("poolId"), 1 if m.get("mayhemMode") else 0,
                        ts, ts, recv, mc, tip, cross))
    db.execute("COMMIT")


def meta(db, **kv):
    for k, v in kv.items():
        db.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, str(v)))


async def run():
    import ssl
    import certifi
    import websockets


    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    db = open_db()
    back, total, last_prune = 1, 0, 0
    while True:
        try:
            async with websockets.connect(URL, max_size=None, ping_interval=20, ping_timeout=30, open_timeout=20, ssl=ssl_ctx) as ws:
                print(time.strftime("%F %T"), "connected", flush=True)
                meta(db, connected=int(time.time()))
                back = 1
                buf, t_flush = [], time.time()
                last_msg = time.time()
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=FLUSH_S)
                        recv = int(time.time() * 1000)
                        last_msg = time.time()
                        if isinstance(raw, (bytes, bytearray)):
                            raw = raw.decode("utf-8", "replace")
                        if '"pool": "pump"' in raw or '"pool":"pump"' in raw:
                            m = json.loads(raw)
                            if m.get("pool") == "pump":
                                buf.append((m, recv))
                    except asyncio.TimeoutError:
                        pass
                    now = time.time()
                    if now - last_msg > SILENCE_S:
                        raise RuntimeError(f"no data for {int(now - last_msg)}s (silent socket)")
                    if now - t_flush >= FLUSH_S:
                        if buf:
                            apply(db, buf)
                            total += len(buf)
                            buf = []
                        meta(db, heartbeat=round(now, 3), events=total)
                        t_flush = now
                        if now - last_prune > 60:
                            db.execute("DELETE FROM coins WHERE last_ms < ?", (int((now - PRUNE_AGE_S) * 1000),))
                            last_prune = now
        except Exception as e:
            print(time.strftime("%F %T"), f"stream error {type(e).__name__}: {e}; reconnect in {back}s", flush=True)
            meta(db, connected=0)
            await asyncio.sleep(back)
            back = min(back * 2, 60)


if __name__ == "__main__":
    asyncio.run(run())
