import json
import os


def hard_filter(coin, now, cfg, dup_ticker=False):
    e = cfg["entry"]
    if coin.get("complete") and e["skip_graduated"]:
        return "graduated"
    if "mayhem_state" not in coin or coin.get("mayhem_state"):
        return "mayhem_skip"
    if e["sol_quote_only"] and coin.get("quote_mint") != cfg["feed"]["sol_quote_mint"] \
            and coin.get("quote_mint") not in (e.get("allowed_quote_mints") or []):
        return "non_sol_quote"
    mcap = coin.get("mcap_usd")
    if mcap is None:
        return "no_mcap"
    if not (e["min_mcap_usd"] <= mcap <= e["max_mcap_usd"]):
        return "mcap_out_of_band"
    cp = coin.get("curve_pct")
    if cp is None or not (e["min_curve_pct"] <= cp <= e["max_curve_pct"]):
        return "curve_out_of_band"
    if coin.get("created_ts") is None:
        return "no_age"
    age_min = (now - coin["created_ts"]) / 60.0
    if age_min > e["max_age_minutes"]:
        return "too_old"
    if dup_ticker and e.get("skip_duplicate_ticker", True):
        return "duplicate_ticker"
    ath = coin.get("ath_mcap_usd")
    if ath and ath > 0 and not e["allow_reclaim"]:
        dd = (1 - mcap / ath) * 100
        if dd > cfg["regime"]["round_trip_drawdown_pct"]:
            return "round_tripped"
    return None


def _in_iqr(val, dist):
    return dist and val is not None and dist.get("p25") is not None and dist["p25"] <= val <= dist["p75"]


def score_coin(coin, now, cfg, prior=None, bundle=None, wallet_hits=None):
    s = cfg["score"]
    bd = [["base", s["base"]]]
    age_min = (now - coin["created_ts"]) / 60.0
    cp = coin["curve_pct"]
    mcap = coin["mcap_usd"]
    replies = coin.get("reply_count") or 0

    if s["curve_sweet_low"] <= cp <= s["curve_sweet_high"]:
        bd.append([f"curve {cp:.0f}% in sweet spot {s['curve_sweet_low']}-{s['curve_sweet_high']}%",
                   s["curve_sweet_points"]])

    ignore_replies = bool(s.get("ignore_replies"))
    if ignore_replies:
        bd.append([f"replies ignored ({replies} replies, 0 pts)", 0])
    elif replies == 0:
        bd.append(["silent thread (0 replies)", -s["silent_thread_penalty"]])
    else:
        pts = min(s["reply_points_max"], replies * s["reply_points_per_reply"])
        bd.append([f"{replies} replies", round(pts, 2)])

    if coin.get("has_socials"):
        bd.append(["lists socials (tag only)", s["socials_points"]])

    if age_min < s["vertical_age_minutes"] and cp > s["vertical_curve_pct"]:
        bd.append([f"vertical: {cp:.0f}% curve at {age_min:.1f} min (bundle risk)", -s["vertical_penalty"]])

    ath = coin.get("ath_mcap_usd")
    if ath and ath > 0:
        ratio = min(1.0, mcap / ath)
        if age_min >= s["ath_hold_min_age_minutes"] and ratio < s["ath_hold_min_ratio"]:
            bd.append([f"holds only {ratio:.0%} of ATH after {age_min:.0f} min", -s["ath_hold_penalty"]])
        elif ratio >= s["near_ath_ratio"]:
            bd.append([f"near ATH ({ratio:.0%})", s["near_ath_points"]])

    prev = coin.get("prev_mcap_sol")
    cur = coin.get("mcap_sol")
    if prev and cur and prev > 0:
        chg = (cur / prev - 1) * 100
        if chg > 0:
            pts = s["momentum_points_max"] * min(1.0, chg / s["momentum_full_pct"])
            bd.append([f"mcap +{chg:.0f}% since last seen", round(pts, 2)])
        elif chg < 0:
            bd.append([f"mcap {chg:.0f}% since last seen", -s["momentum_drop_penalty"]])
    else:
        bd.append(["momentum: first sighting, no prior price", 0])

    if prior and prior.get("usable"):
        feats = {"curve_pct": cp, "mcap_usd": mcap, "reply_count": replies, "age_min": age_min}
        if ignore_replies:
            feats.pop("reply_count")
        for k, v in feats.items():
            if _in_iqr(v, prior.get(k)):
                bd.append([f"{k} inside recent 2x winners' middle 50%", s["prior_points_per_feature"]])
    else:
        bd.append(["prior: not enough recent 2x winners yet", 0])

    if bundle is None:
        bd.append(["bundle check: unavailable (no HELIUS_API_KEY / trade data)", 0])
    elif bundle.get("bundled"):
        bd.append([f"bundled: {bundle.get('reason')}", -s["bundle_penalty"]])
    else:
        bd.append([f"funding clusters ok: {bundle.get('reason')}", 0])

    tb = top10_band_bonus(bundle, s.get("top10_bonus"))
    if tb:
        bd.append(tb)

    if wallet_hits:
        wh = wallet_hits
        if wh.get("follow"):
            bd.append([f"{wh['follow']} follow-tier wallet(s) among top holders",
                       min(s["follow_wallet_points_max"], wh["follow"] * s["follow_wallet_points"])])
        neg = wh.get("risky", 0) * s["risky_wallet_penalty"] + wh.get("bundler", 0) * s["bundler_wallet_penalty"]
        if neg:
            bd.append([f"{wh.get('risky', 0)} risky + {wh.get('bundler', 0)} bundler wallet(s) among top holders",
                       -min(s["risky_penalty_max"], neg)])
        if wh.get("dev_tier") in ("risky", "bundler"):
            bd.append([f"dev wallet is {wh['dev_tier']}", -s["risky_dev_penalty"]])

    total = round(sum(p for _, p in bd), 2)
    return total, bd


def top10_band_bonus(holder, rule):
    if not rule:
        return None
    lo, hi, pts = float(rule["min_share"]), float(rule["max_share"]), rule["points"]
    band = f"{lo:.0%}-{hi:.0%}"
    if not holder or holder.get("holder_src") != "pumpfun" or holder.get("top10_holder_share") is None:
        return [f"top10 winners range {band}: no holder data (0, never guessed)", 0]
    t = float(holder["top10_holder_share"])
    if holder.get("bundled"):
        return [f"top10 winners range {band}: n/a, bundled (top10 {t:.1%})", 0]
    if lo <= t <= hi:
        return [f"top10 in winners range {band} ({t:.1%}) +{pts:g}", pts]
    return [f"top10 {t:.1%} outside winners range {band}", 0]


def holder_bonus_max(cfg):
    return max(0, (cfg["score"].get("top10_bonus") or {}).get("points", 0))


def momentum_since_last_look(coin):
    prev, cur = coin.get("prev_mcap_sol"), coin.get("mcap_sol")
    if not (prev and cur and prev > 0):
        return None
    chg = (cur / prev - 1) * 100
    if chg == 0:
        return 0.0
    return float(f"{chg:.0f}")


def filter_tags(coin, now, cfg, creator_prior_n=None, dup_ticker=None):
    k = cfg.get("skips") or {}
    out = []
    if k.get("spike_pct") is not None:
        mom = momentum_since_last_look(coin)
        if mom is not None and mom >= float(k["spike_pct"]):
            out.append(("spike", f"mcap {mom:+.0f}% since last look (spike >= +{k['spike_pct']:g}%)"))
    s = cfg["score"]
    age_min = (now - coin["created_ts"]) / 60.0
    if age_min < s["vertical_age_minutes"] and (coin["curve_pct"] or 0) > s["vertical_curve_pct"]:
        out.append(("vertical", f"vertical {coin['curve_pct']:.0f}% curve at {age_min:.1f} min (bundle risk)"))
    if k.get("serial_creator_at") is not None and creator_prior_n is not None:
        if creator_prior_n >= int(k["serial_creator_at"]):
            out.append(("serial_creator", f"creator has {creator_prior_n} earlier coins in our records (>= {k['serial_creator_at']})"))
    if dup_ticker:
        out.append(("dup_ticker", f"same ticker launched within {cfg['entry'].get('dup_ticker_window_minutes')} min before it"))
    return out


def skip_enabled(cfg, tag):
    k = cfg.get("skips") or {}
    return bool(k.get(tag, tag != "vertical"))


def pre_skip(coin, now, cfg, creator_prior_n=None):
    for tag, why in filter_tags(coin, now, cfg, creator_prior_n):
        if skip_enabled(cfg, tag):
            return tag, why
    return None


def holder_count_tag(holder, cfg):
    lim = (cfg.get("skips") or {}).get("holders_at_most")
    if lim is None or not holder or holder.get("holder_src") != "pumpfun":
        return None
    n = holder.get("total_holders")
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "holder_count_missing"
    return "too_few_holders" if n <= lim else None


def holder_count_skip(holder, cfg):
    t = holder_count_tag(holder, cfg)
    return t if t and skip_enabled(cfg, t) else None


def min_score_for(verdict, cfg):
    m = cfg["score"]["min_score"]
    if verdict == "selective":
        m += cfg["score"]["selective_min_score_bump"]
    return m






SCORE_V2_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "score_v2.json")
_V2_CACHE = {"mtime": None, "table": None}


V2_EDGES = {
    "holders": [10, 50],
    "top10": [0.10, 0.40],
    "bs": [0.05, 0.25],
    "dev": [0.0005],
    "n_prior": [1, 3],
    "age_min": [1, 3, 10],
    "entry_mcap_usd": [5000, 15000],
}


def v2_buckets(f, edges=None):
    E = dict(V2_EDGES, **(edges or {}))

    def b(x, cuts):
        if x is None:
            return "no data"
        lo = -1e18
        for i, hi in enumerate(list(cuts) + [1e18]):
            if lo <= x < hi:
                return f"bucket {i + 1}"
            lo = hi
        return "no data"
    return {
        "holders": b(f.get("holders"), E["holders"]),
        "top10": b(f.get("top10"), E["top10"]),
        "bs": b(f.get("bs"), E["bs"]),
        "dev": b(f.get("dev"), E["dev"]),
        "creator": b(f.get("n_prior"), E["n_prior"]),
        "age": b(f.get("age_min"), E["age_min"]),
        "path": {"first_seen": "first seen in entry band", "climbed": "climbed into entry band"}.get(f.get("entry_path"), "no data"),
        "mcap": b(f.get("entry_mcap_usd"), E["entry_mcap_usd"]),
        "dup": "no data" if f.get("dup_ticker") is None else ("dup ticker" if f.get("dup_ticker") else "unique ticker"),
        "vertical": "no data" if f.get("vertical") is None else ("vertical" if f.get("vertical") else "not vertical"),
    }


V2_NAMES = {"holders": "holders", "top10": "top-10 share", "bs": "bundlers+snipers", "dev": "dev holds",
            "creator": "creator", "age": "age at entry", "path": "entry path", "mcap": "entry mcap", "dup": "ticker",
            "vertical": "vertical tag"}


def load_score_v2_table(path=None):
    path = path or SCORE_V2_PATH
    try:
        mt = os.path.getmtime(path)
    except OSError:
        return None
    if _V2_CACHE["mtime"] != mt or _V2_CACHE.get("path") != path:
        with open(path) as fh:
            _V2_CACHE.update(mtime=mt, path=path, table=json.load(fh))
    return _V2_CACHE["table"]


def score_v2(f, table=None):
    t = table if table is not None else load_score_v2_table()
    if not t:
        return None, []
    pts = t["points"]
    base = float(t.get("base", 50))
    s, why = base, [["base", base]]
    for k, bucket in v2_buckets(f, t.get("edges")).items():
        if k not in pts:
            continue
        p = float(pts[k].get(bucket, 0))
        s += p
        if p:
            why.append([f"{V2_NAMES.get(k, k)}: {bucket}", p])
    return round(max(0.0, min(100.0, s)), 1), why
