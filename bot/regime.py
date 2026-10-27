import json
import os
import statistics
import time
from datetime import datetime, timezone

from store import DATA_DIR


def pct(vals, q):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    k = (len(vals) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def dist(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"n": len(vals), "p25": round(pct(vals, .25), 2), "median": round(pct(vals, .5), 2),
            "p75": round(pct(vals, .75), 2)}


def _ratio(a, b):
    return (a / b) if b else None


def window_report(store, cfg, now, hours, extra):
    rg, e = cfg["regime"], cfg["entry"]
    start = now - hours * 3600
    obs = store.one("SELECT MIN(created_ts) t FROM coins WHERE seen_in_new_feed=1")["t"]
    eff_start = max(start, obs) if obs else now
    out = {"window_hours": hours,
           "observed_hours": round((now - eff_start) / 3600, 2),
           "note": None}
    if obs and obs > start:
        out["note"] = (f"bot's launch sample only reaches back {out['observed_hours']}h of this {hours}h window; "
                       f"numbers cover that part only, and only launches the bot actually saw")

    seen = store.q("""SELECT * FROM coins WHERE seen_in_new_feed=1 AND created_ts>=?""", (start,))
    n = len(seen)
    out["mints_seen"] = n
    mature_s = rg["grad_maturity_minutes"] * 60
    mature = [c for c in seen if now - c["created_ts"] >= mature_s]
    grads = [c for c in mature if c["complete"]]
    out["mature_mints"] = len(mature)
    out["grad_maturity_min"] = rg["grad_maturity_minutes"]
    out["graduated"] = len(grads)
    out["graduation_rate"] = round(_ratio(len(grads), len(mature)), 5) if mature else None
    out["graduated_any_age"] = sum(1 for c in seen if c["complete"])


    est = None
    gp, lr = extra.get("grad_page_created") or [], extra.get("launch_rate")
    if gp and lr:
        s0, s1 = max(min(gp), start), now - mature_s
        if s1 > s0:
            g_in = sum(1 for t in gp if s0 <= t <= s1)
            launches = lr * (s1 - s0) / 60
            est = {"graduated_in_span": g_in, "span_minutes": round((s1 - s0) / 60, 1),
                   "launch_rate_per_min": round(lr, 1), "launches_in_span_est": round(launches),
                   "rate": round(g_in / launches, 5) if launches else None}
    out["graduation_rate_feed_estimate"] = est
    out["graduation_rate_direct"] = out["graduation_rate"]
    if len(mature) < rg["min_sample_mature"] and est and est["rate"] is not None:
        out["graduation_rate"] = est["rate"]
        out["graduation_rate_source"] = "feed estimate (graduated list / measured launch rate)"
    else:
        out["graduation_rate_source"] = "direct (launches the bot saw)" if mature else "unavailable"
    out["graduation_note"] = (f"direct rate over {len(mature)} launches at least {rg['grad_maturity_minutes']} min old; "
                              f"{n - len(mature)} younger launches excluded (may still graduate)")


    dead_s = rg["dead_volume_minutes"] * 60
    death_ages = [(c["last_trade_ts"] - c["created_ts"]) / 60 for c in store.q(
        """SELECT created_ts,last_trade_ts FROM coins WHERE created_ts>=? AND last_trade_ts IS NOT NULL
           AND last_seen_ts - last_trade_ts >= ?""", (start, dead_s))]
    out["median_age_when_volume_dies_min"] = round(statistics.median(death_ages), 2) if death_ages else None
    out["volume_death_sample"] = len(death_ages)


    follow = rg["min_followup_minutes_ath"] * 60
    rows = store.q("""SELECT mcap_usd, ath_mcap_usd FROM coins WHERE created_ts>=? AND ath_mcap_usd>0
                      AND mcap_usd IS NOT NULL AND last_seen_ts - created_ts >= ?""", (start, follow))
    thr = rg["round_trip_drawdown_pct"] / 100
    rt = [r for r in rows if 1 - r["mcap_usd"] / r["ath_mcap_usd"] > thr]
    out["down_gt_ath_pct_share_all"] = round(_ratio(len(rt), len(rows)), 4) if rows else None
    band_reached = [r for r in rows if r["ath_mcap_usd"] >= e["min_mcap_usd"]]
    rt_b = [r for r in band_reached if 1 - r["mcap_usd"] / r["ath_mcap_usd"] > thr]
    out["down_gt_ath_pct_share_band_reached"] = round(_ratio(len(rt_b), len(band_reached)), 4) if band_reached else None
    out["ath_sample"] = {"all": len(rows), "band_reached": len(band_reached)}


    rs = [c["real_sol"] for c in store.q(
        """SELECT real_sol FROM coins WHERE created_ts>=? AND complete=0 AND real_sol IS NOT NULL""", (start,))]
    out["median_real_sol_on_curve"] = round(statistics.median(rs), 3) if rs else None


    win = e["dup_ticker_window_minutes"] * 60
    dup = store.one("""SELECT COUNT(*) n FROM coins a WHERE a.seen_in_new_feed=1 AND a.created_ts>=?
        AND EXISTS (SELECT 1 FROM coins b WHERE b.mint<>a.mint AND lower(b.symbol)=lower(a.symbol)
                    AND b.created_ts < a.created_ts AND b.created_ts >= a.created_ts - ?)""",
                    (start, win))["n"]
    out["duplicate_ticker_rate"] = round(_ratio(dup, n), 4) if n else None
    out["mayhem_share"] = round(_ratio(sum(1 for c in seen if c["mayhem_state"]), n), 4) if n else None


    omin = rg["outcome_min_minutes"] * 60
    band = store.q("""SELECT * FROM coins WHERE band_ts>=? AND band_ts <= ? AND
                      (last_seen_ts - band_ts >= ? OR complete=1)""", (start, now - omin, omin))
    winners = []
    for c in band:
        peak_mult = (c["peak_mcap_sol_after_band"] or 0) / c["band_mcap_sol"] if c["band_mcap_sol"] else 0
        ath_mult = 0
        if c["ath_mcap_usd"] and c["band_mcap_usd"] and c["ath_ts"] and c["ath_ts"] > c["band_ts"]:
            ath_mult = c["ath_mcap_usd"] / c["band_mcap_usd"]
        if max(peak_mult, ath_mult) >= rg["win_multiple"]:
            winners.append(c)
    out["band_coins_resolved"] = len(band)
    out["band_2x"] = len(winners)
    out["two_x_rate"] = round(_ratio(len(winners), len(band)), 4) if len(band) >= rg["min_sample_band"] else None
    out["what_worked"] = {
        "winners": len(winners),
        "usable": len(winners) >= rg["min_sample_prior"],
        "curve_pct": dist([c["band_curve_pct"] for c in winners]),
        "mcap_usd": dist([c["band_mcap_usd"] for c in winners]),
        "reply_count": dist([c["band_reply_count"] for c in winners]),
        "age_min": dist([c["band_age_min"] for c in winners]),
    }


    fresh = now - 3 * cfg["poll"]["interval_seconds"] - 120
    now_rows = store.q("""SELECT * FROM coins WHERE last_seen_ts>=? AND complete=0 AND mcap_usd BETWEEN ? AND ?
                          AND curve_pct BETWEEN ? AND ? AND created_ts >= ?""",
                       (fresh, e["min_mcap_usd"], e["max_mcap_usd"], e["min_curve_pct"], e["max_curve_pct"],
                        now - e["max_age_minutes"] * 60))
    out["in_entry_band_now"] = len(now_rows)
    rt_now = [r for r in now_rows if r["ath_mcap_usd"] and 1 - r["mcap_usd"] / r["ath_mcap_usd"] > thr]
    out["band_now_round_trip_share"] = (round(len(rt_now) / len(now_rows), 4)
                                        if len(now_rows) >= rg["min_sample_round_trip"] else None)

    td_fn = extra.get("trade_data_fn")
    out["trade_data"] = (td_fn(start) if td_fn else extra.get("trade_data")) or {
        "available": False,
        "reason": "no HELIUS_API_KEY / trade stream: same-block bundle supply share and first-10-min "
                  "unique buyers are NOT computed"}

    out["verdict"], out["verdict_reasons"], out["low_confidence"] = verdict(out, cfg)
    return out


def verdict(w, cfg):
    rg = cfg["regime"]
    reasons = []
    if not w["mints_seen"]:
        return "stand_down", ["no launch data observed (feed down or blocked)"], True
    low = w["mints_seen"] < rg["min_sample_mints"]
    if low:
        reasons.append(f"low sample: {w['mints_seen']} mints < {rg['min_sample_mints']}")
    g, x, rt = w["graduation_rate"], w["two_x_rate"], w["band_now_round_trip_share"]
    grad_wash = g is not None and g <= rg["stand_down_grad_rate_max"]
    x_wash = x is not None and x <= rg["stand_down_2x_rate_max"]
    if grad_wash and x_wash:
        reasons.append(f"graduation {g:.2%} and band 2x rate {x:.1%} both washed out")
        return "stand_down", reasons, low
    if rt is not None and rt >= rg["stand_down_round_trip_share_min"]:
        reasons.append(f"{rt:.0%} of in-band names already round-tripped")
        return "stand_down", reasons, low
    if rg.get("stand_down_if_no_2x_24h") and w.get("window_hours", 0) >= 24 and x is not None and x == 0:
        reasons.append(f"no 2x from the entry band in the last {w['window_hours']}h "
                       f"({w['band_coins_resolved']} resolved band coins): the rule stopped paying")
        return "stand_down", reasons, low
    if x is None:
        reasons.append(f"2x rate unknown (resolved band coins {w['band_coins_resolved']} < {rg['min_sample_band']})")
    ok = (g is not None and g >= rg["tradable_grad_rate_min"] and x is not None and x >= rg["tradable_2x_rate_min"]
          and (rt is None or rt < rg["tradable_round_trip_share_max"]))
    if ok and not low:
        reasons.append(f"graduation {g:.2%}, band 2x {x:.1%}")
        return "tradable", reasons, low
    if g is not None:
        reasons.append(f"graduation {g:.2%}" + (f", band 2x {x:.1%}" if x is not None else ""))
    return "selective", reasons, low


ORDER = {"tradable": 0, "selective": 1, "stand_down": 2}


def compute(store, cfg, now=None, extra=None):
    now = now or time.time()
    extra = extra or {}
    windows = {f"{h}h": window_report(store, cfg, now, h, extra) for h in cfg["regime"]["windows_hours"]}
    vf = cfg["regime"]["verdict_from"]
    if vf == "worst":
        overall = max((w["verdict"] for w in windows.values()), key=lambda v: ORDER[v])
    else:
        overall = windows[vf]["verdict"]

    v24 = windows.get("24h", {}).get("verdict")
    streak = 1 if v24 == "stand_down" else 0
    if streak:
        for r in store.last_regimes(50):
            if r["verdict_24h"] == "stand_down":
                streak += 1
            else:
                break
    feed = store.q("""SELECT source, status, COUNT(*) n FROM feed_log WHERE ts>=? GROUP BY source, status""",
                   (now - 3600,))
    report = {
        "generated_at_utc": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        "verdict": overall,
        "verdict_from": vf,
        "stand_down_streak_24h": streak,
        "stand_down_warning": streak >= cfg["regime"]["stand_down_streak_warn"],
        "windows": windows,
        "sources": extra.get("sources", {}),
        "requests_last_hour": [dict(r) for r in feed],
        "sol_usd_from_feed": extra.get("sol_usd"),
        "launch_rate_per_min_observed": extra.get("launch_rate"),
    }
    return report


def _fmt(v, kind="num"):
    if v is None:
        return "n/a"
    if kind == "pct":
        return f"{v:.2%}" if v < 0.1 else f"{v:.1%}"
    if kind == "usd":
        return f"${v:,.0f}"
    return f"{v:,}" if isinstance(v, int) else f"{v:,.2f}"


def to_markdown(rep):
    L = [f"# Trench regime: **{rep['verdict'].upper()}**", "",
         f"Generated {rep['generated_at_utc']} (UTC). Verdict from: {rep['verdict_from']}. Paper trading only.", ""]
    if rep["stand_down_warning"]:
        L += [f"> 24h regime has been stand_down for {rep['stand_down_streak_24h']} refreshes in a row. "
              "Recommendation: stand down. Do not loosen filters to force trades.", ""]
    for name, w in rep["windows"].items():
        L.append(f"## Last {name}: {w['verdict']}" + (" (low confidence)" if w["low_confidence"] else ""))
        if w["note"]:
            L.append(f"_{w['note']}_")
        L += ["", "| metric | value |", "|---|---|",
              f"| mints seen by bot | {_fmt(w['mints_seen'])} |",
              f"| graduation rate used | {_fmt(w['graduation_rate'], 'pct')} ({w['graduation_rate_source']}) |",
              f"| graduated, direct (launches >= {w['grad_maturity_min']} min old) | {_fmt(w['graduated'])} of {_fmt(w['mature_mints'])} ({_fmt(w['graduation_rate_direct'], 'pct')}) |",
              f"| graduated, feed estimate | " + (f"{w['graduation_rate_feed_estimate']['graduated_in_span']} grads in {w['graduation_rate_feed_estimate']['span_minutes']} min of launches "
                f"/ ~{w['graduation_rate_feed_estimate']['launches_in_span_est']:,} launches ({_fmt(w['graduation_rate_feed_estimate']['rate'], 'pct')})" if w['graduation_rate_feed_estimate'] else "n/a") + " |",
              f"| median age when volume dies (min) | {_fmt(w['median_age_when_volume_dies_min'])} (n={w['volume_death_sample']}) |",
              f"| down >60% from own ATH (all / reached band) | {_fmt(w['down_gt_ath_pct_share_all'], 'pct')} / {_fmt(w['down_gt_ath_pct_share_band_reached'], 'pct')} (n={w['ath_sample']['all']}/{w['ath_sample']['band_reached']}) |",
              f"| median real SOL on curve | {_fmt(w['median_real_sol_on_curve'])} |",
              f"| in entry band right now | {_fmt(w['in_entry_band_now'])} (round-tripped: {_fmt(w['band_now_round_trip_share'], 'pct')}) |",
              f"| duplicate-ticker rate | {_fmt(w['duplicate_ticker_rate'], 'pct')} |",
              f"| mayhem share | {_fmt(w['mayhem_share'], 'pct')} |",
              f"| band coins resolved / did 2x | {w['band_coins_resolved']} / {w['band_2x']} (rate {_fmt(w['two_x_rate'], 'pct')}) |",
              ""]
        ww = w["what_worked"]
        if ww["winners"]:
            L.append(f"What worked ({ww['winners']} coins that later did 2x from band entry; "
                     f"{'used' if ww['usable'] else 'too few to use'} as scoring prior):")
            for k in ("curve_pct", "mcap_usd", "reply_count", "age_min"):
                d = ww[k]
                if d:
                    L.append(f"- {k}: median {d['median']}, middle 50% {d['p25']}–{d['p75']}")
        else:
            L.append("What worked: no resolved 2x winners observed yet.")
        td = w["trade_data"]
        L.append(f"- Trade data: {'available' if td.get('available') else 'UNAVAILABLE'}"
                 + (f" — {td.get('reason')}" if td.get("reason") else ""))
        if td.get("profiled_coins"):
            L.append(f"  - band coins profiled from on-chain trades: {td['profiled_coins']}"
                     f" ({td.get('capped', 0)} hit the parse cap, so their buyer counts are lower bounds)")
            L.append(f"  - same-block (first {td.get('first_slots')} slots) bundle supply share: median "
                     f"{_fmt(td.get('median_first_slot_supply_share'), 'pct')} (excluding creator "
                     f"{_fmt(td.get('median_first_slot_supply_share_ex_creator'), 'pct')})")
            L.append(f"  - unique buyers in first {td.get('window_minutes')} min: median {_fmt(td.get('median_unique_buyers_window'))}")
            L.append(f"  - trade count in first {td.get('window_minutes')} min: median {_fmt(td.get('median_trade_count_window'))} "
                     f"(unique buyers matter more: trade count includes wash churn)")
            L.append(f"  - single-signature multi-wallet buys: {_fmt(td.get('coins_with_multi_wallet_sigs'))} of "
                     f"{_fmt(td.get('profiled_coins'))} profiled coins had one; median supply share "
                     f"{_fmt(td.get('median_multi_wallet_supply_share'), 'pct')}")
        if td.get("coins_cluster_checked"):
            L.append(f"  - funding-cluster check: {td['coins_cluster_checked']} coins checked, "
                     f"{_fmt(td.get('bundled_share_of_checked'), 'pct')} flagged bundled, median largest cluster "
                     f"{_fmt(td.get('median_largest_funding_cluster_share'), 'pct')} of top-holder supply")
        if td.get("wallet_book"):
            L.append(f"  - wallet book: {td['wallet_book']}")
        L.append("- Why: " + "; ".join(w["verdict_reasons"]))
        L.append("")
    L.append("## Data sources")
    for k, v in rep["sources"].items():
        L.append(f"- {k}: {v}")
    if rep["requests_last_hour"]:
        agg = {}
        for r in rep["requests_last_hour"]:
            agg.setdefault(r["source"], {})[r["status"]] = r["n"]
        for src, st in agg.items():
            L.append(f"- {src} requests last hour: " + ", ".join(f"{k}={v}" for k, v in sorted(st.items())))
    if rep.get("sol_usd_from_feed"):
        L.append(f"- SOL/USD implied by feed: {rep['sol_usd_from_feed']:.2f}")
    if rep.get("launch_rate_per_min_observed"):
        L.append(f"- Launch rate seen in newest page: {rep['launch_rate_per_min_observed']:.1f}/min")
    return "\n".join(L) + "\n"


def write(rep, data_dir=DATA_DIR):
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, "regime.json"), "w") as f:
        json.dump(rep, f, indent=2, default=str)
    with open(os.path.join(data_dir, "regime.md"), "w") as f:
        f.write(to_markdown(rep))
