import itertools
import json
import math
import os
import statistics
import time
import uuid
from datetime import datetime, timezone
from statistics import NormalDist

import exits
import store as st

MODES = ("learning", "auto_tune", "live")
CHANGES_LOG = os.path.join(st.HISTORY_DIR, "changes.log")
EULER_GAMMA = 0.5772156649


def log_change(text):
    os.makedirs(st.HISTORY_DIR, exist_ok=True)
    with open(CHANGES_LOG, "a") as f:
        f.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}  {text}\n")


def one_r(cfg):
    return cfg["paper"]["size_sol"] * cfg["exits"]["hard_stop_pct"] / 100.0




def load_paths(store, limit=None):
    rows = store.q("SELECT * FROM paper WHERE status='closed' ORDER BY entry_ts")
    if limit:
        rows = rows[-limit:]
    out = []
    for p in rows:
        ticks = sorted((t - p["entry_ts"], m) for t, m in store.ticks(p["id"]) if t >= p["entry_ts"] and m)
        path = [x for x in ticks if x[0] > 0]
        if len(path) >= 1:
            out.append({"id": p["id"], "symbol": p["symbol"], "path": path})
    return out


def grid_configs(cfg):
    g = {k: v for k, v in cfg["learn"]["grid"].items()}
    keys = list(g)
    for combo in itertools.product(*[g[k] for k in keys]):
        ex = dict(cfg["exits"])
        ex.update(dict(zip(keys, combo)))
        if ex["tp2_multiple"] <= ex["tp1_multiple"]:
            ex["tp2_multiple"] = round(ex["tp1_multiple"] * 1.5, 2)
        yield {k: ex[k] for k in keys}, ex


def pnl_matrix(trades, cfg):
    configs, mat = [], []
    for params, ex in grid_configs(cfg):
        configs.append((params, ex))
        mat.append([exits.replay_trade(t["path"], ex, cfg["paper"], t["id"]) for t in trades])
    return configs, mat




def sharpe(xs):
    if len(xs) < 2:
        return None
    sd = statistics.pstdev(xs)
    return statistics.mean(xs) / sd if sd > 0 else None


def deflated_sharpe(returns, trial_sharpes):
    T = len(returns)
    sr = sharpe(returns)
    trials = [s for s in trial_sharpes if s is not None]
    N = len(trials)
    if sr is None or T < 30 or N < 2:
        return None
    mu, sd = statistics.mean(returns), statistics.pstdev(returns)
    skew = sum(((x - mu) / sd) ** 3 for x in returns) / T
    kurt = sum(((x - mu) / sd) ** 4 for x in returns) / T
    v = statistics.pvariance(trials)
    nd = NormalDist()
    sr0 = math.sqrt(v) * ((1 - EULER_GAMMA) * nd.inv_cdf(1 - 1 / N) + EULER_GAMMA * nd.inv_cdf(1 - 1 / (N * math.e)))
    den = 1 - skew * sr + (kurt - 1) / 4 * sr ** 2
    if den <= 0:
        return None
    return nd.cdf((sr - sr0) * math.sqrt(T - 1) / math.sqrt(den))


def pbo_cscv(mat, n_blocks=8):
    n_cfg = len(mat)
    T = len(mat[0]) if mat else 0
    if n_cfg < 2 or T < n_blocks * 2:
        return None, 0
    size = T // n_blocks
    blocks = [list(range(i * size, (i + 1) * size if i < n_blocks - 1 else T)) for i in range(n_blocks)]
    below, total = 0, 0
    for is_idx in itertools.combinations(range(n_blocks), n_blocks // 2):
        oos_idx = [b for b in range(n_blocks) if b not in is_idx]
        is_cols = [c for b in is_idx for c in blocks[b]]
        oos_cols = [c for b in oos_idx for c in blocks[b]]
        is_perf = [sum(r[c] for c in is_cols) / len(is_cols) for r in mat]
        oos_perf = [sum(r[c] for c in oos_cols) / len(oos_cols) for r in mat]
        best = max(range(n_cfg), key=lambda i: is_perf[i])
        rank = sum(1 for x in oos_perf if x < oos_perf[best]) + 0.5 * sum(1 for x in oos_perf if x == oos_perf[best])
        w = rank / n_cfg
        w = min(max(w, 1e-6), 1 - 1e-6)
        if math.log(w / (1 - w)) <= 0:
            below += 1
        total += 1
    return below / total, total


def walk_forward(trades, cfg):
    L = cfg["learn"]
    A, B = L["walk_forward_train_trades"], L["walk_forward_test_trades"]
    folds = []
    start = 0
    while start + A + B <= len(trades):
        train, test = trades[start:start + A], trades[start + A:start + A + B]
        best, best_ex, best_params = None, None, None
        for params, ex in grid_configs(cfg):
            v = statistics.mean(exits.replay_trade(t["path"], ex, cfg["paper"], t["id"]) for t in train)
            if best is None or v > best:
                best, best_ex, best_params = v, ex, params
        oos = statistics.mean(exits.replay_trade(t["path"], best_ex, cfg["paper"], t["id"]) for t in test)
        cur = statistics.mean(exits.replay_trade(t["path"], cfg["exits"], cfg["paper"], t["id"]) for t in test)
        folds.append({"train": [start, start + A], "test": [start + A, start + A + B], "is_expectancy": best,
                      "oos_expectancy": oos, "current_oos_expectancy": cur, "params": best_params})
        start += B
    if not folds:
        return {"folds": [], "degradation": None, "note": f"need at least {A + B} trades for one walk-forward fold"}
    is_m = statistics.mean(f["is_expectancy"] for f in folds)
    oos_m = statistics.mean(f["oos_expectancy"] for f in folds)
    deg = (oos_m / is_m) if is_m > 0 else None
    return {"folds": folds, "degradation": deg, "is_mean": is_m, "oos_mean": oos_m,
            "current_oos_mean": statistics.mean(f["current_oos_expectancy"] for f in folds),
            "note": None if deg is not None else "in-sample expectancy <= 0: nothing to degrade from (rejected)"}


def peak_stats(trades):
    peaks = sorted(max(m for _, m in t["path"]) for t in trades)
    if not peaks:
        return None
    n = len(peaks)
    return {"n": n, "median": round(statistics.median(peaks), 2), "p25": round(peaks[n // 4], 2),
            "p75": round(peaks[(3 * n) // 4], 2), "share_1_5x": round(sum(p >= 1.5 for p in peaks) / n, 3),
            "share_2x": round(sum(p >= 2 for p in peaks) / n, 3), "share_3x": round(sum(p >= 3 for p in peaks) / n, 3)}




def adaptive_exits(store, cfg, log_trials=True):
    L = cfg["learn"]
    trades = load_paths(store)
    n = len(trades)
    notes, diffs = [], []
    stats = {"n": n, "pbo": None, "dsr": None, "degradation": None}
    if n == 0:
        notes.append("adaptive exits: no closed paper trades with price paths yet")
        return {"notes": notes, "diffs": diffs, "stats": stats}
    R = one_r(cfg)
    configs, mat = pnl_matrix(trades, cfg)
    cur_ex = cfg["exits"]
    cur = [exits.replay_trade(t["path"], cur_ex, cfg["paper"], t["id"]) for t in trades]
    exp_cur = statistics.mean(cur)
    trial_sharpes = [sharpe(r) for r in mat]
    best_i = max(range(len(mat)), key=lambda i: statistics.mean(mat[i]))
    best_params = configs[best_i][0]
    exp_best = statistics.mean(mat[best_i])
    pbo, n_splits = pbo_cscv(mat, L["pbo_blocks"])
    dsr = deflated_sharpe(mat[best_i], trial_sharpes)
    wf = walk_forward(trades, cfg)
    stats.update(pbo=pbo, dsr=dsr, degradation=wf["degradation"], trials=len(mat), expectancy_current=exp_cur,
                 expectancy_best_in_sample=exp_best, best_params=best_params)
    if log_trials:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
        for i, ((params, _), row) in enumerate(zip(configs, mat)):
            store.add_trial({"run_id": run_id, "kind": "grid", "params": params, "n_trades": n,
                             "pnl_sol": sum(row), "expectancy_sol": statistics.mean(row),
                             "expectancy_r": statistics.mean(row) / R, "sharpe": trial_sharpes[i],
                             "selected": i == best_i})
        store.add_trial({"run_id": run_id, "kind": "summary", "params": {"current": {k: cur_ex[k] for k in L["grid"]}},
                         "n_trades": n, "pnl_sol": sum(cur), "expectancy_sol": exp_cur, "expectancy_r": exp_cur / R,
                         "note": json.dumps({"pbo": pbo, "pbo_splits": n_splits, "deflated_sharpe": dsr,
                                             "walk_forward_degradation": wf["degradation"], "folds": len(wf["folds"])})})
        store.commit()
        stats["run_id"] = run_id
    ps = peak_stats(trades)
    notes.append(f"adaptive exits: replayed {n} closed trades x {len(mat)} exit configs (all logged in tune_trials). "
                 f"Peaks: median {ps['median']}x, middle 50% {ps['p25']}-{ps['p75']}x; {ps['share_1_5x']:.0%} reached the first target, "
                 f"{ps['share_2x']:.0%} 2x, {ps['share_3x']:.0%} 3x.")
    notes.append(f"current exits: expectancy {exp_cur:+.4f} SOL/trade ({exp_cur / R:+.2f}R), total {sum(cur):+.3f} SOL after costs.")
    sweep = {}
    for (params, _), row in zip(configs, mat):
        sweep.setdefault(params.get("tp1_multiple"), []).append(statistics.mean(row))
    if None not in sweep:
        notes.append("first-target evidence (best expectancy per first target, SOL/trade): "
                     + ", ".join(f"{k}x {max(v):+.4f}" for k, v in sorted(sweep.items())))
    notes.append(f"in-sample best: {best_params} at {exp_best:+.4f} SOL/trade. PBO "
                 f"{'n/a (too few trades)' if pbo is None else f'{pbo:.2f}'}, deflated Sharpe "
                 f"{'n/a' if dsr is None else f'{dsr:.2f}'}, walk-forward degradation "
                 f"{'n/a' if wf['degradation'] is None else f'{wf['degradation']:.2f}'}"
                 + (f" ({wf['note']})" if wf.get("note") else f" over {len(wf['folds'])} folds"))
    if n < L["min_trades"]:
        notes.append(f"adaptive exits: {n} of {L['min_trades']} trades needed (learn.min_trades). Preview only, NOT a suggestion.")
        return {"notes": notes, "diffs": diffs, "stats": stats}
    deg = wf["degradation"]
    if deg is None or deg < L["reject_degradation_below"]:
        notes.append(f"REJECTED: walk-forward degradation {deg if deg is None else round(deg, 2)} < "
                     f"{L['reject_degradation_below']} (the optimizer memorized noise). No exit change suggested.")
        return {"notes": notes, "diffs": diffs, "stats": stats}
    if pbo is not None and pbo > L["max_pbo"]:
        notes.append(f"REJECTED: PBO {pbo:.2f} > {L['max_pbo']} (the search destroyed information). No exit change suggested.")
        return {"notes": notes, "diffs": diffs, "stats": stats}
    if wf["oos_mean"] - wf["current_oos_mean"] <= 0:
        notes.append("walk-forward picks did not beat current exits out of sample: no exit change suggested.")
        return {"notes": notes, "diffs": diffs, "stats": stats}
    warn = "" if deg >= L["tolerable_degradation"] else f" (degradation {deg:.2f} is below the tolerable {L['tolerable_degradation']})"
    for k, v in best_params.items():
        if v != cur_ex[k]:
            diffs.append((f"exits.{k}", cur_ex[k], v,
                          f"replay of {n} trades: {exp_best:+.4f} vs {exp_cur:+.4f} SOL/trade; walk-forward OOS "
                          f"{wf['oos_mean']:+.4f} vs current {wf['current_oos_mean']:+.4f}; PBO {pbo}; degradation {deg:.2f}{warn}"))
    return {"notes": notes, "diffs": diffs, "stats": stats}




def unlock_status(store, cfg, stats=None):
    L = cfg["learn"]
    n = store.one("SELECT COUNT(*) n FROM paper WHERE status='closed'")["n"]
    if n < L["min_trades"]:
        return False, f"{n} of {L['min_trades']} closed paper trades (learn.min_trades)"
    stats = stats or adaptive_exits(store, cfg, log_trials=False)["stats"]
    deg, pbo = stats.get("degradation"), stats.get("pbo")
    if deg is None or deg < L["unlock_min_degradation"]:
        return False, f"walk-forward degradation {deg if deg is None else round(deg, 2)} < {L['unlock_min_degradation']}"
    if pbo is None or pbo > L["max_pbo"]:
        return False, f"PBO {pbo if pbo is None else round(pbo, 2)} > {L['max_pbo']} (or not computable)"
    return True, f"{n} trades, degradation {deg:.2f}, PBO {pbo:.2f}"


def set_mode(mode, store=None):
    if mode not in ("learning", "auto_tune"):
        if mode == "live":
            return ("live mode can't be switched on from here. This build has no signer: live trading isn't "
                    "implemented. It would also need execution.enabled=true, a key env var set on your own computer, "
                    "and size caps, and that only after paper is green over a few hundred signals.")
        return f"unknown mode '{mode}'. Use: learning or auto_tune"
    raw = st.load_raw_config()
    old = raw.get("mode")
    if mode == "auto_tune":
        cfg = st.load_config()
        ok, why = unlock_status(store or st.Store(), cfg)
        if not ok:
            log_change(f"mode auto_tune requested but LOCKED ({why}); staying in {old}")
            return (f"auto_tune is LOCKED: {why}. It unlocks after {cfg['learn']['min_trades']} closed paper trades, "
                    f"with walk-forward degradation >= {cfg['learn']['unlock_min_degradation']} and PBO <= "
                    f"{cfg['learn']['max_pbo']}. Staying in {old}.")
    if old == mode:
        return f"mode is already {mode}"
    raw["mode"] = mode
    snap = st.write_config(raw)
    log_change(f"mode {old} -> {mode} (python bot.py mode)")
    return f"mode changed {old} -> {mode}. Snapshot: {os.path.relpath(snap, st.ROOT) if snap else '-'}"


LOOSENING = {"score.min_score": -1, "entry.min_curve_pct": -1}


def within_bounds(diffs, cfg):
    out = []
    b = cfg["learn"]["bounds"]
    for k, old, new, why in diffs:
        if k not in b:
            continue
        lo, hi = b[k]
        nv = min(hi, max(lo, new))
        if isinstance(old, int) and isinstance(new, int):
            nv = int(nv)
        if nv != old:
            out.append((k, old, nv, why + ("" if nv == new else f" (clamped to bounds {lo}-{hi})")))
    return out


def auto_tune(diffs, cfg, stand_down, state_path=None, now=None, unlocked=True):
    now = now or time.time()
    state_path = state_path or os.path.join(st.DATA_DIR, "auto_tune_state.json")
    if cfg.get("mode") != "auto_tune" or not unlocked:
        return []
    try:
        with open(state_path) as fh:
            last = json.load(fh).get("last_ts", 0)
    except (OSError, ValueError):
        last = 0
    if now - last < cfg["learn"]["auto_tune_every_minutes"] * 60:
        return []
    ok = within_bounds(diffs, cfg)
    if stand_down:
        ok = [d for d in ok if d[0] in LOOSENING and (d[2] - d[1]) * LOOSENING[d[0]] < 0]
    if not ok:
        return []
    raw = st.load_raw_config()
    for k, old, new, why in ok:
        st.set_config_value(raw, k, new)
    st.write_config(raw)
    for k, old, new, why in ok:
        log_change(f"auto_tune {k}: {old} -> {new} because {why}")
    with open(state_path, "w") as f:
        json.dump({"last_ts": now}, f)
    return ok
