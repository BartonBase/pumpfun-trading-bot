import statistics
from datetime import datetime, timezone


def _pf(pnls):
    w = sum(x for x in pnls if x > 0)
    l_ = -sum(x for x in pnls if x <= 0)
    if l_ == 0:
        return None if w == 0 else float("inf")
    return w / l_


def book_metrics(trades, one_r, now=None):
    trades = sorted(trades, key=lambda t: t["close_ts"])
    pnl = [t["pnl_sol"] for t in trades]
    n = len(pnl)
    if n == 0:
        return {"n": 0}
    wins = [x for x in pnl if x > 0]
    losses = [x for x in pnl if x <= 0]
    best = max(range(n), key=lambda i: pnl[i])
    ex_best = pnl[:best] + pnl[best + 1:]

    eq, peak, mdd = 0.0, 0.0, 0.0
    under_since, under_total, under_longest = None, 0.0, 0.0
    for t, x in zip(trades, pnl):
        eq += x
        if eq >= peak - 1e-12:
            if under_since is not None:
                d = t["close_ts"] - under_since
                under_total += d
                under_longest = max(under_longest, d)
                under_since = None
            peak = max(peak, eq)
        else:
            if under_since is None:
                under_since = t["close_ts"]
            mdd = max(mdd, peak - eq)
    still_under = under_since is not None
    if still_under:
        d = (now or trades[-1]["close_ts"]) - under_since
        under_total += d
        under_longest = max(under_longest, d)
    streak = longest = 0
    for x in pnl:
        streak = streak + 1 if x <= 0 else 0
        longest = max(longest, streak)
    mix = {}
    for t in trades:
        mix[t["exit_reason_final"]] = mix.get(t["exit_reason_final"], 0) + 1
    daily = {}
    for t, x in zip(trades, pnl):
        d = datetime.fromtimestamp(t["close_ts"], timezone.utc).strftime("%Y-%m-%d")
        daily[d] = daily.get(d, 0.0) + x
    dvals = list(daily.values())
    sharpe_d = sortino_d = None
    if len(dvals) >= 5:
        sd = statistics.pstdev(dvals)
        sharpe_d = statistics.mean(dvals) / sd if sd > 0 else None
        down = [min(0.0, v) for v in dvals]
        dd = (sum(v * v for v in down) / len(down)) ** 0.5
        sortino_d = statistics.mean(dvals) / dd if dd > 0 else None
    total = sum(pnl)
    return {
        "n": n, "expectancy_sol": total / n, "expectancy_r": (total / n) / one_r if one_r else None,
        "profit_factor": _pf(pnl), "profit_factor_ex_best": _pf(ex_best) if ex_best else None,
        "best_trade_sol": pnl[best],
        "payoff_ratio": (statistics.mean(wins) / -statistics.mean(losses)) if wins and losses and statistics.mean(losses) < 0 else None,
        "max_drawdown_sol": mdd, "time_underwater_h": under_total / 3600, "longest_underwater_h": under_longest / 3600,
        "still_underwater": still_under, "longest_losing_streak": longest, "exit_mix": mix,
        "win_rate": len(wins) / n, "mean_sol": total / n, "median_sol": statistics.median(pnl), "total_sol": total,
        "avg_win": statistics.mean(wins) if wins else None, "avg_loss": statistics.mean(losses) if losses else None,
        "recovery_factor": (total / mdd) if mdd > 0 else None, "daily_pnl": daily,
        "sharpe_daily": sharpe_d, "sortino_daily": sortino_d,
    }


def fmt_pf(x):
    if x is None:
        return "n/a"
    return "inf (no losses)" if x == float("inf") else f"{x:.2f}"


def sample_note(n):
    if n < 30:
        return f"{n} trades: far too few. Ignore Sharpe; expectancy is a rumor (needs 100+)."
    if n < 100:
        return f"{n} trades: expectancy is still a rumor (needs 100+ after costs)."
    return f"{n} trades."
