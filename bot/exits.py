

def new_state(entry_ts):
    return {"entry_ts": entry_ts, "remaining": 1.0, "peak": 1.0, "tp1": False, "tp2": False}


def moonbag_frac(ex):
    return float(ex.get("moonbag_hold_frac") or 0.0)


def in_moonbag(state, ex):
    h = moonbag_frac(ex)
    return bool(h > 0 and state.get("tp2") and 1e-9 < state["remaining"] <= h + 1e-9)


def step(state, mult, now, ex):
    s = dict(state)
    actions = []
    if s["remaining"] <= 1e-9:
        return actions, s
    s["peak"] = max(s["peak"], mult)
    minutes = (now - s["entry_ts"]) / 60.0

    def sell(reason, frac):
        frac = min(frac, s["remaining"])
        if frac > 1e-9:
            actions.append({"reason": reason, "frac": frac, "mult": mult})
            s["remaining"] = round(s["remaining"] - frac, 10)

    hold = moonbag_frac(ex)
    if in_moonbag(s, ex):
        if mult <= float(ex.get("moonbag_rug_mult", 0.05)):
            sell("moonbag_rug", s["remaining"])
        elif minutes >= float(ex.get("moonbag_max_hours", 24)) * 60:
            sell("moonbag_24h", s["remaining"])
        return actions, s

    def sell_rest(reason):
        keep = hold if (hold > 0 and s["tp2"]) else 0.0
        sell(reason, s["remaining"] - keep)


    if not s["tp1"]:
        if ex.get("early_stop_pct") is not None and minutes < float(ex.get("early_stop_minutes") or 0):
            if mult <= 1 - float(ex["early_stop_pct"]) / 100.0:
                sell("early_stop", s["remaining"])
                return actions, s
        elif mult <= 1 - ex["hard_stop_pct"] / 100.0:
            sell("hard_stop", s["remaining"])
            return actions, s

    if ex.get("early_check_seconds") is not None and not s.get("early_checked") and minutes * 60 >= float(ex["early_check_seconds"]):
        s["early_checked"] = True
        if not s["tp1"] and mult <= float(ex.get("early_check_max_mult", 1.0)):
            sell("early_check", s["remaining"])
            return actions, s


    if not s["tp1"] and mult >= ex["tp1_multiple"]:
        sell("scale1", ex["tp1_sell_frac"])
        s["tp1"] = True
    if s["tp1"] and not s["tp2"] and mult >= ex["tp2_multiple"]:
        sell("scale2", ex["tp2_sell_frac"])
        s["tp2"] = True


    if ex.get("tp3_multiple") and s["tp2"] and mult >= ex["tp3_multiple"]:
        sell("scale3", s["remaining"])
        return actions, s


    trail_active = s["tp1"] or not ex.get("moonbag_requires_tp1", True)
    if trail_active and s["remaining"] > 1e-9 and mult <= s["peak"] * (1 - ex["trail_pct"] / 100.0):
        sell_rest("trail")
        return actions, s


    if (not s["tp1"] and minutes >= ex["stall_minutes"]
            and mult < 1 + ex["stall_min_gain_pct"] / 100.0):
        sell("stall", s["remaining"])
        return actions, s


    if minutes >= ex["time_stop_minutes"]:
        sell_rest("time_stop")
    return actions, s


def proceeds_sol(size_sol, frac, mult, paper):
    fee = paper["fee_pct"] / 100.0
    slip = paper["slippage_pct"] / 100.0
    gross = size_sol * frac * mult * (1 - fee) ** 2 * (1 - slip) / (1 + slip)
    return gross - paper.get("priority_fee_sol", 0.0)


def mark_value(size_sol, frac, mult):
    return size_sol * frac * mult


def entry_cost(paper):
    return paper.get("priority_fee_sol", 0.0) + paper.get("token_account_deposit_sol", 0.0)


def sell_fails(trade_id, action_index, rate):
    if rate <= 0:
        return False
    h = (int(trade_id) * 2654435761 + action_index * 40503) % 10007
    return h / 10007.0 < rate


def run_path(path, ex, entry_ts=0.0):
    s = new_state(entry_ts)
    out = []
    for t, m in path:
        acts, s = step(s, m, entry_ts + t, ex)
        for a in acts:
            a["t"] = t
        out.extend(acts)
        if s["remaining"] <= 1e-9:
            break
    return out, s


def replay_trade(path, ex, paper, trade_id=0):
    size = paper["size_sol"]
    rate = paper.get("failed_sell_rate", 0.0)
    acts, s = run_path(path, ex)
    got = 0.0
    for k, a in enumerate(acts):
        m = a["mult"]
        if sell_fails(trade_id, k, rate):
            later = [mm for t, mm in path if t > a["t"]]
            m = later[0] if later else path[-1][1]
        got += proceeds_sol(size, a["frac"], m, paper)
    if s["remaining"] > 1e-9:
        got += proceeds_sol(size, s["remaining"], path[-1][1], paper)
    return got - size - entry_cost(paper)
