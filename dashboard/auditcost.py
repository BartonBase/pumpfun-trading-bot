"""Audited-cost PnL model used for display only (never used for trading decisions).

For a trade of size S SOL bought at entry bonding-curve market cap M0 (in SOL):
  pump.fun bonding-curve fee f = 1.25% each side (public pump.fun fee docs).
  Curve virtual reserves follow a constant product k = 30 SOL * 1,073,000,000 tokens,
  so vSol(M) = sqrt(M * k / 1e9).
  Buy price impact  ib = S / vSol(M0)
  Each sell of fraction q at multiple m: y = S*q*m, sell impact is = y / (vSol(M0*m) + y)
  proceeds = y * (1-f)*(1-ib) * (1-f)*(1-is) - P   (P = priority fee per transaction)
  Cost in = S + P. The token-account deposit is refunded when the account closes.
"""
import math

FEE = 0.0125
PRIO = 0.0005
K_OVER_SUPPLY = 30.0 * 1_073_000_000 / 1e9


def vsol(mcap_sol):
    return math.sqrt(max(mcap_sol, 1e-9) * K_OVER_SUPPLY)


def audited_pnl(size_sol, entry_mcap_sol, sells, fee=FEE, prio=PRIO):
    """sells: iterable of (frac_sold, multiple). Returns PnL in SOL, or None if inputs are missing."""
    if not size_sol or not entry_mcap_sol:
        return None
    ib = size_sol / vsol(entry_mcap_sol)
    got = 0.0
    for q, m in sells:
        if not q or m is None:
            continue
        y = size_sol * q * m
        is_ = y / (vsol(entry_mcap_sol * max(m, 1e-6)) + y)
        got += y * (1 - fee) * (1 - ib) * (1 - fee) * (1 - is_) - prio
    return got - (size_sol + prio)


def audited_open(size_sol, entry_mcap_sol, sells, remaining_frac, mult, fee=FEE, prio=PRIO):
    """Open position marked as if the remainder sold now at `mult`."""
    s = list(sells) + ([(remaining_frac, mult)] if remaining_frac else [])
    return audited_pnl(size_sol, entry_mcap_sol, s, fee, prio)
