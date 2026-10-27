import hashlib
import struct

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return (b"\x00" * pad + raw).rjust(32, b"\x00") if len(s) >= 32 else b"\x00" * pad + raw


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(b) - len(b.lstrip(b"\x00"))
    return "1" * pad + out



_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P


def _on_curve(b):
    y = int.from_bytes(b, "little") & ((1 << 255) - 1)
    if y >= _P:
        return False
    sign = b[31] >> 7
    y2 = y * y % _P
    u, v = (y2 - 1) % _P, (_D * y2 + 1) % _P
    x2 = u * pow(v, _P - 2, _P) % _P
    if x2 == 0:
        return sign == 0
    x = pow(x2, (_P + 3) // 8, _P)
    if (x * x - x2) % _P != 0:
        x = x * pow(2, (_P - 1) // 4, _P) % _P
    if (x * x - x2) % _P != 0:
        return False
    return True


def find_pda(seeds, program_b):
    for bump in range(255, -1, -1):
        h = hashlib.sha256(b"".join(seeds) + bytes([bump]) + program_b + b"ProgramDerivedAddress").digest()
        if not _on_curve(h):
            return h
    raise ValueError("no PDA")


PUMP_PROGRAM = b58decode("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
PUMP_AMM = b58decode("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
WSOL_MINT = b58decode("So11111111111111111111111111111111111111112")
DISC_POOL = bytes([241, 154, 109, 4, 17, 177, 109, 188])


def canonical_pool(mint_str, quote_mint_str=None):
    mint = b58decode(mint_str)
    auth = find_pda([b"pool-authority", mint], PUMP_PROGRAM)
    q = b58decode(quote_mint_str) if quote_mint_str else WSOL_MINT
    return b58encode(find_pda([b"pool", struct.pack("<H", 0), auth, mint, q], PUMP_AMM))


def decode_pool(raw):
    if len(raw) < 211 or raw[:8] != DISC_POOL:
        raise ValueError("not a PumpSwap pool account")
    vq = 0
    if len(raw) >= 261:
        vq = int.from_bytes(raw[245:261], "little", signed=True)
    return {"base_mint": b58encode(raw[43:75]), "quote_mint": b58encode(raw[75:107]),
            "pool_base_token_account": b58encode(raw[139:171]), "pool_quote_token_account": b58encode(raw[171:203]),
            "virtual_quote_reserves": vq}


def token_amount(raw):
    if len(raw) < 72:
        raise ValueError("not a token account")
    return struct.unpack_from("<Q", raw, 64)[0]


PUMP_AMM_STR = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
WSOL_STR = "So11111111111111111111111111111111111111112"
