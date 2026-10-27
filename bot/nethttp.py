import threading

import requests

CONNECT_TIMEOUT = 5.0


class HardTimeout(requests.Timeout):
    pass


def fresh_session(old=None):
    s = requests.Session()
    if old is not None:
        s.headers.update(old.headers)
    return s


def request(session, method, url, read_timeout=20.0, total=None, connect_timeout=CONNECT_TIMEOUT, **kw):
    total = total or (connect_timeout + read_timeout + 5.0)
    box = {}

    def run():
        try:
            box["r"] = session.request(method, url, timeout=(connect_timeout, read_timeout), **kw)
        except BaseException as e:
            box["e"] = e

    t = threading.Thread(target=run, daemon=True, name="http")
    t.start()
    t.join(total)
    if t.is_alive():
        raise HardTimeout(f"no complete response within {total:.0f}s")
    if "e" in box:
        raise box["e"]
    return box["r"]
