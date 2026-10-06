"""Pump.fun callout copier -- PAPER ONLY.

Watches five callers' callouts on pump.fun. When one posts a new callout it
records a paper buy at the real price a moment later, then sells by our own
rules. It holds no key and contains no code that can send a transaction.

    .venv\\Scripts\\python.exe bot.py           run
    .venv\\Scripts\\python.exe bot.py report    results so far

Why callouts and not the callers' wallet buys: 84% of their buys are never
called and those lose; the callout is what moves the price (see the study in
the Obsidian vault, robinhood-bot AD-050).
"""
import json
import logging
import math
import os
import statistics
import sys
import time
from logging.handlers import RotatingFileHandler

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, "state")
API = "https://frontend-api-v3.pump.fun"
SOL = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"

CALLERS = {  # pump.fun profile id -> the callout feed is keyed by it
    "marcell": "240b8242-5715-492e-828c-1622aac53376",
    "ily": "e77e7b24-34ee-4a1e-9966-3fa8c27f46e8",
    "schoen": "cd23adcb-133e-4614-9493-535dc5c3c85a",
    "pxblocito": "fb7b35f3-7815-4e7b-8985-a7d96cc403d7",
    "SoloFNF": "d18fbf14-83bd-4331-acfe-920c81096899",
}

STAKE_USD = float(os.getenv("STAKE_USD", "100"))
COST = float(os.getenv("COST_EACH_WAY", "0.02"))   # pump.fun fee ~1.2% + price impact
TIP_USD = float(os.getenv("TIP_USD", "1.0"))       # priority fee / Jito tip per transaction
EXEC_DELAY_S = 2         # our buy would land ~2 s after we see the callout
MAX_CALLOUT_AGE_S = 60   # older than this when first seen (e.g. after a restart): not bought
MIN_GAP_S = 1.1          # pump.fun allows about 60 requests a minute per address
PRICE_EVERY_S = 3        # how often an open position is re-priced
LIMIT_MIN = 60           # anything still held after this is sold at the market

RULES = {  # name: ([(fraction to sell, at this multiple of our fill)], stop multiple)
    "main: 80% at +20%, 20% rides to 2x": ([(0.8, 1.2), (0.2, 2.0)], 0.7),
    "all at +10%": ([(1.0, 1.1)], 0.7),
    "50% at +20%, 50% at 2x": ([(0.5, 1.2), (0.5, 2.0)], 0.7),
    "all at 2x": ([(1.0, 2.0)], 0.7),
}

log = logging.getLogger("callout-bot")


class Api:
    """Paced pump.fun GETs. A 429 pauses every request, doubling up to 5 minutes."""

    def __init__(self, session=None, clock=time.time, sleep=time.sleep):
        self.s = session or requests.Session()
        self.clock, self.sleep = clock, sleep
        self.last, self.blocked_until, self.backoff = 0.0, 0.0, 0.0

    def get(self, path, params):
        wait = max(self.last + MIN_GAP_S, self.blocked_until) - self.clock()
        if wait > 0:
            self.sleep(wait)
        self.last = self.clock()
        try:
            r = self.s.get(API + path, params=params, timeout=15,
                           headers={"User-Agent": "Mozilla/5.0", "Origin": "https://pump.fun"})
        except requests.RequestException as exc:
            log.warning("request failed (%s): %s", type(exc).__name__, path)
            return None
        if r.status_code == 429:
            self.backoff = min(300.0, max(15.0, self.backoff * 2))
            self.blocked_until = self.clock() + self.backoff
            log.warning("rate limited by pump.fun; pausing all requests %.0fs", self.backoff)
            return None
        self.backoff = 0.0
        if r.status_code != 200:
            log.warning("HTTP %s for %s: %s", r.status_code, path, r.text[:120])
            return None
        try:
            return r.json()
        except ValueError:
            log.warning("unreadable response for %s", path)
            return None


# ---- state -----------------------------------------------------------------------

def load():
    try:
        with open(os.path.join(STATE_DIR, "state.json"), encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"seen": {}, "positions": {}}


def save(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = os.path.join(STATE_DIR, "state.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, os.path.join(STATE_DIR, "state.json"))   # never a half-written file


def record(trade):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, "trades.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(trade) + "\n")


# ---- callouts --------------------------------------------------------------------

def poll(api, state, caller, now_ms):
    """New, fresh, Solana callouts from one caller. Each callout is considered once."""
    d = api.get(f"/callout/list/{CALLERS[caller]}", {"limit": 3, "sortBy": "TIMESTAMP", "sortOrder": "DESC"})
    out = []
    for c in (d or {}).get("callouts") or []:
        if c["calloutId"] in state["seen"]:
            continue
        state["seen"][c["calloutId"]] = c["createdAt"]
        age = (now_ms - c["createdAt"]) / 1000
        if age > MAX_CALLOUT_AGE_S:
            log.info("%s callout %s is %.0fs old; not buying", caller, c["coinMint"], age)
        elif c["coinMint"].startswith("0x"):
            log.info("%s called EVM coin %s; only Solana coins are tracked", caller, c["coinMint"])
        else:
            out.append(c)
    return out


def new_position(caller, c, now_ms):
    return {"id": c["calloutId"], "caller": caller, "mint": c["coinMint"], "callout_ms": c["createdAt"],
            "seen_ms": now_ms, "mcap": c.get("marketCap"), "entry": None, "tokens": None,
            "last_bar_ms": None, "last_close": None, "checked_ms": 0,
            "rules": {name: {"left": 1.0, "hit": [False] * len(legs), "proceeds": 0.0, "sells": 0, "done": False}
                      for name, (legs, _) in RULES.items()}}


# ---- prices and exits ------------------------------------------------------------

def bars(api, mint, to_ms, count, res="1s"):
    d = api.get(f"/candles/{SOL}/{mint}", {"res": res, "measure": "price_usd", "to": to_ms, "count": count})
    width = 1000 if res == "1s" else 60_000
    return [(b["openTimeMs"], width, float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
             b.get("final", True)) for b in (d or {}).get("candles") or []]


def fill_price(bs, seen_ms, landed_ms):
    """What our buy pays: the highest trade between seeing the callout and landing."""
    window = [h for (t, w, o, h, l, c, f) in bs if seen_ms - w < t <= landed_ms]
    if window:
        return max(window)
    before = [c for (t, w, o, h, l, c, f) in bs if t <= landed_ms]
    return before[-1] if before else None


def sell(rule, frac, price, tokens):
    rule["proceeds"] += frac * tokens * price * (1 - COST)
    rule["left"] -= frac
    rule["sells"] += 1


def apply_bar(pos, bar):
    """One price bar against every rule. A bar that touches both the stop and a
    target is taken as the stop (we cannot know the order inside a bar)."""
    t, w, o, h, l, c, _ = bar
    entry, tokens = pos["entry"], pos["tokens"]
    for name, (legs, stop) in RULES.items():
        r = pos["rules"][name]
        if r["done"]:
            continue
        if t >= pos["entry_ms"] + LIMIT_MIN * 60_000:
            sell(r, r["left"], c, tokens)
        elif o <= entry * stop:
            sell(r, r["left"], o, tokens)
        elif l <= entry * stop:
            sell(r, r["left"], entry * stop, tokens)
        else:
            for i, (frac, tgt) in enumerate(legs):
                if not r["hit"][i] and h >= entry * tgt:
                    r["hit"][i] = True
                    sell(r, min(frac, r["left"]), max(entry * tgt, o), tokens)
        if r["left"] <= 1e-9:
            r["left"], r["done"] = 0.0, True


def pnl(rule):
    return rule["proceeds"] - STAKE_USD - TIP_USD * (1 + rule["sells"])


def update(api, state, pos, now_ms):
    pos["checked_ms"] = now_ms
    if pos["entry"] is None:                      # the paper buy
        landed = pos["seen_ms"] + EXEC_DELAY_S * 1000
        bs = bars(api, pos["mint"], now_ms, 15)
        price = fill_price(bs, pos["seen_ms"], landed)
        if price is None:
            if now_ms - pos["seen_ms"] > 30_000:
                log.warning("no price for %s after 30s; dropping it", pos["mint"])
                del state["positions"][pos["id"]]
            return
        pos.update(entry=price, entry_ms=landed, tokens=STAKE_USD * (1 - COST) / price, last_close=price,
                   last_bar_ms=max([b[0] for b in bs if b[0] <= landed] or [landed - 1000]))
        log.info("PAPER BUY $%.0f of %s (called by %s) at $%.3g; callout seen %.1fs after posting",
                 STAKE_USD, pos["mint"], pos["caller"], price, (pos["seen_ms"] - pos["callout_ms"]) / 1000)
        return
    gap_s = (now_ms - pos["last_bar_ms"]) / 1000
    bs = (bars(api, pos["mint"], now_ms, min(300, math.ceil(gap_s / 60) + 1), "1m") if gap_s > 290
          else bars(api, pos["mint"], now_ms, min(300, math.ceil(gap_s) + 2)))
    for b in bs:
        if b[0] > pos["last_bar_ms"] and b[6]:   # final bars only: a forming bar can still move
            apply_bar(pos, b)
            pos["last_bar_ms"], pos["last_close"] = b[0], b[5]
    if now_ms >= pos["entry_ms"] + (LIMIT_MIN + 1) * 60_000:
        # No trades means no bars, and a rule that never sees a bar never times
        # out: sell what is left at the last price anyone paid.
        for r in pos["rules"].values():
            if not r["done"]:
                sell(r, r["left"], pos["last_close"], pos["tokens"])
                r["left"], r["done"] = 0.0, True
    if all(r["done"] for r in pos["rules"].values()):
        trade = {"id": pos["id"], "caller": pos["caller"], "mint": pos["mint"], "mcap": pos["mcap"],
                 "callout_ms": pos["callout_ms"], "seen_ms": pos["seen_ms"], "entry": pos["entry"],
                 "pnl": {name: round(pnl(r), 2) for name, r in pos["rules"].items()}}
        record(trade)
        del state["positions"][pos["id"]]
        log.info("CLOSED %s (%s): %s", pos["mint"], pos["caller"],
                 " | ".join(f"{k}: {v:+.2f}" for k, v in trade["pnl"].items()))


def due(pos, now_ms):
    if pos["entry"] is None:
        return now_ms >= pos["seen_ms"] + (EXEC_DELAY_S + 1) * 1000
    return now_ms - pos["checked_ms"] >= PRICE_EVERY_S * 1000


def step(api, state, turn, now_ms):
    """One request: a due position, or the next caller's callouts, alternating so
    open positions never starve the callout watch."""
    waiting = [p for p in state["positions"].values() if due(p, now_ms)]
    if waiting and turn % 2:
        update(api, state, min(waiting, key=lambda p: p["checked_ms"]), now_ms)
        return
    i = state.get("next_caller", 0)
    state["next_caller"] = (i + 1) % len(CALLERS)
    caller = list(CALLERS)[i % len(CALLERS)]
    for c in poll(api, state, caller, now_ms):
        state["positions"][c["calloutId"]] = new_position(caller, c, now_ms)
        log.info("CALLOUT %s -> %s (mcap $%s), seen %.1fs after posting", caller, c["coinMint"],
                 f"{c.get('marketCap') or 0:,.0f}", (now_ms - c["createdAt"]) / 1000)
    week_ago = now_ms - 7 * 86_400_000
    state["seen"] = {k: v for k, v in state["seen"].items() if v >= week_ago}


def run():
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    handlers = [logging.StreamHandler(sys.stdout),
                RotatingFileHandler(os.path.join(HERE, "logs", "bot.log"), maxBytes=5_000_000, backupCount=3,
                                    encoding="utf-8")]
    logging.basicConfig(level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s")
    log.info("PAPER MODE: no wallet, no keys, nothing is ever sent. $%.0f a trade, %d callers, rules: %s",
             STAKE_USD, len(CALLERS), "; ".join(RULES))
    api, state, turn = Api(), load(), 0
    try:
        while True:
            try:
                step(api, state, turn, int(time.time() * 1000))
            except Exception:   # logged with its traceback; paper positions stay saved
                log.exception("step failed; retrying in 5s")
                time.sleep(5)
            save(state)
            turn += 1
    except KeyboardInterrupt:
        save(state)
        log.info("stopped; %d open paper positions saved", len(state["positions"]))


def report():
    path = os.path.join(STATE_DIR, "trades.jsonl")
    trades = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            trades = [json.loads(line) for line in f]
    print(f"{len(trades)} closed paper trades, {len(load()['positions'])} open, ${STAKE_USD:.0f} each\n")
    for name in RULES:
        xs = [t["pnl"][name] for t in trades if name in t["pnl"]]
        if xs:
            print(f"  {name:<36} wins {sum(x > 0 for x in xs):>3}/{len(xs):<3}  total {sum(xs):+9.2f}  "
                  f"avg {sum(xs) / len(xs):+7.2f}")
    lag = sorted((t["seen_ms"] - t["callout_ms"]) / 1000 for t in trades)
    if lag:
        print(f"\n  callout seen after posting: median {statistics.median(lag):.1f}s, "
              f"p95 {lag[int(0.95 * (len(lag) - 1))]:.1f}s, max {lag[-1]:.1f}s")


if __name__ == "__main__":
    report() if sys.argv[1:] == ["report"] else run()
