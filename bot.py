"""Pump.fun callout copier -- PAPER ONLY.

Watches five callers' callouts on pump.fun. When one posts a new callout it
records a paper buy at the real price a moment later, then sells by our own
rules. It holds no key and contains no code that can send a transaction.

    .venv\\Scripts\\python.exe bot.py           run
    .venv\\Scripts\\python.exe bot.py report    results so far

Why callouts and not the callers' wallet buys: 84% of their buys are never
called; the callout is what moves the price (study in the Obsidian vault,
robinhood-bot AD-050).

Why it also watches their wallets: pump.fun allows only about 8-10 requests a
minute per address (a request every 1.1 s was blocked within 11 s and stayed
blocked for minutes; one every 7 s held up in long runs). Five callers asked in
turn would each be checked only every ~35 s, and the main rule needs the
callout within ~5 s. Callers buy a median 1.3 minutes before calling, so the bot
watches their wallets' SOL balances on-chain (the Helius websocket in .env) and
spends its pump.fun requests on a caller right after that caller spends SOL.
"""
import asyncio
import json
import logging
import math
import os
import statistics
import sys
import threading
import time
from logging.handlers import RotatingFileHandler

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, "state")
API = "https://frontend-api-v3.pump.fun"
SOL = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"

CALLERS = {  # name: (pump.fun profile id, which keys the callout feed; Solana wallet)
    "marcell": ("240b8242-5715-492e-828c-1622aac53376", "6vbrTLsWy76TxDQAUKLqZrPohQuq6EVaJgfGzR82paFa"),
    "ily": ("e77e7b24-34ee-4a1e-9966-3fa8c27f46e8", "AcoNeFQsTPYs7ZrH8RMWaxxGJTTQJJ4H5aTXmptaz5UK"),
    "schoen": ("cd23adcb-133e-4614-9493-535dc5c3c85a", "5hAgYC8TJCcEZV7LTXAzkTrm7YL29YXyQQJPCNrG84zM"),
    "pxblocito": ("fb7b35f3-7815-4e7b-8985-a7d96cc403d7", "7Mwof5tBvNPC6e1zwtHRQynqXcuDpqqbeY9vSZLW2Bv8"),
    "SoloFNF": ("d18fbf14-83bd-4331-acfe-920c81096899", "GccvEcZqMrcNdp3CarCasGdN5dUfdQ4wZ6iQsk5myRzX"),
}

STAKE_USD = float(os.getenv("STAKE_USD", "100"))
COST = float(os.getenv("COST_EACH_WAY", "0.02"))   # pump.fun fee ~1.2% + price impact
TIP_USD = float(os.getenv("TIP_USD", "1.0"))       # priority fee / Jito tip per transaction
EXEC_DELAY_S = 2         # our buy would land ~2 s after we see the callout
MAX_CALLOUT_AGE_S = 60   # older than this when first seen (e.g. after a restart): not bought
MIN_GAP_S = float(os.getenv("PUMP_MIN_GAP_S", "7"))   # one pump.fun request per this many seconds
SPEND_SOL = 0.05         # a wallet dropping more than this bought something; fees are far smaller,
                         # and all 32 buys before a callout in the study were 0.084 SOL or more
HOT_MIN = 10             # after a caller spends, check their callouts at full speed this long
OVERDUE_S = 180          # meanwhile every other caller is still checked at least this often
LIMIT_MIN = 60           # anything still held after this is sold at the market

RULES = {  # name: ([(fraction to sell, at this multiple of our fill)], stop multiple)
    "main: 80% at +20%, 20% rides to 2x": ([(0.8, 1.2), (0.2, 2.0)], 0.7),
    "all at +10%": ([(1.0, 1.1)], 0.7),
    "50% at +20%, 50% at 2x": ([(0.5, 1.2), (0.5, 2.0)], 0.7),
    "all at 2x": ([(1.0, 2.0)], 0.7),
}

log = logging.getLogger("callout-bot")
HOT = {}   # caller -> unix ms until which they are checked at full speed (set by the wallet watch)


class Api:
    """Paced pump.fun GETs. A 429 pauses every request, doubling up to 5 minutes."""

    def __init__(self, session=None, clock=time.time, sleep=time.sleep):
        self.s = session or requests.Session()
        self.clock, self.sleep = clock, sleep
        self.last, self.blocked_until, self.backoff = 0.0, 0.0, 0.0

    def wait(self):
        """Sleep until pump.fun may be asked again."""
        left = max(self.last + MIN_GAP_S, self.blocked_until) - self.clock()
        if left > 0:
            self.sleep(left)

    def get(self, path, params):
        self.wait()
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

    def now_ms(self):
        return int(self.clock() * 1000)


def load_env():
    """KEY=value lines from .env. The values hold an API key: never log them."""
    vals = {}
    try:
        with open(os.path.join(HERE, ".env"), encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return vals


# ---- state -----------------------------------------------------------------------

def load():
    try:
        with open(os.path.join(STATE_DIR, "state.json"), encoding="utf-8") as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {}
    for key in ("seen", "positions", "polled"):
        state.setdefault(key, {})
    return state


def save(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = os.path.join(STATE_DIR, "state.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, os.path.join(STATE_DIR, "state.json"))   # never a half-written file


def record(name, row):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, name), "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


# ---- callouts --------------------------------------------------------------------

def poll(api, state, caller):
    """New, fresh, Solana callouts from one caller. Each callout is considered once.
    The seen time is taken when the answer arrives: the request itself may have waited."""
    d = api.get(f"/callout/list/{CALLERS[caller][0]}", {"limit": 3, "sortBy": "TIMESTAMP", "sortOrder": "DESC"})
    now_ms = api.now_ms()
    state["polled"][caller] = now_ms
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
            c["_seen_ms"] = now_ms
            out.append(c)
    if len(state["seen"]) > 5000:   # by count, not age: an idle caller's old callouts must stay seen
        state["seen"] = dict(sorted(state["seen"].items(), key=lambda kv: kv[1])[-5000:])
    return out


def new_position(caller, c, seen_ms, hot):
    return {"id": c["calloutId"], "caller": caller, "mint": c["coinMint"], "callout_ms": c["createdAt"],
            "seen_ms": seen_ms, "hot": hot, "mcap": c.get("marketCap"), "entry": None, "tokens": None,
            "last_bar_ms": None, "last_close": None, "checked_ms": 0,
            "rules": {name: {"left": 1.0, "hit": [False] * len(legs), "proceeds": 0.0, "sells": 0, "done": False}
                      for name, (legs, _) in RULES.items()}}


# ---- prices and exits ------------------------------------------------------------

def bars(api, mint, to_ms, count, res="1s"):
    """The last `count` bars up to to_ms, oldest first. Only periods with trades
    have a bar, so 300 one-second bars can span 5 minutes or an hour."""
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
    """Price a position from its bars. Exits are scored after the fact, so a
    position needs a request only every few minutes: late is fine, as long as no
    second is skipped."""
    pos["checked_ms"] = now_ms
    bs = bars(api, pos["mint"], now_ms, 300)
    if pos["entry"] is None:                      # the paper buy
        landed = pos["seen_ms"] + EXEC_DELAY_S * 1000
        price = fill_price(bs, pos["seen_ms"], landed)
        if price is None:
            if now_ms - pos["seen_ms"] > 240_000:
                log.warning("no price for %s after 4 min; dropping it", pos["mint"])
                del state["positions"][pos["id"]]
            return
        pos.update(entry=price, entry_ms=landed, tokens=STAKE_USD * (1 - COST) / price, last_close=price,
                   last_bar_ms=max([b[0] for b in bs if b[0] <= landed] or [landed - 1000]))
        log.info("PAPER BUY $%.0f of %s (called by %s) at $%.3g; callout seen %.1fs after posting%s",
                 STAKE_USD, pos["mint"], pos["caller"], price, (pos["seen_ms"] - pos["callout_ms"]) / 1000,
                 " (early warning was on)" if pos.get("hot") else "")
    elif len(bs) == 300 and bs[0][0] > pos["last_bar_ms"] + 1000:
        # 300 busy seconds since the last look: minute bars instead, coarser but nothing skipped
        bs = bars(api, pos["mint"], now_ms, min(300, math.ceil((now_ms - pos["last_bar_ms"]) / 60_000) + 1), "1m")
    for b in bs:
        end = b[0] + b[1] - 1000   # a minute bar's last second
        if end > pos["last_bar_ms"] and b[6]:   # final bars only: a forming bar can still move
            apply_bar(pos, b)
            pos["last_bar_ms"], pos["last_close"] = end, b[5]
    if now_ms >= pos["entry_ms"] + (LIMIT_MIN + 1) * 60_000:
        # No trades means no bars, and a rule that never sees a bar never times
        # out: sell what is left at the last price anyone paid.
        for r in pos["rules"].values():
            if not r["done"]:
                sell(r, r["left"], pos["last_close"], pos["tokens"])
                r["left"], r["done"] = 0.0, True
    if all(r["done"] for r in pos["rules"].values()):
        trade = {"id": pos["id"], "caller": pos["caller"], "mint": pos["mint"], "mcap": pos["mcap"],
                 "callout_ms": pos["callout_ms"], "seen_ms": pos["seen_ms"], "hot": pos.get("hot", False),
                 "entry": pos["entry"], "pnl": {name: round(pnl(r), 2) for name, r in pos["rules"].items()}}
        record("trades.jsonl", trade)
        del state["positions"][pos["id"]]
        log.info("CLOSED %s (%s): %s", pos["mint"], pos["caller"],
                 " | ".join(f"{k}: {v:+.2f}" for k, v in trade["pnl"].items()))


# ---- what to spend the next request on --------------------------------------------

def pick(state, now_ms, hot):
    """The most useful next pump.fun request, in this order:
    1. a position about to lose data: a busy coin's last 300 one-second bars
       cover only ~5 minutes, and a paper buy must be priced within 4 minutes
    2. a caller not checked for OVERDUE_S
    3. a caller who just spent SOL on-chain (the early warning)
    4. a paper buy waiting for its price
    5. a position not checked for a minute
    6. otherwise, the caller checked longest ago"""
    def since(c):
        return now_ms - state["polled"].get(c, 0)
    held = [p for p in state["positions"].values() if p["entry"] is not None]
    waiting = [p for p in state["positions"].values() if p["entry"] is None
               and now_ms >= p["seen_ms"] + (EXEC_DELAY_S + 5) * 1000 and now_ms - p["checked_ms"] >= 10_000]
    urgent = ([p for p in held if now_ms - p["checked_ms"] >= 240_000]
              + [p for p in waiting if now_ms - p["seen_ms"] >= 120_000])
    if urgent:
        return "position", min(urgent, key=lambda p: p["checked_ms"])
    for group in ([c for c in CALLERS if since(c) >= OVERDUE_S * 1000], [c for c in CALLERS if hot.get(c, 0) > now_ms]):
        if group:
            return "caller", max(group, key=since)
    if waiting:
        return "position", waiting[0]
    stale = [p for p in held if now_ms - p["checked_ms"] >= 60_000]
    if stale:
        return "position", min(stale, key=lambda p: p["checked_ms"])
    return "caller", max(CALLERS, key=since)


def step(api, state, hot):
    """Wait for pump.fun's next free slot, then spend it on the most useful request."""
    api.wait()
    now_ms = api.now_ms()
    kind, x = pick(state, now_ms, hot)
    if kind == "position":
        update(api, state, x, now_ms)
        return
    was_hot = hot.get(x, 0) > now_ms
    for c in poll(api, state, x):
        state["positions"][c["calloutId"]] = new_position(x, c, c["_seen_ms"], was_hot)
        hot.pop(x, None)    # their callout is in: stop spending requests on them
        log.info("CALLOUT %s -> %s (mcap $%s), seen %.1fs after posting%s", x, c["coinMint"],
                 f"{c.get('marketCap') or 0:,.0f}", (c["_seen_ms"] - c["createdAt"]) / 1000,
                 " (early warning was on)" if was_hot else "")


# ---- the early warning: the callers' SOL balances (Helius websocket) ----------------

def spend(msg, by_sub, last):
    """(caller, SOL) when a balance notification shows a caller's wallet falling by
    more than SPEND_SOL, else None. Only a wallet's owner can lower its SOL balance,
    so this is always the caller's own doing. Updates `last`; the first balance seen
    for a caller only sets it."""
    if msg.get("method") != "accountNotification" or msg["params"]["subscription"] not in by_sub:
        return None
    caller = by_sub[msg["params"]["subscription"]]
    lamports = (msg["params"]["result"]["value"] or {}).get("lamports", 0)
    prev, last[caller] = last.get(caller), lamports
    if prev is not None and prev - lamports > SPEND_SOL * 1e9:
        return caller, (prev - lamports) / 1e9
    return None


async def watch(wss_url, hot):
    """Mark a caller hot when their wallet spends SOL. Watching their transaction logs
    instead was tried: other people's failed transactions mention schoen's wallet ~18
    times a second (~1 GB a day), and Helius bills websocket data at 2 credits per
    0.1 MB. Balances change only when money actually moves."""
    import websockets
    # Balances survive reconnects. ponytail: a caller's first balance change after the bot
    # starts only sets their baseline; seed `last` with getMultipleAccounts if that matters.
    last, delay = {}, 1
    while True:
        try:
            async with websockets.connect(wss_url, open_timeout=15, ping_interval=20) as ws:
                by_id, by_sub = {}, {}
                for i, (caller, (_, wallet)) in enumerate(CALLERS.items(), 1):
                    by_id[i] = caller
                    await ws.send(json.dumps({"jsonrpc": "2.0", "id": i, "method": "accountSubscribe",
                                              "params": [wallet, {"encoding": "base64", "commitment": "confirmed"}]}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("id") in by_id:   # a subscription reply
                        if "result" not in msg:
                            log.warning("wallet watch: %s not subscribed: %s", by_id[msg["id"]], msg.get("error"))
                            continue
                        by_sub[msg["result"]] = by_id[msg["id"]]
                        if len(by_sub) == len(CALLERS):
                            log.info("wallet watch on: %d wallets", len(by_sub))
                            delay = 1
                    elif got := spend(msg, by_sub, last):
                        caller, sol = got
                        now = int(time.time() * 1000)
                        hot[caller] = now + HOT_MIN * 60_000
                        record("spends.jsonl", {"caller": caller, "sol": round(sol, 4), "seen_ms": now,
                                                "slot": msg["params"]["result"]["context"]["slot"]})
                        log.info("EARLY WARNING: %s spent %.2f SOL on-chain; checking their callouts at full "
                                 "speed for %d min", caller, sol, HOT_MIN)
            why = "closed by the server"
        except Exception as exc:   # type only: an error message can hold the URL and key
            why = type(exc).__name__
        log.warning("wallet watch dropped (%s); reconnecting in %ds", why, delay)
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)


def run():
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    handlers = [logging.StreamHandler(sys.stdout),
                RotatingFileHandler(os.path.join(HERE, "logs", "bot.log"), maxBytes=5_000_000, backupCount=3,
                                    encoding="utf-8")]
    logging.basicConfig(level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("websockets").setLevel(logging.WARNING)
    log.info("PAPER MODE: no wallet, no keys, nothing is ever sent. $%.0f a trade, %d callers, one pump.fun "
             "request per %.0fs; rules: %s", STAKE_USD, len(CALLERS), MIN_GAP_S, "; ".join(RULES))
    wss = load_env().get("SOLANA_WSS_URL")
    if wss:
        threading.Thread(target=asyncio.run, args=(watch(wss, HOT),), name="wallet-watch", daemon=True).start()
    else:
        log.warning("no SOLANA_WSS_URL in .env: early warning OFF, callouts are seen late")
    api, state, beat = Api(), load(), 0.0
    try:
        while True:
            try:
                step(api, state, HOT)
            except Exception:   # logged with its traceback; paper positions stay saved
                log.exception("step failed; retrying in 5s")
                time.sleep(5)
            save(state)
            if time.time() - beat >= 900:   # a heartbeat, so a quiet log still shows the bot is alive
                beat = time.time()
                log.info("alive: %d open paper positions; early warning on for: %s", len(state["positions"]),
                         ", ".join(c for c, t in HOT.items() if t > beat * 1000) or "nobody")
    except KeyboardInterrupt:
        save(state)
        log.info("stopped; %d open paper positions saved", len(state["positions"]))


def report():
    trades = []
    path = os.path.join(STATE_DIR, "trades.jsonl")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            trades = [json.loads(line) for line in f]
    print(f"{len(trades)} closed paper trades, {len(load()['positions'])} open, ${STAKE_USD:.0f} each")
    fast = [t for t in trades if t["seen_ms"] - t["callout_ms"] <= 10_000]
    for label, group in (("callout seen within 10s", fast), ("callout seen later", [t for t in trades if t not in fast])):
        if group:
            print(f"\n{label}: {len(group)} trades")
        for name in RULES:
            xs = [t["pnl"][name] for t in group if name in t["pnl"]]
            if xs:
                print(f"  {name:<36} wins {sum(x > 0 for x in xs):>3}/{len(xs):<3}  total {sum(xs):+9.2f}  "
                      f"avg {sum(xs) / len(xs):+7.2f}")
    for label, group in (("with early warning", [t for t in trades if t.get("hot")]),
                         ("without", [t for t in trades if not t.get("hot")])):
        lag = sorted((t["seen_ms"] - t["callout_ms"]) / 1000 for t in group)
        if lag:
            print(f"\ncallout seen after posting, {label}: median {statistics.median(lag):.1f}s, "
                  f"p95 {lag[int(0.95 * (len(lag) - 1))]:.1f}s, max {lag[-1]:.1f}s  (n={len(lag)})")


if __name__ == "__main__":
    report() if sys.argv[1:] == ["report"] else run()
