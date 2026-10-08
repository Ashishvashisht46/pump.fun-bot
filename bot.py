"""Pump.fun callout copier -- PAPER ONLY.

Watches our callers' callouts on pump.fun (CALLERS). When one posts a new callout it
records a paper buy at the real price a moment later, then sells by our own
rules. It holds no key and contains no code that can send a transaction.

    .venv\\Scripts\\python.exe bot.py           run
    .venv\\Scripts\\python.exe bot.py report    results so far
    .venv\\Scripts\\python.exe bot.py history   every trade: buy, sells, fees (also state/history.csv)

Why callouts and not the callers' wallet buys: 84% of their buys are never
called; the callout is what moves the price (study in the Obsidian vault,
robinhood-bot AD-050). The main rule needs the callout within ~5 s. A paper track
that bought every coin they bought (2026-10-07/08, commits a022218-fc6ee95) lost
-$1,808 over 148 trades and was removed; its trades stay in state/wallet_trades.jsonl.

How it sees a callout within ~2 s:
- PUSH: pump.fun's website gets every new callout pushed over its public NATS
  websocket (homeFeedNew.head.solana.prod, with the public 'subscriber' login that
  is in the homepage HTML). Measured over 179 callouts on 2026-10-06: they arrived
  0.4-2.9 s after posting, median 1.4 s. A pushed row names the coin, not the caller.
- EARLY WARNING: the callers' SOL balances (Helius websocket in .env). A drop over
  0.05 SOL means the caller bought; one RPC lookup names the coin.
- FEED: GET /home-feed/new lists the newest callouts site-wide WITH their callers,
  and is fresh. A pushed callout on a coin one of our callers just bought makes the
  bot ask the feed at once; a position opens only when the feed shows the callout is
  theirs. (Taking the push alone opened 94 positions on strangers' callouts in an
  hour: copy-traders mirror these callers' buys, at the same size, and call the same
  coins.) The feed is also polled every 20 s for callouts the push alarm misses.
  pump.fun allows ~8-10 requests a minute; the bot makes at most 7, 3 s apart, so a
  confirmation usually goes out at once. The per-caller /callout/list showed a
  callout ~38 s late and is no longer used.
- EVM: marcell and ily also call coins on EVM chains (marcell's are BSC coins from
  the Flap launchpad, addresses ending 7777). The feed covers every chain and prices
  come from the same API by chain id. There is no EVM wallet watch, so the push alarm
  for EVM is a big position instead: an EVM callout with $300+ behind it makes the
  bot ask the feed at once; smaller ones wait for the 20 s poll.
"""
import asyncio
import collections
import csv
import json
import logging
import math
import os
import queue
import re
import statistics
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, "state")
API = "https://frontend-api-v3.pump.fun"
SOL = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
WSOL = "So11111111111111111111111111111111111111112"
CHAINS = {  # pump.fun's chain names -> the CAIP-2 ids its price API takes (from its frontend)
    "solana": SOL, "ethereum": "eip155:1", "base": "eip155:8453", "bsc": "eip155:56",
    "robinhood": "eip155:4663", "hyperevm": "eip155:999", "arc": "eip155:5042",
}
PUSH_SUBJECTS = ("homeFeedNew.head.solana.prod",   # Solana callouts
                 "homeFeedNew.head.all.prod")      # every chain: adds the EVM ones, ~0.3 s behind
PUSH_SILENCE_S = 60      # a snapshot comes every ~2 s, a heartbeat every ~30 s: this long without one means a
                         # dead subscription (seen twice: a fresh one got nothing for minutes while the server's
                         # PINGs kept the connection looking alive)

CALLERS = {  # name: (pump.fun profile id, Solana wallet)
    "marcell": ("240b8242-5715-492e-828c-1622aac53376", "6vbrTLsWy76TxDQAUKLqZrPohQuq6EVaJgfGzR82paFa"),
    "ily": ("e77e7b24-34ee-4a1e-9966-3fa8c27f46e8", "AcoNeFQsTPYs7ZrH8RMWaxxGJTTQJJ4H5aTXmptaz5UK"),
    "schoen": ("cd23adcb-133e-4614-9493-535dc5c3c85a", "5hAgYC8TJCcEZV7LTXAzkTrm7YL29YXyQQJPCNrG84zM"),
    "pxblocito": ("fb7b35f3-7815-4e7b-8985-a7d96cc403d7", "7Mwof5tBvNPC6e1zwtHRQynqXcuDpqqbeY9vSZLW2Bv8"),
    "SoloFNF": ("d18fbf14-83bd-4331-acfe-920c81096899", "GccvEcZqMrcNdp3CarCasGdN5dUfdQ4wZ6iQsk5myRzX"),
    # added 2026-10-07 from Ashish's invite links (join.pump.fun -> pump.fun/profile/<id>; wallet from the page,
    # checked against the profile's own callout list)
    "spuno": ("161a6705-a007-4151-811f-c21a71733360", "GfXQesPe3Zuwg8JhAt6Cg8euJDTVx751enp9EQQmhzPH"),
    "untaxxable": ("c262a0de-4e69-4c2b-be93-85b9eef4231e", "2T5NgDDidkvhJQg8AHDi74uCFwgp25pYFMRZXBaCUNBH"),
    "KayThedoc": ("fd1bf406-c7b4-4d7f-910a-54537f4813ad", "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt"),
    "mitch": ("b8f1ac45-55b5-4704-919a-fb7105e59dc5", "4Be9CvxqHW6BYiRAxW9Q3xu1ycTMWaL5z8NX4HR3ha7t"),
    "FlippingProfits": ("76129303-d90d-45f9-ae69-8c749505f90d", "DdM1tyCdoEyoxYYmGMjdf5rRPcpmj3UzZTpE7ScuTf7d"),
    "sapphy": ("f345034e-684f-47b5-ab1f-7ba6c81464d3", "B5TrL7PsRUwCTL5bVcueieht5QchTw4TWLQDKqEaxEzi"),
    "hexiecs": ("046999d1-1609-4654-b8ed-06827aa50ae9", "21rgbFW6sujQovCw3qt6R2EdE97Yzzvk8sSc37Bb72Cm"),
    "0xEthan": ("36bd4b6f-f409-4be8-a062-5058f5b23bfb", "6yVb4pxNwDfr6rovwNnBg3SyKSvDcHGD4WdFPN1JJBqm"),
    "croakie": ("09d6f634-beb7-436e-9e41-7b9061f9673f", "7fEXteaTtmX1uR8fpChEXsevM4icH5vq8LNL9dzDupX2"),
    "J777Crypto": ("1e2df2f0-4720-4edb-a6d2-dd2833300de9", "4UrFSCrGxgoCtCUBAEZq7ZmPK3Pczkxx7PwYnkBMi1KR"),
    "cooker": ("88126e6e-497b-4cf8-8bcc-fdacf422431f", "8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6"),
    "_logjam": ("bf9041db-31a9-4d3c-9bfa-7b9e0cde1512", "AH39BneW9UeWxQysUcwogQrYFdPvJuRVZ5w4ccq9SKfL"),
    "jackduvalcalls": ("cef4bed6-680b-491c-8089-1f3c6bfe763b", "BAr5csYtpWoNpwhUjixX7ZPHXkUciFZzjBp9uNxZXJPh"),
}
BY_PROFILE = {profile: name for name, (profile, _) in CALLERS.items()}

STAKE_USD = float(os.getenv("STAKE_USD", "100"))
COST = float(os.getenv("COST_EACH_WAY", "0.02"))   # pump.fun fee ~1.2% + price impact
TIP_USD = float(os.getenv("TIP_USD", "1.0"))       # priority fee / Jito tip per transaction
EXEC_DELAY_S = 2         # our buy would land ~2 s after we see the callout
MAX_CALLOUT_AGE_S = 60   # older than this when first seen (e.g. after a restart): not bought
MAX_PER_MIN = 7          # pump.fun requests in any 60 s: 1 per 7 s ran for hours without a 429...
MIN_GAP_S = float(os.getenv("PUMP_MIN_GAP_S", "3"))   # ...and these apart (1.1 s apart was blocked in 11 s)
FEED_EVERY_S = 20        # the site-wide feed is polled at least this often
SPEND_SOL = 0.05         # a wallet dropping more than this bought something; fees are far smaller,
                         # and all 32 buys before a callout in the study were 0.084 SOL or more
MINT_WATCH_MIN = 60      # a pushed callout on a coin a caller bought this recently may be theirs...
SIZE_BAND = (0.5, 2.0)   # ...if the row's position is this many times what they spent: worth asking the
                         # feed. Most strangers hold far less (median callout position $37), but copy-
                         # traders hold the same: 14 strangers on schoen's coins were 0.70-1.23x (2026-10-06)
EVM_ALARM_USD = 300      # no wallet watch on EVM chains, and ~5 EVM callouts a minute site-wide are too many
                         # to ask the feed about: only those with a position this big (top ~10% site-wide;
                         # marcell's Solana buys run in the thousands) get asked about at once
LIMIT_MIN = 60           # anything still held after this is sold at the market

RULES = {  # name: ([(fraction to sell, at this multiple of our fill)], stop multiple,
           #        stop multiple once a target has sold, trailing stop on the rest once a target has sold)
    # Per $100 trade on 58 past callouts, bought 3.4 s after posting (scratchpad candidates.py, 2026-10-07):
    # avg / win rate / max drawdown. All are near break-even; the main has the smallest drawdown and held
    # up best on the newest half (-0.65 vs -4.7..-6.4 for the others), which includes a night of dumps.
    "main: 50% at +30%, rest trails 30%, stop to 1.05x": ([(0.5, 1.3)], 0.7, 1.05, 0.3),       # +1.01 55% 199
    "50% at +30%, 50% rides to 2x": ([(0.5, 1.3), (0.5, 2.0)], 0.7, None, None),               # +0.52 34% 272
    "old main: 80% at +20%, 20% rides to 2x": ([(0.8, 1.2), (0.2, 2.0)], 0.7, None, None),     # -2.55 62% 314
    "50% at +20%, 50% at 2x": ([(0.5, 1.2), (0.5, 2.0)], 0.7, None, None),                     # -0.79 36% 323
    "all at 2x": ([(1.0, 2.0)], 0.7, None, None),                                              # +2.73 29% 343
}
RETIRED = {  # still applied to positions opened before they were dropped or renamed
    "main: 80% at +20%, 20% rides to 2x": RULES["old main: 80% at +20%, 20% rides to 2x"],
    "all at +10%": ([(1.0, 1.1)], 0.7, None, None),                                                 # -2.57
}
RENAMED = {"main: 80% at +20%, 20% rides to 2x": "old main: 80% at +20%, 20% rides to 2x"}

log = logging.getLogger("callout-bot")
MINTS = {}                 # coin -> (caller, unix ms the watch ends, SOL they spent on it)
DETECTED = queue.Queue()   # (caller, callout, arrival ms) from the push thread to the main loop
PRICE = {}                 # "sol": USD, from pump.fun, refreshed every 10 min by the main loop
PENDING = {}               # main loop only: callout id -> (give-up ms, size ratio) awaiting the feed


class Api:
    """Rate-limited pump.fun GETs: at most MAX_PER_MIN in any 60 s, MIN_GAP_S apart.
    A 429 pauses every request, doubling up to 5 minutes."""

    def __init__(self, session=None, clock=time.time, sleep=time.sleep):
        self.s = session or requests.Session()
        self.clock, self.sleep = clock, sleep
        self.sent, self.blocked_until, self.backoff = collections.deque(), 0.0, 0.0

    def ready_at(self):
        now = self.clock()
        while self.sent and self.sent[0] <= now - 60:
            self.sent.popleft()
        return max(self.blocked_until, self.sent[-1] + MIN_GAP_S if self.sent else 0,
                   self.sent[0] + 60 if len(self.sent) >= MAX_PER_MIN else 0)

    def wait(self):
        """Sleep until pump.fun may be asked again."""
        left = self.ready_at() - self.clock()
        if left > 0:
            self.sleep(left)

    def get(self, path, params):
        self.wait()
        self.sent.append(self.clock())
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
    for key in ("seen", "positions"):
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

def remember(state, callout_id, created_ms):
    """Mark a callout as handled. Capped by count, not age: old ones must stay seen."""
    state["seen"][callout_id] = created_ms
    if len(state["seen"]) > 5000:
        state["seen"] = dict(sorted(state["seen"].items(), key=lambda kv: kv[1])[-5000:])


def poll_feed(api, state):
    """(caller, callout, via, seen ms) for new callouts by our callers in pump.fun's
    site-wide "New" feed, which lists every position-backed callout with its caller,
    newest first. via is "push" when the push alarm asked for this poll. It also
    settles every pending push alarm: found (ours or a stranger's), or given up."""
    d = api.get("/home-feed/new", {"pageSize": 50, "chain": "all"})
    now_ms = api.now_ms()
    state["feed_ms"] = now_ms
    out = []
    for row in (d or {}).get("coins") or []:
        pos = row.get("position") or {}
        c = pos.get("callout") or {}
        cid, caller = c.get("calloutId"), BY_PROFILE.get(pos.get("userId"))
        if not cid or not c.get("calloutTimestamp") or row.get("chain") not in CHAINS:
            continue
        alarm = PENDING.pop(cid, None)
        if caller is None or cid in state["seen"]:
            continue
        created = int(datetime.fromisoformat(c["calloutTimestamp"].replace("Z", "+00:00")).timestamp() * 1000)
        remember(state, cid, created)
        age = (now_ms - created) / 1000
        if age > MAX_CALLOUT_AGE_S:
            log.info("%s callout %s is %.0fs old; not buying", caller, row["coinMint"], age)
            continue
        out.append((caller, {"calloutId": cid, "coinMint": row["coinMint"], "createdAt": created,
                             "chain": CHAINS[row["chain"]], "marketCap": c.get("calledOutAtMcap"),
                             "size_ratio": alarm and alarm[1]}, "push" if alarm else "feed", now_ms))
    for cid, (give_up, _) in list(PENDING.items()):
        if give_up < now_ms:
            del PENDING[cid]
    return out


def take_pushed(state, now_ms):
    """Register the push alarms that came in since the last look; True if any did."""
    got = False
    while True:
        try:
            _, c, _ = DETECTED.get_nowait()
        except queue.Empty:
            return got
        if c["calloutId"] not in state["seen"]:
            PENDING[c["calloutId"]] = (now_ms + 15_000, c.get("size_ratio"))
            got = True


def new_position(caller, c, seen_ms, via):
    return {"id": c["calloutId"], "caller": caller, "mint": c["coinMint"], "chain": c.get("chain", SOL),
            "callout_ms": c["createdAt"], "seen_ms": seen_ms, "via": via, "author_ok": True,
            "size_ratio": c.get("size_ratio"),
            "mcap": c.get("marketCap"), "entry": None, "tokens": None,
            "last_bar_ms": None, "last_close": None, "checked_ms": 0,
            "rules": {name: {"left": 1.0, "hit": [False] * len(rule[0]), "proceeds": 0.0, "sells": 0, "done": False}
                      for name, rule in RULES.items()}}


# ---- prices and exits ------------------------------------------------------------

def bars(api, chain, mint, to_ms, count, res="1s"):
    """The last `count` bars up to to_ms, oldest first; None when there was no answer
    (unlike [], which means no trades). Only periods with trades have a bar, so 300
    one-second bars can span 5 minutes or an hour."""
    d = api.get(f"/candles/{chain}/{mint}", {"res": res, "measure": "price_usd", "to": to_ms, "count": count})
    width = 1000 if res == "1s" else 60_000
    return None if d is None else [
        (b["openTimeMs"], width, float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
         b.get("final", True)) for b in d.get("candles") or []]


def fill_price(bs, seen_ms, landed_ms):
    """What our buy pays: the highest trade between seeing the callout and landing."""
    window = [h for (t, w, o, h, l, c, f) in bs if seen_ms - w < t <= landed_ms]
    if window:
        return max(window)
    before = [c for (t, w, o, h, l, c, f) in bs if t <= landed_ms]
    return before[-1] if before else None


def sell(rule, frac, price, tokens, t_ms, why):
    rule["proceeds"] += frac * tokens * price * (1 - COST)
    rule["left"] -= frac
    rule["sells"] += 1
    rule.setdefault("fills", []).append([t_ms, round(frac, 6), price, why])


def apply_bar(pos, bar):
    """One price bar against every rule. A bar that touches both the stop and a
    target is taken as the stop (we cannot know the order inside a bar); a stop
    raised by a target, and a trailing stop, take effect from the next bar."""
    t, w, o, h, l, c, _ = bar
    entry, tokens = pos["entry"], pos["tokens"]
    for name, r in pos["rules"].items():
        if r["done"]:
            continue
        legs, stop, after, trail = {**RETIRED, **RULES}[name]
        level = entry * r.get("stop", stop)
        why = f"stop {r.get('stop', stop):g}x"
        if trail and any(r["hit"]) and r.get("peak", entry) * (1 - trail) > level:
            level, why = r.get("peak", entry) * (1 - trail), f"trailing stop {trail:.0%} below the peak"
        if t >= pos["entry_ms"] + LIMIT_MIN * 60_000:
            sell(r, r["left"], c, tokens, t, f"{LIMIT_MIN}-min limit")
        elif o <= level:
            sell(r, r["left"], o, tokens, t, why + " (opened below it)")
        elif l <= level:
            sell(r, r["left"], level, tokens, t, why)
        else:
            for i, (frac, tgt) in enumerate(legs):
                if not r["hit"][i] and h >= entry * tgt:
                    r["hit"][i] = True
                    sell(r, min(frac, r["left"]), max(entry * tgt, o), tokens, t, f"target {tgt:g}x")
                    if after:
                        r["stop"] = max(r.get("stop", stop), after)
        r["peak"] = max(r.get("peak", entry), h)
        if r["left"] <= 1e-9:
            r["left"], r["done"] = 0.0, True


def pnl(rule):
    return rule["proceeds"] - STAKE_USD - TIP_USD * (1 + rule["sells"])


def settle_ms(chain):
    """How old pump.fun's price bars must be before they are complete. Its EVM bars
    arrived 20+ s late (2026-10-06): two BSC buys were priced from bars 13-15 s
    older than the callout, and the late bars then counted as after the buy."""
    return (5 if chain == SOL else 60) * 1000


def update(api, state, pos, now_ms):
    """Price a position from its bars. Exits are scored after the fact, so a
    position needs a request only every few minutes: late is fine, as long as no
    second is skipped, and only bars old enough to be complete are used."""
    pos["checked_ms"] = now_ms
    chain = pos.get("chain", SOL)
    trusted = now_ms - settle_ms(chain)
    landed = pos["seen_ms"] + EXEC_DELAY_S * 1000
    # A buy is priced from bars around it, which then never scroll out of a 300-bar answer.
    raw = bars(api, chain, pos["mint"], now_ms if pos["entry"] is not None else min(now_ms, landed + 240_000), 300)
    bs = [b for b in raw or [] if b[0] + b[1] - 1000 <= trusted]
    if pos["entry"] is None:                      # the paper buy
        if not any(b[0] > landed for b in bs):
            # Data that does not reach past our landing may miss the fill window. Never
            # guess: a 150 s fallback priced a Solana buy from bars older than the pump
            # (2026-10-07: data minutes late, fill 0.61x the real one). Wait an hour: from
            # ~22:00 to ~01:00 on 2026-10-07 every coin's data came over 10 min late (it all
            # arrived later), and the 10 min this used to wait dropped 9 callouts caught in 1-6 s.
            if now_ms - pos["seen_ms"] > 3_600_000:
                log.warning("no price data past the buy for %s after 60 min; dropping it", pos["mint"])
                del state["positions"][pos["id"]]
            return
        price = fill_price(bs, pos["seen_ms"], landed)
        if price is None:
            log.warning("no trade before the buy for %s; dropping it", pos["mint"])
            del state["positions"][pos["id"]]
            return
        pos.update(entry=price, entry_ms=landed, tokens=STAKE_USD * (1 - COST) / price, last_close=price,
                   last_bar_ms=max([b[0] for b in bs if b[0] <= landed] or [landed - 1000]))
        log.info("PAPER BUY $%.0f of %s (called by %s) at $%.3g; callout seen %.1fs after posting, via %s",
                 STAKE_USD, pos["mint"], pos["caller"], price, (pos["seen_ms"] - pos["callout_ms"]) / 1000,
                 pos.get("via", "list"))
    elif raw is None:
        return   # no answer: nothing learned (and no sign the coin stopped trading); the next look resumes here
    elif len(raw) == 300 and raw[0][0] > pos["last_bar_ms"] + 1000:
        # 300 busy seconds since the last look: minute bars instead, coarser but nothing skipped. The rest of
        # the minute the last look stopped in comes second by second: that minute's whole bar would replay
        # seconds already scored, against a stop raised since and from an open older than them.
        edge = pos["last_bar_ms"] // 60_000 * 60_000 + 60_000
        secs = bars(api, chain, pos["mint"], edge, 61)
        mins = None if secs is None else bars(api, chain, pos["mint"], now_ms,
                                              min(300, math.ceil((now_ms - edge) / 60_000) + 1), "1m")
        if mins is None:
            return
        bs = [b for b in secs if b[0] < edge] + [b for b in mins if b[0] >= edge]
        bs = [b for b in bs if b[0] + b[1] - 1000 <= trusted]
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
                sell(r, r["left"], pos["last_close"], pos["tokens"], pos["entry_ms"] + LIMIT_MIN * 60_000,
                     f"{LIMIT_MIN}-min limit (no trade since)")
                r["left"], r["done"] = 0.0, True
    if all(r["done"] for r in pos["rules"].values()):
        trade = {"id": pos["id"], "caller": pos["caller"], "mint": pos["mint"], "chain": pos.get("chain", SOL),
                 "mcap": pos["mcap"],
                 "callout_ms": pos["callout_ms"], "seen_ms": pos["seen_ms"], "via": pos.get("via"),
                 "author_ok": pos.get("author_ok"), "size_ratio": pos.get("size_ratio"), "entry": pos["entry"],
                 "entry_ms": pos["entry_ms"], "tokens": pos["tokens"],
                 "fills": {name: r.get("fills", []) for name, r in pos["rules"].items()},
                 "pnl": {name: round(pnl(r), 2) for name, r in pos["rules"].items()}}
        record("trades.jsonl", trade)
        del state["positions"][pos["id"]]
        log.info("CLOSED %s (%s): %s", pos["mint"], pos["caller"],
                 " | ".join(f"{k}: {v:+.2f}" for k, v in trade["pnl"].items()))


# ---- what to spend the next request on --------------------------------------------

def pick(state, now_ms, pending):
    """The most useful next pump.fun request, or (None, None) to stay idle and keep
    the rate budget free for the next push alarm. In this order:
    1. the feed, at once, when a push alarm is waiting (again until the
       callout shows up there, MIN_GAP_S apart)
    2. the feed, if not polled for FEED_EVERY_S
    3. a position about to lose data: a busy coin's last 300 one-second bars
       cover only ~5 minutes, and a paper buy must be priced within 4 minutes
    4. the SOL price, if 10 minutes old (the push alarm's size check needs it)
    5. a paper buy waiting for its price, once its chain's bars are complete (tried
       every 10 s, after 2 minutes every 30 s, after 10 minutes every 2 minutes:
       pump.fun's data has been hours late, and an outage must not eat the budget)
    6. a position not checked for 2 minutes"""
    since_feed = now_ms - state.get("feed_ms", 0)
    if (pending and since_feed >= MIN_GAP_S * 1000) or since_feed >= FEED_EVERY_S * 1000:
        return "feed", None
    held = [p for p in state["positions"].values() if p["entry"] is not None]
    waiting = [p for p in state["positions"].values() if p["entry"] is None
               and now_ms >= p["seen_ms"] + EXEC_DELAY_S * 1000 + settle_ms(p.get("chain", SOL)) + 1000
               and now_ms - p["checked_ms"] >= (10_000 if now_ms - p["seen_ms"] < 120_000 else
                                                30_000 if now_ms - p["seen_ms"] < 600_000 else 120_000)]
    urgent = ([p for p in held if now_ms - p["checked_ms"] >= 240_000]
              + [p for p in waiting if now_ms - p["seen_ms"] >= 120_000])
    if urgent:
        return "position", min(urgent, key=lambda p: p["checked_ms"])
    if now_ms - state.get("sol_ms", 0) >= 600_000:
        return "sol", None
    if waiting:
        return "position", waiting[0]
    stale = [p for p in held if now_ms - p["checked_ms"] >= 120_000]
    if stale:
        return "position", min(stale, key=lambda p: p["checked_ms"])
    return None, None


def open_position(state, caller, c, seen_ms, via):
    state["positions"][c["calloutId"]] = new_position(caller, c, seen_ms, via)
    log.info("CALLOUT %s -> %s (mcap $%s) via %s, seen %.1fs after posting%s", caller, c["coinMint"],
             f"{c.get('marketCap') or 0:,.0f}", via, (seen_ms - c["createdAt"]) / 1000,
             f"; their position is {c['size_ratio']:.2f}x what they spent" if c.get("size_ratio") else "")


def step(api, state):
    """Wait for pump.fun's next free slot, then spend it on the most useful request.
    False when there was nothing to ask."""
    api.wait()
    now_ms = api.now_ms()
    take_pushed(state, now_ms)
    kind, pos = pick(state, now_ms, PENDING)
    if kind == "position":
        update(api, state, pos, now_ms)
    elif kind == "sol":
        state["sol_ms"] = now_ms
        price = (api.get("/sol-price", {}) or {}).get("solPrice")
        if price:
            PRICE["sol"] = float(price)
    elif kind == "feed":
        for caller, c, via, seen_ms in poll_feed(api, state):
            open_position(state, caller, c, seen_ms, via)
    return kind is not None


# ---- the push: pump.fun's live "New" callouts feed (NATS over websocket) ------------

def nats_login():
    """The website's public live-data login for its CORE cluster, from the homepage."""
    html = requests.get("https://pump.fun/", headers={"User-Agent": "Mozilla/5.0"}, timeout=20).text
    m = re.search(r'"CORE":\{[^}]*?"servers":"([^"]+)","user":"([^"]+)","pass":"([^"]+)"', html.replace('\\"', '"'))
    if not m:
        raise RuntimeError("pump.fun's homepage no longer carries the NATS login")
    return m.groups()


def nats_parse(buf):
    """Split received NATS protocol bytes into (message payloads, unparsed rest, PINGs)."""
    msgs, pings = [], 0
    while (i := buf.find(b"\r\n")) >= 0:
        line = buf[:i]
        if line.startswith((b"MSG ", b"HMSG ")):
            parts = line.split()
            size, hdr = int(parts[-1]), int(parts[-2]) if line.startswith(b"HMSG") else 0
            if len(buf) < i + 2 + size + 2:
                break                                   # the rest of this message is still coming
            msgs.append(buf[i + 2 + hdr: i + 2 + size])
            buf = buf[i + 2 + size + 2:]
            continue
        if line == b"PING":
            pings += 1
        elif line.startswith(b"-ERR"):
            raise ConnectionError(line.decode(errors="replace"))
        buf = buf[i + 2:]
    return msgs, buf, pings


def push_match(row, mints, now_ms, sol_usd):
    """(caller, callout) when a pushed callout may be one of our callers', else None:
    the coin is one they bought within MINT_WATCH_MIN and the row's position is about
    the size of what they spent. Only an alarm: a row names the coin, not the caller,
    so the feed has to confirm it. The token starts with the creation time in µs."""
    hit = mints.get(row.get("coinMint"))
    if not hit or hit[1] < now_ms or not sol_usd:
        return None
    created = int(str(row["token"]).split(":")[0]) // 1000
    if now_ms - created > MAX_CALLOUT_AGE_S * 1000:   # e.g. the snapshot that comes on (re)connect
        return None
    ratio = (row.get("costBasisUsd") or 0) / (hit[2] * sol_usd)
    if not SIZE_BAND[0] <= ratio <= SIZE_BAND[1]:
        return None
    return hit[0], {"calloutId": row["calloutId"], "coinMint": row["coinMint"], "createdAt": created,
                    "marketCap": row.get("calledOutAtMcap"), "size_ratio": round(ratio, 3)}


def evm_alarm(row, now_ms):
    """The callout, when a pushed EVM callout (whose caller nothing tells us) has a
    position of EVM_ALARM_USD or more: worth asking the feed about at once. Smaller
    ones wait for the routine feed poll."""
    if CHAINS.get(row.get("chain"), SOL) == SOL or (row.get("costBasisUsd") or 0) < EVM_ALARM_USD:
        return None
    created = int(str(row["token"]).split(":")[0]) // 1000
    if now_ms - created > MAX_CALLOUT_AGE_S * 1000:
        return None
    return {"calloutId": row["calloutId"], "coinMint": row["coinMint"], "createdAt": created}


async def push_watch(mints):
    """Every new callout on pump.fun within ~1.4 s, as the website's "New" tab gets them.
    Each message is a snapshot of the newest 25; a callout shows up in several."""
    import websockets
    pushed, delay = {}, 1    # callout id -> None, insertion-ordered so the oldest can be dropped
    while True:
        try:
            url, user, pw = await asyncio.to_thread(nats_login)
            async with websockets.connect(url, open_timeout=15, ping_interval=20, max_size=2**22) as ws:
                await asyncio.wait_for(ws.recv(), 10)   # the server's INFO: anything sent before it is ignored
                await ws.send(("CONNECT " + json.dumps({"verbose": False, "pedantic": False, "user": user,
                                                        "pass": pw, "protocol": 1, "headers": True,
                                                        "lang": "python", "version": "1"})
                               + "".join(f"\r\nSUB {s} {i}" for i, s in enumerate(PUSH_SUBJECTS, 1))
                               + "\r\nPING\r\n").encode())
                log.info("push on: pump.fun's live new-callouts feeds (Solana, and every chain)")
                delay, buf, last_msg = 1, b"", time.time()
                while True:
                    frame = await asyncio.wait_for(ws.recv(), PUSH_SILENCE_S)
                    msgs, buf, pings = nats_parse(buf + (frame if isinstance(frame, bytes) else frame.encode()))
                    for _ in range(pings):
                        await ws.send(b"PONG\r\n")
                    if msgs:
                        last_msg = time.time()
                    elif time.time() - last_msg > PUSH_SILENCE_S:
                        raise ConnectionError(f"nothing pushed for {PUSH_SILENCE_S} s")
                    now_ms = int(time.time() * 1000)
                    for payload in msgs:
                        for row in json.loads(payload).get("head") or []:
                            if row.get("calloutId") in pushed:
                                continue
                            pushed[row.get("calloutId")] = None
                            if (got := push_match(row, mints, now_ms, PRICE.get("sol"))) is not None:
                                DETECTED.put((*got, now_ms))
                            elif (c := evm_alarm(row, now_ms)) is not None:
                                DETECTED.put((None, c, now_ms))
                    while len(pushed) > 2000:
                        del pushed[next(iter(pushed))]
        except Exception as exc:   # nothing secret on this path: the login is public and not in errors
            log.warning("push dropped (%s: %s); reconnecting in %ds", type(exc).__name__, str(exc)[:120], delay)
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)


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


def received(tx, wallet):
    """Coins (not wrapped SOL) whose balance rose for `wallet` in a transaction it
    signed; nothing for someone else's transaction that merely mentions it."""
    if not any(k.get("pubkey") == wallet and k.get("signer") for k in tx["transaction"]["message"]["accountKeys"]):
        return set()
    meta = tx["meta"]
    before = {b["mint"]: int(b["uiTokenAmount"]["amount"]) for b in meta.get("preTokenBalances") or []
              if b.get("owner") == wallet}
    return {b["mint"] for b in meta.get("postTokenBalances") or []
            if b.get("owner") == wallet and b["mint"] != WSOL
            and int(b["uiTokenAmount"]["amount"]) > before.get(b["mint"], 0)}


def bought_coins(rpc_url, wallet, slot):
    """What the wallet bought in `slot`. Thousands of other people's transactions
    mention some of these wallets, so only successful ones in that slot are read."""
    def rpc(method, params):
        return requests.post(rpc_url, timeout=10, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                        "params": params}).json().get("result")
    for _ in range(3):
        recent = rpc("getSignaturesForAddress", [wallet, {"limit": 300, "commitment": "confirmed"}]) or []
        sigs = [s["signature"] for s in recent if s["slot"] == slot and s.get("err") is None]
        if sigs:
            coins = set()
            for sig in sigs:
                tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                                                  "commitment": "confirmed"}])
                coins |= received(tx, wallet) if tx else set()
            return coins
        time.sleep(1)    # not indexed yet
    return set()


async def name_coins(rpc_url, caller, sol, slot, mints):
    try:
        coins = await asyncio.to_thread(bought_coins, rpc_url, CALLERS[caller][1], slot)
    except Exception as exc:   # type only: a requests error message holds the RPC URL and key
        log.warning("could not read what %s bought (%s)", caller, type(exc).__name__)
        return
    now = int(time.time() * 1000)
    for coin in coins:   # several coins in one slot: split the SOL evenly (rare)
        prev = mints.get(coin)
        before = prev[2] if prev and prev[0] == caller and prev[1] >= now else 0
        mints[coin] = (caller, now + MINT_WATCH_MIN * 60_000, before + sol / len(coins))
    for coin, (_, t, _) in list(mints.items()):
        if t < now:
            del mints[coin]
    record("spends.jsonl", {"caller": caller, "sol": round(sol, 4), "slot": slot, "coins": sorted(coins),
                            "seen_ms": int(time.time() * 1000)})
    log.info("EARLY WARNING: %s spent %.2f SOL on %s; a callout on it counts as theirs for %d min",
             caller, sol, ", ".join(sorted(coins)) or "no coin found", MINT_WATCH_MIN)


def balances(rpc_url, callers):
    """Each caller's SOL balance now, in lamports: one getMultipleAccounts call."""
    value = requests.post(rpc_url, timeout=10, json={
        "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
        "params": [[CALLERS[c][1] for c in callers], {"encoding": "base64", "commitment": "confirmed"}]}
    ).json()["result"]["value"]
    return {c: (v or {}).get("lamports", 0) for c, v in zip(callers, value)}


async def watch(wss_url, rpc_url, mints):
    """Watch the callers' wallet balances. Their transaction logs were tried first:
    other people's failed transactions mention schoen's wallet ~18 times a second
    (~1 GB a day), and Helius bills websocket data at 2 credits per 0.1 MB."""
    import websockets
    # Balances survive reconnects, so a drop during one still shows. They are read before the
    # first subscription: otherwise a caller's first change after a start only set the baseline,
    # and a buy then gave its callout no push alarm.
    last, delay, tasks = {}, 1, set()
    while True:
        try:
            async with websockets.connect(wss_url, open_timeout=15, ping_interval=20) as ws:
                if missing := [c for c in CALLERS if c not in last]:
                    try:
                        last.update(await asyncio.to_thread(balances, rpc_url, missing))
                    except Exception as exc:   # type only: the error can hold the RPC URL and its key
                        log.warning("wallet watch: starting balances unread (%s); first changes only set them",
                                    type(exc).__name__)
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
                        task = asyncio.create_task(name_coins(rpc_url, *got, msg["params"]["result"]["context"]["slot"],
                                                              mints))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
            why = "closed by the server"
        except Exception as exc:   # type only: an error message can hold the URL and its key
            why = type(exc).__name__
        log.warning("wallet watch dropped (%s); reconnecting in %ds", why, delay)
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)


async def live(wss_url, rpc_url):
    await asyncio.gather(watch(wss_url, rpc_url, MINTS), push_watch(MINTS))


def run():
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)
    handlers = [logging.StreamHandler(sys.stdout),
                RotatingFileHandler(os.path.join(HERE, "logs", "bot.log"), maxBytes=5_000_000, backupCount=3,
                                    encoding="utf-8")]
    logging.basicConfig(level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("websockets").setLevel(logging.WARNING)
    log.info("PAPER MODE: no wallet, no keys, nothing is ever sent. $%.0f a trade, %d callers, at most %d "
             "pump.fun requests a minute, %.0f s apart; rules: %s", STAKE_USD, len(CALLERS), MAX_PER_MIN, MIN_GAP_S,
             "; ".join(RULES))
    env = load_env()
    if env.get("SOLANA_WSS_URL") and env.get("SOLANA_RPC_URL"):
        threading.Thread(target=asyncio.run, args=(live(env["SOLANA_WSS_URL"], env["SOLANA_RPC_URL"]),),
                         name="live", daemon=True).start()
    else:
        log.warning("no SOLANA_WSS_URL/SOLANA_RPC_URL in .env: push matching OFF, callouts seen in 7-14 s")
    api, state, beat = Api(), load(), 0.0
    state.pop("sol_ms", None)   # the price itself is not saved: fetch it now, the alarm's size check needs it
    # Earlier versions opened positions on unconfirmed push matches, mostly strangers' callouts.
    # Now a position opens only once the feed confirms the caller, so those are not this strategy.
    # The wallet track was removed on 2026-10-08: its open positions would otherwise be scored as callouts.
    drop = [k for k, p in state["positions"].items()
            if p.get("via") == "wallet" or (p.get("via") == "push" and p.get("author_ok") is not True)]
    for k in drop:
        del state["positions"][k]
    state.pop("wallet_full", None)
    if drop:
        log.info("dropped %d open paper positions: wallet-track ones, or opened on unconfirmed push matches", len(drop))
    try:
        while True:
            try:
                busy = step(api, state)
            except Exception:   # logged with its traceback; paper positions stay saved
                log.exception("step failed; retrying in 5s")
                busy = False
                time.sleep(5)
            if busy:
                save(state)
            else:
                time.sleep(0.2)   # idle keeps the rate budget free; push alarms are picked up within 0.2 s
            if time.time() - beat >= 900:   # a heartbeat, so a quiet log still shows the bot is alive
                beat = time.time()
                watched = [f"{c}:{m[:6]}" for m, (c, t, _) in dict(MINTS).items() if t > beat * 1000]
                log.info("alive: %d open paper positions; coins watched for a push alarm: %s",
                         len(state["positions"]), ", ".join(watched) or "none yet")
    except KeyboardInterrupt:
        save(state)
        log.info("stopped; %d open paper positions saved", len(state["positions"]))


def read_trades():
    path = os.path.join(STATE_DIR, "trades.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        trades = [json.loads(line) for line in f]
    for t in trades:   # rules renamed since the trade was recorded
        for key in ("pnl", "fills", "original_pnl"):
            if isinstance(t.get(key), dict):
                t[key] = {RENAMED.get(k, k): v for k, v in t[key].items()}
    return trades


def history():
    """Every trade on a confirmed callout: the buy, each sell with its fee and tip,
    and the result per rule. Also writes state/history.csv (one row per buy or sell;
    its net column adds up to each trade's result)."""
    rows = [["trade", "callout", "caller", "chain", "coin", "via", "seen_after_s", "rule", "action", "time", "share",
             "multiple", "price_usd", "value_usd", "fee_usd", "tip_usd", "net_usd", "reason"]]
    chain_name = {caip2: name for name, caip2 in CHAINS.items()}
    trades = [t for t in read_trades() if t.get("author_ok", True) is True]   # older trades predate the check

    def when(ms):
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    totals = collections.Counter()
    for n, t in enumerate(trades, 1):
        entry, lag = t["entry"], (t["seen_ms"] - t["callout_ms"]) / 1000
        tokens = t.get("tokens") or STAKE_USD * (1 - COST) / entry
        bought = t.get("entry_ms") or t["seen_ms"] + EXEC_DELAY_S * 1000
        chain = chain_name.get(t.get("chain", SOL), t.get("chain"))
        head = [n, when(t["callout_ms"]), t["caller"], chain, t["mint"], t.get("via") or "list", round(lag, 1)]
        print(f"\n#{n} {when(t['callout_ms'])}  {t['caller']} called {t['mint']} ({chain}) at "
              f"${t.get('mcap') or 0:,.0f} market cap; seen {lag:.1f}s later via {t.get('via') or 'the old per-caller list'}"
              + ("  [scored afterwards: pump.fun's prices came too late]" if t.get("replayed") else ""))
        print(f"   BUY  {when(bought)}  ${STAKE_USD:.2f} = fee ${STAKE_USD * COST:.2f} + ${STAKE_USD * (1 - COST):.2f}"
              f" for {tokens:,.0f} tokens at ${entry:.4g}; tip ${TIP_USD:.2f}")
        for name, result in t["pnl"].items():
            totals[name] += result
            rows.append(head + [name, "BUY", when(bought), 1, 1, entry, -STAKE_USD, round(STAKE_USD * COST, 2),
                                TIP_USD, -STAKE_USD - TIP_USD, ""])
            fills = (t.get("fills") or {}).get(name)
            print(f"   {name}: {result:+.2f}")
            if fills is None:
                print("      (this version did not record the sells)")
                rows.append(head + [name, "SELLS", "", "", "", "", "", "", "", round(result + STAKE_USD + TIP_USD, 2),
                                    "not recorded by this version"])
                continue
            for ms, share, price, why in fills:
                value = share * tokens * price
                fee, net = value * COST, value * (1 - COST) - TIP_USD
                print(f"      SELL {when(ms)}  {share:.0%} at {price / entry:.2f}x (${price:.4g}), {why}: "
                      f"${value:.2f} - fee ${fee:.2f} - tip ${TIP_USD:.2f} = ${net:.2f}")
                rows.append(head + [name, "SELL", when(ms), share, round(price / entry, 4), price, round(value, 2),
                                    round(fee, 2), TIP_USD, round(net, 2), why])
    if trades:
        print(f"\nTotals over {len(trades)} trades ($100 each):")
        for name, total in totals.items():
            print(f"   {name:<36} {total:+9.2f}")
    path = os.path.join(STATE_DIR, "history.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)
    print(f"\nwritten to {path} (opens in Excel)")


def by_rule(label, group):
    if group:
        print(f"\n{label}: {len(group)} trades")
    for name in RULES:
        xs = [t["pnl"][name] for t in group if name in t["pnl"]]
        if xs:
            print(f"  {name:<36} wins {sum(x > 0 for x in xs):>3}/{len(xs):<3}  total {sum(xs):+9.2f}  "
                  f"avg {sum(xs) / len(xs):+7.2f}")


def report():
    trades = read_trades()
    print(f"{len(trades)} closed paper trades, {len(load()['positions'])} open, ${STAKE_USD:.0f} each")
    wrong = sum(t.get("author_ok") is False for t in trades)
    unsure = sum("author_ok" in t and t["author_ok"] is None for t in trades)
    if wrong or unsure:
        print(f"left out below: {wrong} pushed callouts that were strangers', {unsure} whose caller was never "
              f"confirmed")
    good =[t for t in trades if t.get("author_ok", True) is True]   # older trades predate the check
    ratios = sorted(t["size_ratio"] for t in good if t.get("size_ratio"))
    if ratios:
        print(f"confirmed push matches: position/spend ratio {ratios[0]:.2f}-{ratios[-1]:.2f}, "
              f"median {statistics.median(ratios):.2f} (band {SIZE_BAND[0]}-{SIZE_BAND[1]})")
    fast = [t for t in good if t["seen_ms"] - t["callout_ms"] <= 5_000]
    by_rule("callout seen within 5s", fast)
    by_rule("callout seen later", [t for t in good if t not in fast])
    for via in ("push", "feed", None):
        lag = sorted((t["seen_ms"] - t["callout_ms"]) / 1000 for t in good if t.get("via") == via)
        if lag:
            print(f"\ncallout seen after posting, via {via or 'the old per-caller list'}: median "
                  f"{statistics.median(lag):.1f}s, p95 {lag[math.ceil(0.95 * len(lag)) - 1]:.1f}s, max {lag[-1]:.1f}s"
                  f"  (n={len(lag)})")
    replayed = sum(bool(t.get("replayed")) for t in good)
    if replayed:
        print(f"\n{replayed} of these were scored afterwards: pump.fun's prices came over 10 min late and an older "
              f"version gave up on them")


if __name__ == "__main__":
    {"report": report, "history": history}.get(sys.argv[1] if sys.argv[1:] else "", run)()
