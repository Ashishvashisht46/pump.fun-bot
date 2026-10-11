"""Offline tests: a fake pump.fun, a fake clock, a throwaway state folder.

    .venv\\Scripts\\python.exe -m unittest test_bot -v
"""
import asyncio
import collections
import contextlib
import csv
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import bot

ENTRY, TOKENS = 1.0, bot.STAKE_USD * (1 - bot.COST)       # a fill at $1.00 buys 98 tokens
PROFILE = {name: ids[0] for name, ids in bot.CALLERS.items()}
WALLET = bot.CALLERS["schoen"][1]


def position(**kw):
    p = bot.new_position("schoen", {"calloutId": "c1", "coinMint": "Mint1", "createdAt": 0}, 0, "feed")
    p.update({"entry": ENTRY, "entry_ms": 0, "tokens": TOKENS, "last_bar_ms": 0, "last_close": ENTRY, **kw})
    return p


def read_trade(d):
    with open(os.path.join(d, "trades.jsonl"), encoding="utf-8") as f:
        return json.loads(f.read())


def bar(t_s, o, h, l, c, final=True):
    return (t_s * 1000, 1000, o, h, l, c, final)


def feed_row(cid, profile, mint, created_ms, mcap=50_000, chain="solana"):
    """A /home-feed/new row as pump.fun serves it (only the fields the bot reads)."""
    ts = datetime.fromtimestamp(created_ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")
    return {"coinMint": mint, "chain": chain, "position": {"userId": profile, "userName": "someone", "callout": {
        "calloutId": cid, "calledOutAtMcap": mcap, "calloutTimestamp": ts}}}


class FakeApi:
    """Serves the feed and candles like pump.fun (candles: the last `count` bars up
    to `to`) on a fake clock; records every request."""

    def __init__(self, feed=None, candles=None, now_ms=0, takes_ms=0, sol=120.0):
        self.feed, self.candles, self.calls, self.sol = feed or [], candles or [], [], sol
        self.t, self.takes_ms = now_ms, takes_ms

    def wait(self):
        pass

    def now_ms(self):
        return self.t

    def get(self, path, params):
        self.calls.append((path, params))
        self.t += self.takes_ms
        if path == "/home-feed/new":   # like pump.fun: chain=all, or one chain's rows only
            return {"coins": [r for r in self.feed if params.get("chain") in ("all", r.get("chain"))]}
        if path == "/sol-price":
            return {"solPrice": self.sol}
        width = 1000 if params["res"] == "1s" else 60_000
        rows = [b for b in self.candles if b[1] == width and b[0] <= params["to"]][-params["count"]:]
        return {"candles": [{"openTimeMs": t, "open": str(o), "high": str(h), "low": str(l), "close": str(c),
                             "final": f} for (t, w, o, h, l, c, f) in rows]}


class ExitRules(unittest.TestCase):
    def test_main_rule_sells_80_percent_at_1_2x_and_the_rest_at_2x(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 1.25, 0.95, 1.2))
        bot.apply_bar(p, bar(2, 1.5, 2.1, 1.4, 2.0))
        main = p["rules"]["old main: 80% at +20%, 20% rides to 2x"]
        want = 0.8 * TOKENS * 1.2 * (1 - bot.COST) + 0.2 * TOKENS * 2.0 * (1 - bot.COST)
        self.assertAlmostEqual(main["proceeds"], want)
        self.assertTrue(main["done"])
        self.assertAlmostEqual(bot.pnl(main), want - bot.STAKE_USD - 3 * bot.TIP_USD)   # 1 buy + 2 sells
        self.assertEqual(main["fills"], [[1000, 0.8, 1.2, "target 1.2x"], [2000, 0.2, 2.0, "target 2x"]])

    def test_safer_rule_raises_its_stop_after_the_first_sale_from_the_next_bar(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 1.35, 1.0, 1.3))           # 50% sold at 1.3x; stop goes to 1.05x
        bot.apply_bar(p, bar(2, 1.3, 1.3, 1.04, 1.1))           # dips to 1.04: the raised stop sells the rest
        safer = p["rules"]["main: 50% at +30%, rest trails 30%, stop to 1.05x"]
        self.assertEqual(safer["fills"], [[1000, 0.5, 1.3, "target 1.3x"], [2000, 0.5, 1.05, "stop 1.05x"]])
        self.assertAlmostEqual(p["rules"]["50% at +30%, 50% rides to 2x"]["left"], 0.5)   # the 2x rule still rides

    def test_safer_rule_trails_30_percent_below_the_peak(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 1.35, 1.0, 1.3))
        bot.apply_bar(p, bar(2, 1.4, 2.0, 1.4, 1.9))            # peak 2.0: the trailing stop is now 1.4
        bot.apply_bar(p, bar(3, 1.9, 1.9, 1.39, 1.5))
        safer = p["rules"]["main: 50% at +30%, rest trails 30%, stop to 1.05x"]
        self.assertEqual(safer["fills"][-1][1:3], [0.5, 1.4])
        self.assertTrue(safer["fills"][-1][3].startswith("trailing stop"))

    def test_positions_opened_under_a_retired_rule_still_sell_by_it(self):
        p = position()
        p["rules"]["all at +10%"] = {"left": 1.0, "hit": [False], "proceeds": 0.0, "sells": 0, "done": False}
        bot.apply_bar(p, bar(1, 1.0, 1.15, 1.0, 1.1))
        self.assertTrue(p["rules"]["all at +10%"]["done"])

    def test_bar_touching_stop_and_target_is_the_stop(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 2.5, 0.6, 1.0))
        for r in p["rules"].values():
            self.assertTrue(r["done"])
            self.assertAlmostEqual(r["proceeds"], TOKENS * 0.7 * (1 - bot.COST))

    def test_gap_through_the_stop_fills_at_the_open(self):
        p = position()
        bot.apply_bar(p, bar(1, 0.4, 0.45, 0.3, 0.35))
        self.assertAlmostEqual(p["rules"]["all at 2x"]["proceeds"], TOKENS * 0.4 * (1 - bot.COST))

    def test_time_limit_sells_the_remainder_at_the_close(self):
        p = position()
        bot.apply_bar(p, bar(bot.LIMIT_MIN * 60, 1.05, 1.06, 1.04, 1.05))
        self.assertAlmostEqual(p["rules"]["all at 2x"]["proceeds"], TOKENS * 1.05 * (1 - bot.COST))

    def test_a_coin_nobody_trades_still_closes_after_the_limit(self):
        state = {"seen": {}, "positions": {"c1": position()}}
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            bot.update(FakeApi(), state, state["positions"]["c1"], (bot.LIMIT_MIN + 2) * 60_000)
            self.assertEqual(state["positions"], {})
            trade = read_trade(d)
            self.assertAlmostEqual(trade["pnl"]["all at 2x"], TOKENS * (1 - bot.COST) - bot.STAKE_USD - 2 * bot.TIP_USD,
                                   places=2)

    def test_falling_behind_a_busy_coin_falls_back_to_minute_bars(self):
        p = position(checked_ms=0, last_bar_ms=30_000)                         # seen up to second 30
        state = {"seen": {}, "positions": {"c1": p}}
        busy = [bar(400 + i, 1.1, 1.1, 1.1, 1.1) for i in range(300)]          # the last 300 seconds all traded
        minutes = [(m * 60_000, 60_000, 1.0, 1.0, 0.6 if m == 0 else 1.0, 1.0, True) for m in range(12)]
        bot.update(FakeApi(candles=[bar(45, 1.0, 1.0, 0.6, 0.6)] + busy + minutes), state, p, 720_000)
        # the stop broke at second 45, after the last look and before those 300 seconds: it must not be skipped
        self.assertAlmostEqual(p["rules"]["all at 2x"]["proceeds"], TOKENS * 0.7 * (1 - bot.COST))

    def test_a_partly_seen_minute_is_finished_in_seconds_not_replayed(self):
        p = position(checked_ms=0)
        bot.apply_bar(p, bar(10, 1.0, 1.0, 1.0, 1.0))
        bot.apply_bar(p, bar(50, 1.0, 1.35, 1.0, 1.3))                        # half sold at 1.3x; stop now 1.05x
        p["last_bar_ms"] = 50_000
        state = {"seen": {}, "positions": {"c1": p}}
        busy = [bar(400 + i, 1.3, 1.3, 1.3, 1.3) for i in range(300)]
        minutes = [(m * 60_000, 60_000, 1.0 if m == 0 else 1.3, 1.35, 1.0 if m == 0 else 1.25, 1.3, True)
                   for m in range(12)]
        bot.update(FakeApi(candles=[bar(55, 1.3, 1.3, 1.29, 1.3)] + busy + minutes), state, p, 720_000)
        # minute 0's bar opens at 1.0 and dips to 1.0, but both were before the stop was raised
        self.assertAlmostEqual(p["rules"]["main: 50% at +30%, rest trails 30%, stop to 1.05x"]["left"], 0.5)
        self.assertEqual(p["last_bar_ms"], 659_000)     # through minute 10's last second (minute 11 is too new)

    def test_no_answer_is_not_taken_for_no_trades(self):
        p = position(checked_ms=0)
        state = {"seen": {}, "positions": {"c1": p}}
        api = FakeApi()
        api.get = lambda path, params: None                                    # rate limited, or the network
        bot.update(api, state, p, (bot.LIMIT_MIN + 2) * 60_000)
        self.assertIn("c1", state["positions"])                               # not sold at an hour-old price

    def test_buy_pays_the_highest_price_between_seeing_and_landing(self):
        bs = [bar(9, 1.0, 1.1, 0.9, 1.0), bar(10, 1.0, 1.3, 1.0, 1.2), bar(11, 1.2, 1.25, 1.1, 1.2),
              bar(13, 1.2, 1.9, 1.2, 1.5)]
        self.assertEqual(bot.fill_price(bs, 10_000, 12_000), 1.3)
        self.assertEqual(bot.fill_price([bar(5, 1, 1, 1, 0.8)], 10_000, 12_000), 0.8)   # no trade: last price
        self.assertIsNone(bot.fill_price([], 10_000, 12_000))


class Feed(unittest.TestCase):
    NOW = 1_000_000_000

    def setUp(self):
        bot.PENDING.clear()

    def test_only_our_callers_fresh_callouts_timed_when_the_answer_arrives(self):
        now = self.NOW
        rows = [feed_row("a", PROFILE["schoen"], "MintA", now - 3_000),
                feed_row("b", "someone-else", "MintB", now - 1_000),
                feed_row("c", PROFILE["marcell"], "MintC", now - 120_000)]
        api = FakeApi(feed=rows, now_ms=now - 400, takes_ms=400)
        state = {"seen": {}, "positions": {}}
        got = bot.poll_feed(api, state)
        self.assertEqual([(caller, c["calloutId"], c["createdAt"], via, t) for caller, c, via, t in got],
                         [("schoen", "a", now - 3_000, "feed", now)])
        self.assertEqual(state["feed_ms"], now)
        self.assertEqual(bot.poll_feed(api, state), [])     # the same callouts again: nothing

    def test_the_feed_settles_push_alarms(self):
        now = self.NOW
        bot.PENDING.update({"ours": (now + 9_000, 0.98), "stranger": (now + 9_000, 1.02),
                            "not-yet": (now + 9_000, 1.0), "gone": (now - 1, 1.0)})
        rows = [feed_row("ours", PROFILE["schoen"], "M", now - 2_000), feed_row("stranger", "copycat", "M", now - 2_000)]
        got = bot.poll_feed(FakeApi(feed=rows, now_ms=now), {"seen": {}, "positions": {}})
        self.assertEqual([(caller, c["calloutId"], via, c["size_ratio"]) for caller, c, via, _ in got],
                         [("schoen", "ours", "push", 0.98)])
        self.assertEqual(set(bot.PENDING), {"not-yet"})      # a stranger's is settled; an old alarm given up

    def test_evm_callouts_by_our_callers_count_on_supported_chains(self):
        now = self.NOW
        rows = [feed_row("b", PROFILE["marcell"], "0xB88a", now - 2_000, chain="bsc"),
                feed_row("m", PROFILE["ily"], "0xM0n", now - 2_000, chain="monad")]       # no price data: skipped
        got = bot.poll_feed(FakeApi(feed=rows, now_ms=now), {"seen": {}, "positions": {}})
        self.assertEqual([(caller, c["coinMint"], c["chain"]) for caller, c, _, _ in got], [("marcell", "0xB88a", "eip155:56")])
        p = bot.new_position("marcell", got[0][1], now, "feed")
        api = FakeApi(now_ms=now + 8_000)
        bot.update(api, {"seen": {}, "positions": {"b": p}}, p, now + 8_000)
        self.assertEqual(api.calls[0][0], "/candles/eip155:56/0xB88a")   # its own chain's prices

    def test_an_evm_buy_waits_until_its_late_price_bars_have_arrived(self):
        t = 1_000_000_000
        p = bot.new_position("SoloFNF", {"calloutId": "e", "coinMint": "0xC0in", "createdAt": t - 2_000,
                                         "chain": "eip155:56"}, t, "push")
        state = {"seen": {}, "positions": {"e": p}, "feed_ms": t + 30_000, "sol_ms": t + 30_000}
        api = FakeApi(candles=[bar(t // 1000 - 15, 1.0, 1.0, 1.0, 1.0)])      # all BSC data had at first
        self.assertEqual(bot.pick(state, t + 30_000, {}), (None, None))       # too soon to ask: bars not complete
        bot.update(api, state, p, t + 63_000)
        self.assertIsNone(p["entry"])                                          # nothing past our landing yet
        api.candles += [bar(t // 1000 + 1, 1.3, 1.3, 1.3, 1.3), bar(t // 1000 + 5, 1.2, 1.2, 1.2, 1.2)]
        bot.update(api, state, p, t + 70_000)
        self.assertEqual(p["entry"], 1.3)                                      # the real price in the window
        api.candles.append(bar(t // 1000 + 40, 0.5, 0.5, 0.5, 0.5))           # a crash only 30 s old: not yet trusted
        bot.update(api, state, p, t + 70_000)
        self.assertFalse(any(r["done"] for r in p["rules"].values()))

    def test_a_buy_is_never_priced_from_data_that_stops_before_it(self):
        t = 1_000_000_000
        p = bot.new_position("SoloFNF", {"calloutId": "s", "coinMint": "Late", "createdAt": t - 1_000}, t, "push")
        state = {"seen": {}, "positions": {"s": p}}
        api = FakeApi(candles=[bar(t // 1000 - 30, 1.0, 1.0, 1.0, 1.0)])     # the data is minutes behind
        bot.update(api, state, p, t + 300_000)
        self.assertIsNone(p["entry"])                                        # no guessing from older bars
        self.assertEqual(api.calls[-1][1]["to"], t + 2_000 + 240_000)        # asked around the buy, not "now"
        bot.update(api, state, p, t + 601_000)
        self.assertIn("s", state["positions"])                               # pump.fun's data has been >10 min late
        bot.update(api, state, p, t + 3_601_000)
        self.assertEqual(state["positions"], {})                             # given up after 60 min

    def test_seen_list_keeps_the_newest_5000_however_old(self):
        state = {"seen": {f"s{i}": i for i in range(5_000)}, "positions": {}}
        bot.remember(state, "new", 10**13)
        self.assertEqual(len(state["seen"]), 5_000)
        self.assertNotIn("s0", state["seen"])
        self.assertIn("s1", state["seen"])


class Push(unittest.TestCase):
    def test_nats_bytes_split_into_messages_pings_and_leftovers(self):
        p = b'{"kind":"head"}'
        stream = (b'INFO {"server_id":"x"}\r\nPING\r\n'
                  + b"MSG homeFeedNew.head.solana.prod 1 %d\r\n" % len(p) + p + b"\r\n"
                  + b"HMSG s 1 12 %d\r\nNATS/1.0\r\n\r\n" % (12 + len(p)) + p + b"\r\n")
        cut = len(stream) - 5
        msgs, rest, pings = bot.nats_parse(stream[:cut])            # the second message is still arriving
        self.assertEqual((msgs, pings), ([p], 1))
        self.assertEqual(bot.nats_parse(rest + stream[cut:]), ([p], b"", 0))
        with self.assertRaises(ConnectionError):
            bot.nats_parse(b"-ERR 'Authorization Violation'\r\n")

    def test_an_alarm_needs_a_watched_coin_and_a_position_the_size_of_the_spend(self):
        now, sol_usd = 1_000_000_000, 120.0
        row = {"calloutId": "x", "coinMint": "CoinA", "token": f"{(now - 1_500) * 1000}:x", "calledOutAtMcap": 40_000,
               "costBasisUsd": 1_150.0}                                           # schoen spent 10 SOL = $1,200

        def watch():
            return {"CoinA": ("schoen", now + 60_000, 10.0)}
        for other in (dict(row, costBasisUsd=357), dict(row, costBasisUsd=5_000),   # far off the spend
                      dict(row, coinMint="CoinB"),                                   # nobody we watch bought it
                      dict(row, token=f"{(now - 61_000) * 1000}:x")):                # old: the snapshot on connect
            self.assertIsNone(bot.push_match(other, watch(), now, sol_usd))
        self.assertIsNone(bot.push_match(row, watch(), now, None))                  # no SOL price yet
        self.assertIsNone(bot.push_match(row, {"CoinA": ("schoen", now - 1, 10.0)}, now, sol_usd))   # watch over
        caller, c = bot.push_match(row, watch(), now, sol_usd)
        self.assertEqual((caller, c["calloutId"], c["createdAt"], c["size_ratio"]), ("schoen", "x", now - 1_500, 0.958))

    def test_an_evm_alarm_needs_a_fresh_callout_with_a_big_position(self):
        now = 1_000_000_000
        row = {"calloutId": "e", "coinMint": "0xC0in", "chain": "bsc", "token": f"{(now - 1_000) * 1000}:e",
               "costBasisUsd": 2_500.0}
        self.assertEqual(bot.evm_alarm(row, now), {"calloutId": "e", "coinMint": "0xC0in", "createdAt": now - 1_000})
        for other in (dict(row, costBasisUsd=120),                                  # small: the routine poll will do
                      dict(row, chain="solana"),                                    # Solana has its own alarm
                      dict(row, chain="monad"),                                     # no price data
                      dict(row, token=f"{(now - 61_000) * 1000}:e")):               # old: the snapshot on connect
            self.assertIsNone(bot.evm_alarm(other, now))

    def test_push_alarms_wait_for_the_feed_unless_already_seen(self):
        bot.PENDING.clear()
        state = {"seen": {"dup": 1}, "positions": {}}
        bot.DETECTED.put(("schoen", {"calloutId": "dup", "coinMint": "M", "createdAt": 1}, 5))
        bot.DETECTED.put(("schoen", {"calloutId": "new", "coinMint": "M", "createdAt": 1, "size_ratio": 0.9}, 5))
        self.assertTrue(bot.take_pushed(state, 1_000))
        self.assertEqual(bot.PENDING, {"new": (16_000, 0.9)})
        self.assertEqual(state["positions"], {})            # nothing is bought on the push alone
        self.assertFalse(bot.take_pushed(state, 2_000))


class PushWatchdog(unittest.TestCase):
    def test_a_subscription_that_only_gets_pings_is_reconnected(self):
        connects = []

        class SilentSocket:     # the server answers, but no callout snapshot ever comes
            def __init__(self):
                self.frames = 0

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def recv(self):
                self.frames += 1
                await asyncio.sleep(0.01)
                return b'INFO {"server_id":"x"}\r\n' if self.frames == 1 else b"PING\r\n"

            async def send(self, data):
                pass

        def connect(*args, **kwargs):
            connects.append(1)
            return SilentSocket()

        async def run_briefly():
            try:
                await asyncio.wait_for(bot.push_watch({}), 1.5)
            except asyncio.TimeoutError:
                pass
        with mock.patch.dict(sys.modules, {"websockets": SimpleNamespace(connect=connect)}), \
                mock.patch.object(bot, "nats_login", return_value=("wss://x", "u", "p")), \
                mock.patch.object(bot, "PUSH_SILENCE_S", 0.1), \
                contextlib.redirect_stderr(io.StringIO()):
            asyncio.run(run_briefly())
        self.assertGreaterEqual(len(connects), 2)


class Pacing(unittest.TestCase):
    def test_a_429_pauses_every_request(self):
        clock = SimpleNamespace(t=1000.0)
        slept = []

        def sleep(s):
            slept.append(s)
            clock.t += s
        responses = iter([SimpleNamespace(status_code=429, text=""),
                          SimpleNamespace(status_code=200, text="", json=lambda: {"ok": 1})])
        api = bot.Api(session=SimpleNamespace(get=lambda *a, **k: next(responses)), clock=lambda: clock.t, sleep=sleep)
        self.assertIsNone(api.get("/x", {}))
        self.assertEqual(api.get("/x", {}), {"ok": 1})
        self.assertGreaterEqual(slept[-1], 15)

    def test_at_most_7_requests_a_minute_3_seconds_apart(self):
        clock = SimpleNamespace(t=0.0)
        times = []

        def sleep(s):
            clock.t += s
        ok = SimpleNamespace(status_code=200, text="", json=lambda: {})
        api = bot.Api(session=SimpleNamespace(get=lambda *a, **k: times.append(clock.t) or ok),
                      clock=lambda: clock.t, sleep=sleep)
        for _ in range(9):
            api.get("/x", {})
        self.assertEqual(times, [0, 3, 6, 9, 12, 15, 18, 60, 63])   # the 8th waits for the 1st to age out


class Scheduler(unittest.TestCase):
    def test_the_priority_order(self):
        now = 10_000_000

        def callout(i, ago_s):
            return {"calloutId": i, "coinMint": "M" + i, "createdAt": now - ago_s * 1000}
        state = {"seen": {}, "feed_ms": now - 5_000, "sol_ms": now - 700_000, "positions": {
            "late": bot.new_position("ily", callout("late", 131), now - 130_000, "feed"),   # unpriced for 2 min
            "c1": position(checked_ms=now - 250_000),        # its 300-bar window is nearly gone
            "p": bot.new_position("marcell", callout("p", 9), now - 8_000, "feed"),        # landed; price is in
            "early": bot.new_position("schoen", callout("early", 3), now - 2_000, "feed"),  # too soon to price
            "s": dict(position(checked_ms=now - 130_000), id="s"),
            "q": dict(position(checked_ms=now - 10_000), id="q"),   # no trade for ages, but just checked
        }}
        pending = {"x": (now + 9_000, 1.0)}
        self.assertEqual(bot.pick(state, now, pending), ("feed", None))      # a push alarm: ask the feed now
        self.assertEqual(bot.pick(state, now, {})[0], "position")            # no alarm, feed polled 5 s ago
        order = [("position", "late"), ("position", "c1"), ("sol", None), ("position", "p"), ("position", "s"),
                 (None, None)]                                               # then stay idle
        for want in order:
            kind, x = bot.pick(state, now, {})
            self.assertEqual((kind, x["id"] if kind == "position" else x), want)
            if kind == "position":
                del state["positions"][x["id"]]
            elif kind:
                state[f"{kind}_ms"] = now
        self.assertEqual(bot.pick(state, now + 20_000, {}), ("feed", None))  # and the feed every 20 s


class LateBuyRetries(unittest.TestCase):
    def test_a_late_buy_is_retried_every_30_s_after_its_first_2_minutes(self):
        now = 10_000_000
        p = bot.new_position("schoen", {"calloutId": "w", "coinMint": "M", "createdAt": now - 181_000}, now - 180_000, "push")
        state = {"seen": {}, "feed_ms": now, "sol_ms": now, "positions": {"w": p}}
        p["checked_ms"] = now - 20_000
        self.assertEqual(bot.pick(state, now, {}), (None, None))
        p["checked_ms"] = now - 31_000
        self.assertEqual(bot.pick(state, now, {}), ("position", p))

    def test_after_10_minutes_a_late_buy_is_retried_every_2_minutes(self):
        now = 10_000_000
        p = bot.new_position("schoen", {"calloutId": "w", "coinMint": "M", "createdAt": now - 1_201_000}, now - 1_200_000,
                             "push")
        state = {"seen": {}, "feed_ms": now, "sol_ms": now, "positions": {"w": p}}
        p["checked_ms"] = now - 110_000
        self.assertEqual(bot.pick(state, now, {}), (None, None))   # an outage must not eat the budget
        p["checked_ms"] = now - 121_000
        self.assertEqual(bot.pick(state, now, {}), ("position", p))


class WalletWatch(unittest.TestCase):
    def test_only_a_drop_bigger_than_fees_in_a_callers_balance_counts(self):
        def note(sol, sub=7):
            return {"method": "accountNotification", "params": {"subscription": sub, "result": {
                "context": {"slot": 1}, "value": {"lamports": int(sol * 1e9)}}}}
        subs, last = {7: "schoen"}, {}
        self.assertIsNone(bot.spend(note(10), subs, last))                    # the first balance is a baseline
        self.assertIsNone(bot.spend(note(9.995), subs, last))                 # a fee
        self.assertIsNone(bot.spend(note(10.5), subs, last))                  # money in: a payout or a sale
        self.assertEqual(bot.spend(note(8.5), subs, last), ("schoen", 2.0))   # they bought something
        self.assertIsNone(bot.spend(note(1, sub=8), subs, last))              # not a caller's wallet
        self.assertIsNone(bot.spend({"jsonrpc": "2.0", "result": 7, "id": 1}, subs, last))   # a reply

    def test_the_first_change_after_a_start_is_measured_from_the_starting_balance(self):
        sub = {c: 100 + i for i, c in enumerate(bot.CALLERS, 1)}
        frames = [json.dumps({"jsonrpc": "2.0", "id": i, "result": s}) for i, s in enumerate(sub.values(), 1)]
        frames.append(json.dumps({"method": "accountNotification", "params": {"subscription": sub["schoen"], "result": {
            "context": {"slot": 9}, "value": {"lamports": 8 * 10**9}}}}))      # the first change: 10 -> 8 SOL
        named = []

        class Socket:
            def __init__(self):
                self.frames = list(frames)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def send(self, data):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not self.frames:
                    await asyncio.sleep(10)                  # nothing more comes; the test stops it first
                return self.frames.pop(0)

        async def name_coins(rpc_url, caller, sol, slot, mints):
            named.append((caller, round(sol, 3), slot))

        async def run_briefly():
            try:
                await asyncio.wait_for(bot.watch("wss://x", "https://rpc", {}), 0.5)
            except asyncio.TimeoutError:
                pass
        with mock.patch.dict(sys.modules, {"websockets": SimpleNamespace(connect=lambda *a, **k: Socket())}), \
                mock.patch.object(bot, "balances", return_value={c: 10 * 10**9 for c in bot.CALLERS}), \
                mock.patch.object(bot, "name_coins", name_coins):
            asyncio.run(run_briefly())
        self.assertEqual(named, [("schoen", 2.0, 9)])

    def test_only_coins_the_wallet_itself_received_count(self):
        def tx(signer, post):
            return {"transaction": {"message": {"accountKeys": [{"pubkey": WALLET, "signer": signer}]}},
                    "meta": {"preTokenBalances": [{"owner": WALLET, "mint": "Held", "uiTokenAmount": {"amount": "5"}}],
                             "postTokenBalances": [{"owner": WALLET, "mint": m, "uiTokenAmount": {"amount": a}}
                                                   for m, a in post]}}
        self.assertEqual(bot.received(tx(True, [("Held", "5"), ("CoinA", "9"), (bot.WSOL, "7")]), WALLET), {"CoinA"})
        self.assertEqual(bot.received(tx(True, [("Held", "9")]), WALLET), {"Held"})    # more of one it held
        self.assertEqual(bot.received(tx(False, [("CoinA", "9")]), WALLET), set())     # someone else's transaction


class History(unittest.TestCase):
    def test_the_csv_adds_up_to_each_trades_result_with_or_without_recorded_sells(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 1.25, 0.95, 1.2))
        bot.apply_bar(p, bar(2, 0.6, 0.65, 0.5, 0.55))           # the rest gaps through the stop
        new = {"id": "n", "caller": "schoen", "mint": "M", "mcap": 1, "callout_ms": 0, "seen_ms": 500, "via": "push",
               "author_ok": True, "entry": ENTRY, "entry_ms": 0, "tokens": TOKENS,
               "fills": {k: r["fills"] for k, r in p["rules"].items()},
               "pnl": {k: round(bot.pnl(r), 2) for k, r in p["rules"].items()}}
        old = {"id": "o", "caller": "schoen", "mint": "N", "mcap": 1, "callout_ms": 0, "seen_ms": 38_400,
               "entry": ENTRY, "pnl": {k: -26.71 for k in bot.RULES}}                     # an older record
        stranger = dict(old, id="s", author_ok=False)
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            for t in (new, old, stranger):
                bot.record("trades.jsonl", t)
            with contextlib.redirect_stdout(io.StringIO()):
                bot.history()
            with open(os.path.join(d, "history.csv"), encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        sums = collections.Counter()
        for r in rows:
            sums[(r["trade"], r["rule"])] += float(r["net_usd"])
        for n, t in (("1", new), ("2", old)):                     # the stranger's trade is left out
            for rule, result in t["pnl"].items():
                self.assertAlmostEqual(sums[(n, rule)], result, places=1)
        self.assertNotIn("3", {r["trade"] for r in rows})


class Calm45(unittest.TestCase):
    T = 1_000_000_000                                    # the callout, in ms
    CALM = next(iter(bot.CALM_RULES))

    def calm_position(self, call_price=1.0, seen_after_ms=1_500):
        state = {"seen": {}, "positions": {}}
        bot.open_position(state, "schoen", {"calloutId": "k", "coinMint": "M", "createdAt": self.T,
                                            "calloutPrice": call_price}, self.T + seen_after_ms, "push")
        del state["positions"]["k"]                      # the control's position: tested elsewhere
        return state, state["positions"]["calm:k"]

    def quiet_coin(self, window_high=1.08):
        s = self.T // 1000
        return [bar(s - 10, 1.0, 1.0, 1.0, 1.0),          # the price before the callout: p0 = 1.0
                bar(s + 5, 1.0, 1.03, 0.99, 1.02),
                bar(s + 30, 1.02, window_high, 1.0, 1.03),  # range so far: 0.99 .. window_high (9%)
                bar(s + 46, 1.03, 1.05, 1.03, 1.04),        # inside the landing window after 45 s: pays 1.05
                bar(s + 60, 1.2, 1.5, 1.2, 1.45)]           # +30% on the fill: sells everything

    def test_every_callout_also_opens_a_calm_position_deciding_45_s_after_it(self):
        state, p = self.calm_position()
        self.assertEqual((p["decide_ms"], p["call_price"], list(p["rules"])), (self.T + 45_000, 1.0, [self.CALM]))
        _, late = self.calm_position(seen_after_ms=50_000)
        self.assertEqual(late["decide_ms"], self.T + 50_000)           # seen after 45 s: decides at once
        state.update(feed_ms=self.T + 60_000, sol_ms=self.T + 60_000)
        ready = self.T + 45_000 + bot.EXEC_DELAY_S * 1000 + bot.settle_ms(bot.SOL) + 1000
        self.assertEqual(bot.pick(state, ready - 1, {}), (None, None))  # its bars are not complete yet
        self.assertEqual(bot.pick(state, ready, {}), ("position", p))

    def test_a_calm_coin_is_bought_after_45_s_and_kept_apart_from_the_control(self):
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            state, p = self.calm_position()
            bot.update(FakeApi(candles=self.quiet_coin()), state, p, self.T + 120_000)
            self.assertEqual(state["positions"], {})                     # bought, then +30% sold it all
            [t] = bot.read_trades("calm_trades.jsonl")
            self.assertEqual(bot.read_trades(), [])                       # nothing in the control's file
            self.assertEqual((t["entry"], t["entry_ms"], t["p0"], t["calm_range"], t["active_bars"]),
                             (1.05, self.T + 47_000, 1.0, 0.09, 2))          # not the 1.08 seen before deciding
            tokens = bot.STAKE_USD * (1 - bot.COST) / 1.05
            self.assertAlmostEqual(t["pnl"][self.CALM], round(tokens * 1.05 * 1.3 * (1 - bot.COST) - bot.STAKE_USD
                                                               - 2 * bot.TIP_USD, 2))

    def test_a_coin_that_moved_more_than_10_percent_or_a_broken_feed_is_skipped(self):
        for kwargs, coin, why in (({}, self.quiet_coin(window_high=1.2), "moved 21% in 45 s"),
                                  ({"call_price": 2.0}, self.quiet_coin(), "price feed below 0.8x the posted call price")):
            with tempfile.TemporaryDirectory() as d:
                bot.STATE_DIR = d
                state, p = self.calm_position(**kwargs)
                bot.update(FakeApi(candles=coin), state, p, self.T + 120_000)
                self.assertEqual(state["positions"], {})
                [skip] = bot.read_trades("calm_skips.jsonl")
                self.assertEqual(skip["why"], why)
                self.assertEqual(bot.read_trades("calm_trades.jsonl"), [])

    def test_the_report_compares_calm_45_with_the_control_at_both_tips(self):
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            state, p = self.calm_position()
            bot.update(FakeApi(candles=self.quiet_coin()), state, p, self.T + 120_000)
            ctrl = position()
            bot.apply_bar(ctrl, bar(1, 1.0, 1.35, 1.0, 1.3))
            bot.apply_bar(ctrl, bar(2, 0.6, 0.6, 0.6, 0.6))
            main = next(iter(bot.RULES))
            t = {"id": "c", "caller": "schoen", "mint": "M", "mcap": 1, "callout_ms": self.T, "seen_ms": self.T + 1000,
                 "via": "push", "author_ok": True, "entry": 1.0, "entry_ms": self.T + 3000, "tokens": TOKENS,
                 "fills": {main: ctrl["rules"][main]["fills"]}, "pnl": {main: round(bot.pnl(ctrl["rules"][main]), 2)}}
            bot.record("trades.jsonl", t)
            self.assertAlmostEqual(bot.at_tip(t, main, 0.15), t["pnl"][main] + 0.85 * 3)   # a buy and 2 sells
            with contextlib.redirect_stdout(io.StringIO()) as out:
                bot.report()
            text = out.getvalue()
            for want in ("CALM-45", "1 closed", "control over the same period: 1 trades", "at $0.15 tips"):
                self.assertIn(want, text)


class EndToEnd(unittest.TestCase):
    def test_push_alarm_to_feed_confirmed_paper_buy_to_trade_across_a_restart(self):
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            bot.PENDING.clear()
            t0 = 2_000_000_000
            row = {"calloutId": "x1", "coinMint": "MintX", "token": f"{(t0 - 1_000) * 1000}:x1", "calledOutAtMcap": 9e4,
                   "costBasisUsd": 1_188.0}
            alarm = bot.push_match(row, {"MintX": ("marcell", t0 + 3_600_000, 10.0)}, t0, 120.0)
            bot.DETECTED.put((*alarm, t0))                       # pushed 1 s after posting
            api = FakeApi(now_ms=t0, takes_ms=800, feed=[feed_row("x1", PROFILE["marcell"], "MintX", t0 - 1_000)],
                          candles=[bar(t0 // 1000, 1.0, 1.02, 1.0, 1.01), bar(t0 // 1000 + 1, 1.01, 1.05, 1.0, 1.04),
                                   bar(t0 // 1000 + 3, 1.04, 1.04, 1.03, 1.04)])
            state = bot.load()
            self.assertTrue(bot.step(api, state))                # the alarm makes it ask the feed at once
            p = state["positions"]["x1"]
            self.assertEqual((p["via"], p["author_ok"], p["seen_ms"], p["size_ratio"]), ("push", True, t0 + 800, 0.99))
            bot.save(state)
            state = bot.load()                                   # a restart in between
            api.t = t0 + 9_000
            bot.step(api, state)                                 # the SOL price, which the alarm's size check needs
            self.assertEqual(bot.PRICE["sol"], 120.0)
            bot.step(api, state)                                 # the paper buy, at the window high
            self.assertEqual(state["positions"]["x1"]["entry"], 1.05)
            api.candles.append(bar(t0 // 1000 + 4, 1.1, 2.3, 1.1, 2.2))
            api.candles.append(bar(t0 // 1000 + 6, 1.5, 1.5, 1.5, 1.5))   # back through the 30% trailing stop
            api.candles.append(bar(t0 // 1000 + 50, 1.5, 1.5, 1.5, 1.5))  # a trade after Calm-45's landing
            api.t = t0 + 140_000
            bot.step(api, state)                                 # the feed is due first...
            bot.step(api, state)                                 # ...then Calm-45, which waited 45 s, decides...
            bot.step(api, state)                                 # ...then the routine check: every target reached
            self.assertEqual(state["positions"], {})
            [skip] = bot.read_trades("calm_skips.jsonl")         # this coin has no price before its callout
            self.assertEqual((skip["id"], skip["why"]), ("calm:x1", "no price before the callout"))
            trade = read_trade(d)
            self.assertEqual((trade["via"], trade["author_ok"], trade["seen_ms"] - trade["callout_ms"], trade["size_ratio"]),
                             ("push", True, 1_800, 0.99))
            self.assertTrue(all(v > 0 for v in trade["pnl"].values()), trade["pnl"])
            self.assertFalse(bot.step(api, state))               # nothing left to ask: stay idle


if __name__ == "__main__":
    unittest.main(verbosity=2)
