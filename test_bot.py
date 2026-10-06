"""Offline tests: a fake pump.fun, a fake clock, a throwaway state folder.

    .venv\\Scripts\\python.exe -m unittest test_bot -v
"""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace

import bot

ENTRY, TOKENS = 1.0, bot.STAKE_USD * (1 - bot.COST)       # a fill at $1.00 buys 98 tokens
PROFILE = {name: ids[0] for name, ids in bot.CALLERS.items()}


def position(**kw):
    p = bot.new_position("schoen", {"calloutId": "c1", "coinMint": "Mint1", "createdAt": 0}, 0, False)
    p.update({"entry": ENTRY, "entry_ms": 0, "tokens": TOKENS, "last_bar_ms": 0, "last_close": ENTRY, **kw})
    return p


def read_trade(d):
    with open(os.path.join(d, "trades.jsonl"), encoding="utf-8") as f:
        return json.loads(f.read())


def bar(t_s, o, h, l, c, final=True):
    return (t_s * 1000, 1000, o, h, l, c, final)


class FakeApi:
    """Serves callout lists and candles like pump.fun (the last `count` bars up to
    `to`) on a fake clock; records every request."""

    def __init__(self, callouts=None, candles=None, now_ms=0, takes_ms=0):
        self.callouts, self.candles, self.calls = callouts or {}, candles or [], []
        self.t, self.takes_ms = now_ms, takes_ms

    def wait(self):
        pass

    def now_ms(self):
        return self.t

    def get(self, path, params):
        self.calls.append((path, params))
        self.t += self.takes_ms
        if path.startswith("/callout/list/"):
            return {"callouts": self.callouts.get(path.rsplit("/", 1)[1], [])}
        width = 1000 if params["res"] == "1s" else 60_000
        rows = [b for b in self.candles if b[1] == width and b[0] <= params["to"]][-params["count"]:]
        return {"candles": [{"openTimeMs": t, "open": str(o), "high": str(h), "low": str(l), "close": str(c),
                             "final": f} for (t, w, o, h, l, c, f) in rows]}


class ExitRules(unittest.TestCase):
    def test_main_rule_sells_80_percent_at_1_2x_and_the_rest_at_2x(self):
        p = position()
        bot.apply_bar(p, bar(1, 1.0, 1.25, 0.95, 1.2))
        bot.apply_bar(p, bar(2, 1.5, 2.1, 1.4, 2.0))
        main = p["rules"]["main: 80% at +20%, 20% rides to 2x"]
        want = 0.8 * TOKENS * 1.2 * (1 - bot.COST) + 0.2 * TOKENS * 2.0 * (1 - bot.COST)
        self.assertAlmostEqual(main["proceeds"], want)
        self.assertTrue(main["done"])
        self.assertAlmostEqual(bot.pnl(main), want - bot.STAKE_USD - 3 * bot.TIP_USD)   # 1 buy + 2 sells

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
        state = {"seen": {}, "polled": {}, "positions": {"c1": position()}}
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            bot.update(FakeApi(), state, state["positions"]["c1"], (bot.LIMIT_MIN + 2) * 60_000)
            self.assertEqual(state["positions"], {})
            trade = read_trade(d)
            self.assertAlmostEqual(trade["pnl"]["all at 2x"], TOKENS * (1 - bot.COST) - bot.STAKE_USD - 2 * bot.TIP_USD,
                                   places=2)

    def test_falling_behind_a_busy_coin_falls_back_to_minute_bars(self):
        p = position(checked_ms=0, last_bar_ms=30_000)                         # seen up to second 30
        state = {"seen": {}, "polled": {}, "positions": {"c1": p}}
        busy = [bar(400 + i, 1.1, 1.1, 1.1, 1.1) for i in range(300)]          # the last 300 seconds all traded
        minutes = [(m * 60_000, 60_000, 1.0, 1.0, 0.6 if m == 0 else 1.0, 1.0, True) for m in range(12)]
        bot.update(FakeApi(candles=busy + minutes), state, p, 720_000)
        # the stop broke in minute 0, partly seen and before those 300 seconds: it must not be skipped
        self.assertAlmostEqual(p["rules"]["all at 2x"]["proceeds"], TOKENS * 0.7 * (1 - bot.COST))


class Callouts(unittest.TestCase):
    def test_only_new_fresh_solana_callouts_are_bought_timed_when_the_answer_arrives(self):
        now = 1_000_000
        fresh = {"calloutId": "a", "coinMint": "SoLMint", "createdAt": now - 3_000}
        old = {"calloutId": "b", "coinMint": "OldMint", "createdAt": now - 120_000}
        evm = {"calloutId": "c", "coinMint": "0xabc", "createdAt": now - 1_000}
        api = FakeApi(callouts={PROFILE["schoen"]: [fresh, old, evm]}, now_ms=now - 400, takes_ms=400)
        state = {"seen": {}, "positions": {}, "polled": {}}
        got = bot.poll(api, state, "schoen")
        self.assertEqual([(c["calloutId"], c["_seen_ms"]) for c in got], [("a", now)])
        self.assertEqual(state["polled"]["schoen"], now)
        self.assertEqual(bot.poll(api, state, "schoen"), [])     # the same callouts again: nothing

    def test_seen_list_keeps_the_newest_5000_however_old(self):
        state = {"seen": {f"s{i}": i for i in range(5_001)}, "positions": {}, "polled": {}}
        bot.poll(FakeApi(now_ms=10**13), state, "ily")
        self.assertEqual(len(state["seen"]), 5_000)
        self.assertNotIn("s0", state["seen"])
        self.assertIn("s1", state["seen"])

    def test_buy_pays_the_highest_price_between_seeing_and_landing(self):
        bs = [bar(9, 1.0, 1.1, 0.9, 1.0), bar(10, 1.0, 1.3, 1.0, 1.2), bar(11, 1.2, 1.25, 1.1, 1.2),
              bar(13, 1.2, 1.9, 1.2, 1.5)]
        self.assertEqual(bot.fill_price(bs, 10_000, 12_000), 1.3)
        self.assertEqual(bot.fill_price([bar(5, 1, 1, 1, 0.8)], 10_000, 12_000), 0.8)   # no trade: last price
        self.assertIsNone(bot.fill_price([], 10_000, 12_000))


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


class Scheduler(unittest.TestCase):
    def test_the_priority_order(self):
        now = 10_000_000
        polled = {c: now - 5_000 for c in bot.CALLERS}
        polled["SoloFNF"] = now - 20_000          # checked longest ago, but not overdue
        polled["pxblocito"] = now - 200_000       # overdue
        hot = {"ily": now + 60_000}               # just bought on-chain

        def callout(i, ago_s):
            return {"calloutId": i, "coinMint": "M" + i, "createdAt": now - ago_s * 1000}
        state = {"seen": {}, "polled": polled, "positions": {
            "late": bot.new_position("ily", callout("late", 131), now - 130_000, True),   # unpriced for 2 min
            "c1": position(checked_ms=now - 250_000),        # its 300-bar window is nearly gone
            "p": bot.new_position("marcell", callout("p", 9), now - 8_000, False),       # landed; price is in
            "early": bot.new_position("schoen", callout("early", 3), now - 2_000, False),  # too soon to price
            "s": dict(position(checked_ms=now - 70_000), id="s"),
            "q": dict(position(checked_ms=now - 10_000), id="q"),   # no trade for ages, but just checked
        }}
        order = [("position", "late"), ("position", "c1"), ("caller", "pxblocito"), ("caller", "ily"),
                 ("position", "p"), ("position", "s"), ("caller", "SoloFNF")]
        for want in order:
            kind, x = bot.pick(state, now, hot)
            self.assertEqual((kind, x["id"] if kind == "position" else x), want)
            if kind == "position":
                del state["positions"][x["id"]]
            else:
                polled[x] = now
                hot.pop(x, None)


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


class EndToEnd(unittest.TestCase):
    def test_early_warned_callout_to_paper_buy_to_recorded_trade_across_a_restart(self):
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            t0 = 2_000_000
            call = {"calloutId": "x1", "coinMint": "MintX", "createdAt": t0 - 2_000, "marketCap": 300_000}
            api = FakeApi(callouts={PROFILE["marcell"]: [call]}, now_ms=t0,
                          candles=[bar(t0 // 1000, 1.0, 1.02, 1.0, 1.01), bar(t0 // 1000 + 1, 1.01, 1.05, 1.0, 1.04)])
            state = bot.load()
            state["polled"] = {c: t0 for c in bot.CALLERS}     # everyone was just checked...
            hot = {"marcell": t0 + 600_000}                     # ...but marcell just bought on-chain
            bot.step(api, state, hot)                           # so marcell is asked, and the callout is in
            self.assertTrue(state["positions"]["x1"]["hot"])
            self.assertNotIn("marcell", hot)
            bot.save(state)
            state = bot.load()                                  # a restart in between
            api.t = t0 + 8_000
            bot.step(api, state, hot)                           # the paper buy, at the window high
            self.assertEqual(state["positions"]["x1"]["entry"], 1.05)
            api.candles.append(bar(t0 // 1000 + 4, 1.1, 2.3, 1.1, 2.2))
            api.t = t0 + 75_000
            bot.step(api, state, hot)                           # the routine check: every target reached
            self.assertEqual(state["positions"], {})
            trade = read_trade(d)
            self.assertTrue(trade["hot"])
            self.assertTrue(all(v > 0 for v in trade["pnl"].values()), trade["pnl"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
