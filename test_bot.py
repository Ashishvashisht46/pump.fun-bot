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


def position(**kw):
    p = bot.new_position("schoen", {"calloutId": "c1", "coinMint": "Mint1", "createdAt": 0}, 0)
    p.update(entry=ENTRY, entry_ms=0, tokens=TOKENS, last_bar_ms=0, last_close=ENTRY, **kw)
    return p


def read_trade(d):
    with open(os.path.join(d, "trades.jsonl"), encoding="utf-8") as f:
        return json.loads(f.read())


def bar(t_s, o, h, l, c, final=True):
    return (t_s * 1000, 1000, o, h, l, c, final)


class FakeApi:
    """Serves callout lists and candles from dicts; records every request."""

    def __init__(self, callouts=None, candles=None):
        self.callouts, self.candles, self.calls = callouts or {}, candles or [], []

    def get(self, path, params):
        self.calls.append((path, params))
        if path.startswith("/callout/list/"):
            return {"callouts": self.callouts.get(path.rsplit("/", 1)[1], [])}
        return {"candles": [{"openTimeMs": t, "open": str(o), "high": str(h), "low": str(l), "close": str(c),
                             "final": f} for (t, w, o, h, l, c, f) in self.candles if t <= params["to"]]}


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
        state = {"seen": {}, "positions": {"c1": position()}}
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            bot.update(FakeApi(), state, state["positions"]["c1"], (bot.LIMIT_MIN + 2) * 60_000)
            self.assertEqual(state["positions"], {})
            trade = read_trade(d)
            self.assertAlmostEqual(trade["pnl"]["all at 2x"], TOKENS * (1 - bot.COST) - bot.STAKE_USD - 2 * bot.TIP_USD,
                                   places=2)


class Callouts(unittest.TestCase):
    def test_only_new_fresh_solana_callouts_are_bought(self):
        now = 1_000_000
        fresh = {"calloutId": "a", "coinMint": "SoLMint", "createdAt": now - 3_000}
        old = {"calloutId": "b", "coinMint": "OldMint", "createdAt": now - 120_000}
        evm = {"calloutId": "c", "coinMint": "0xabc", "createdAt": now - 1_000}
        api = FakeApi(callouts={bot.CALLERS["schoen"]: [fresh, old, evm]})
        state = {"seen": {}, "positions": {}}
        self.assertEqual([c["calloutId"] for c in bot.poll(api, state, "schoen", now)], ["a"])
        self.assertEqual(bot.poll(api, state, "schoen", now), [])     # the same callouts again: nothing

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


class EndToEnd(unittest.TestCase):
    def test_callout_to_paper_buy_to_recorded_trade_and_restart_keeps_it(self):
        with tempfile.TemporaryDirectory() as d:
            bot.STATE_DIR = d
            t0 = 2_000_000
            call = {"calloutId": "x1", "coinMint": "MintX", "createdAt": t0 - 2_000, "marketCap": 300_000}
            api = FakeApi(callouts={bot.CALLERS["marcell"]: [call]},
                          candles=[bar(t0 // 1000, 1.0, 1.02, 1.0, 1.01), bar(t0 // 1000 + 1, 1.01, 1.05, 1.0, 1.04)])
            state = {"seen": {}, "positions": {}, "next_caller": 0}
            bot.step(api, state, 0, t0)                         # marcell's callout is seen
            self.assertIn("x1", state["positions"])
            bot.save(state)
            state = bot.load()                                   # a restart in between
            bot.step(api, state, 1, t0 + 3_000)                  # the paper buy, at the window high
            self.assertEqual(state["positions"]["x1"]["entry"], 1.05)
            api.candles.append(bar(t0 // 1000 + 4, 1.1, 2.3, 1.1, 2.2))
            bot.step(api, state, 3, t0 + 7_000)                  # every target reached
            self.assertEqual(state["positions"], {})
            trade = read_trade(d)
            self.assertTrue(all(v > 0 for v in trade["pnl"].values()), trade["pnl"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
