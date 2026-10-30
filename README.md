# pump-callout-bot

A **paper-only** research tool. It follows the public pump.fun callouts of a few chosen callers
and simulates ("paper-trades") buying each called coin. Fixed exit rules then score every
simulated trade, to measure whether tailing those callouts would make money after fees.

- **No real trades.** It holds no wallet or private key and contains no code that can send a
  transaction.
- **Read-only.** It only reads pump.fun data (callouts, prices) and public Solana data.
- **One caller.** It currently follows `slingoor` (`CALLERS` in `bot.py`).

## Status (2026-10-30)

**The bot cannot run right now.**
- It reads the pump.fun website's own endpoints (`frontend-api-v3`, the site's live feed).
- Since late October 2026 those answer scripts with a Cloudflare bot check (HTTP 403,
  `cf-mitigated: challenge`).
- The plan is to move to the official [pump.fun Developer API](https://docs.pump.fun). Access has
  been requested.

When it moves to the Developer API, it will follow that API's terms:
- calls are made server-side only;
- callout data is kept no longer than 24 hours (today's trade log keeps it indefinitely; this
  will change);
- pump.fun is credited as the source;
- nothing is redistributed.

## How it works

**Seeing a callout:**
- pump.fun's live push of new callouts carries no caller. So it is matched to a caller who just
  bought that coin; a Solana websocket watches the callers' wallets for those buys. This is
  optional.
- The site-wide "New" callouts feed then confirms who posted the callout.
- Callouts seen more than 60 s after posting are not bought.

**The paper buy:**
- It is booked at the highest traded price between seeing the callout and landing 2 s later.
- Each trade costs 2% each way plus a $1 tip per transaction. `report` also shows a realistic
  $0.15 tip.

**Exit rules** are all scored on every trade. Each has a stop at 0.7x and a 60-minute limit.

| Rule | Exits |
|---|---|
| main | 50% at +30%, the rest trails 30% below its peak, and the stop moves to 1.05x |
| | 50% at +30%, 50% rides to 2x |
| | 80% at +20%, 20% rides to 2x |
| | 50% at +20%, 50% at 2x |
| | all at 2x |
| | 40% at +20%, 40% at +40%, 20% at 2x |

**Calm-45** is a second paper track. It waits 45 s after the callout and buys only if the price
stayed within a 10% range. It then sells everything at +30%, with a stop at 0.8x.

## Results so far (paper, $100 a trade)

- **Up to 22 callers, 2026-10-06 to 10-12:** about 260 confirmed trades. Every exit rule lost money.
  Calm-45 failed the stop rules set before it started.
- **Copying slingoor's wallet buys, backtested 2026-10-19 to 10-26** on on-chain prices: 353
  trades, and every rule lost.
  - The main rule lost $2,437, or −$6.90 a trade.
  - Our simulated fills were a median 1.10x his price, because faster bots got in first.

His callouts themselves are not tested yet; that needs the Developer API.

Nothing here is financial advice.

## Run

Developed on Python 3.13.

```
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python.exe bot.py            # run
.venv\Scripts\python.exe bot.py report     # results so far
.venv\Scripts\python.exe bot.py history    # every trade: buy, sells, fees (also state/history.csv)
.venv\Scripts\python.exe -m unittest test_bot
```

**Settings:**
- An optional `.env` holds `SOLANA_RPC_URL` and `SOLANA_WSS_URL`, for the wallet-watch push alarm.
- Optional environment variables: `STAKE_USD`, `COST_EACH_WAY`, `TIP_USD` and `PUMP_MIN_GAP_S`.

**Files:**
- `state/` holds the open positions and trade logs; `logs/` holds the log.
- `.env`, `state/` and `logs/` are never committed.

Data shown or derived here comes from pump.fun and the Solana blockchain.
