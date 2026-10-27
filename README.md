# Trench Bot (paper-trading showcase)

A Solana bot I built that watches brand-new pump.fun token launches, filters them, and paper-trades the ones that pass. It never sends real transactions. Everything is simulated so I can test ideas safely.

This is a cleaned showcase copy. All strategy numbers (entry ranges, filters, score weights, take-profit and stop levels) have been replaced with example values in `bot/config.example.json`. The structure is real; the settings are not the ones I use.

## What it does

1. **Watches new launches.** A small stream process (`stream/stream.py`) connects to a public pump.fun trade feed over WebSocket and writes new coins and trades into a local SQLite database. The bot also polls the public pump.fun coin list as a backup.
2. **Prices coins on-chain.** For coins near the entry range, it reads the pump.fun bonding-curve accounts straight from Solana RPC (read-only `get*` calls) to get a current price, and reads PumpSwap pool accounts after a coin graduates.
3. **Filters coins.** Each candidate goes through checks: market cap range, age, holder count, top-10 holder share, bundler and sniper share, dev holdings, how many coins the creator launched before, duplicate tickers, and more. pump.fun "Mayhem mode" tokens are always skipped. Coins that pass get a score with a written breakdown of why.
4. **Paper-trades with realistic costs.** Simulated buys and sells include pump.fun's fee on each side, slippage, priority fees, the token-account deposit, curve price impact, and a modelled rate of failed sells.
5. **Manages exits.** Partial take-profits, a trailing stop, a hard stop, an early check shortly after entry, a time stop, and handling for coins that graduate to PumpSwap.
6. **Tracks PnL.** Every trade, partial sell, and skip reason is stored in SQLite so results can be reviewed and backtested.
7. **Live dashboard.** `dashboard/server.py` serves a small read-only web page (open trades, recent sells, entry market cap, PnL) that refreshes every few seconds. It only reads the databases.

## Layout

```
stream/stream.py        WebSocket listener, writes launches and trades to SQLite
bot/bot.py              main loop: poll, evaluate, open and close paper positions, reports
bot/feed.py             public pump.fun feed client and read-only Solana RPC client
bot/fastfeed.py         reacts to the stream database within about a second
bot/score.py            filters and scoring with reasons
bot/exits.py            exit logic and paper fill/cost model
bot/store.py            SQLite storage
bot/regime.py           market-condition stats (graduation rate, how many coins round-trip)
bot/wallets.py, worker.py, holdercounts.py   holder and wallet checks (optional data sources)
bot/learn.py            walk-forward checks before any setting change is trusted
bot/poolprice.py        PumpSwap pool price reads after graduation
bot/solprice.py         SOL/USD from free public tickers
bot/execution.py        live trading guard; this build has no signer and refuses all orders
dashboard/server.py     read-only dashboard server, dashboard/index.html is the page
dashboard/auditcost.py  stricter cost model used to double-check paper PnL
```

## Running it

```
pip install -r bot/requirements.txt
cp bot/config.example.json bot/config.json
cp bot/score_v2.example.json bot/config/score_v2.json   # optional score table
python stream/stream.py &
python bot/bot.py
DASH_TOKEN=pick-a-long-random-string python dashboard/server.py
```

No API keys are needed. An optional Helius key can be supplied with the `HELIUS_API_KEY` environment variable if `data_sources.helius_enabled` is turned on. Never commit keys; `.gitignore` excludes `.env`, keypairs, data and logs.

## What I learned

- **Test before risking money.** I ran several paper bots side by side, each with one change, so I could compare sell strategies on the same coins.
- **Let the data decide.** I logged every buy, sell, and skip, then backtested sell-rule variations (different take-profit levels, trailing stops, early stops, time stops) against the full trade history, including losing trades.
- **Costs matter.** Fees, the token-account deposit, and price impact on small bonding curves change results a lot, so I built a separate audited cost model to check the paper numbers.
- **Guard against overfitting.** Setting changes have to hold up on walk-forward tests before they are trusted.
- **Reliability.** Rate limits, stale prices, failed RPC calls, and restarts all had to be handled so the bot could run unattended for days.
