# V21 Clean Empirical Accumulation Paper Bot

Paper-only Polymarket research/execution simulator. It never submits live CLOB orders.

## Core guarantees
- Durable SQLite event journal for public market trades, orders, fills and accounting.
- Financial mutations for a public trade are committed atomically; failed transactions leave the trade unseen and retryable.
- Deterministic fill identity plus database uniqueness prevents duplicate accounting.
- Database checks prevent order overfill and fill/notional inconsistencies.
- Public passive BUY fills require an observed public SELL print at the exact simulated bid, after activation latency and estimated queue-ahead volume. A lower-priced print is **not** treated as proof of execution.
- Same public trade volume cannot be allocated more than once by the simulator.
- WebSocket book state is reset on reconnect and price changes are ignored until a fresh full book snapshot is observed for that token.
- Older book/price-change timestamps cannot overwrite newer token state.
- New simulated orders require a fresh per-token book/WS observation.
- Discovery failures do not orphan markets that have open orders/positions; market metadata is recoverable from durable order/fill records after restart.
- Settlement is idempotent and accounting conservation is audited.
- Strategy behavior is reconstructed from durable accepted-order metadata; there is no separate strategy-state file.

## Important modeling limits
Public CLOB data does not reveal exact hidden maker queue position, cancellations/replenishment, or the original trader's private trigger. V21 deliberately uses conservative, explicit approximations rather than claiming those are observable facts.

## Run
```bash
python bot.py
python -m pytest -q
```
