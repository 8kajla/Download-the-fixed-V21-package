# V21 Strategy Specification

## Evidence source

The strategy uses the supplied 778,116-trade / 76,154-market analysis covering 2026-05-30 through 2026-08-23.

Verified observable behavior encoded by V21:

- 100% BUY action in the supplied rows.
- Six asset trade-count shares: BTC 164,062; SOL 137,041; DOGE 134,338; HYPE 127,532; ETH 125,405; BNB 89,738.
- Asset-specific regime shares for CHEAP/MID/CORE/HIGH.
- Thirteen fine price bands.
- Empirical entry sizing by fine band and entry number.
- Historical intertrade-gap distribution.
- Regime-specific trajectory shares.
- Approximate outcome-side persistence of 0.893, kept separate from BUY/SELL action.

## Non-evidence / limitations

The dataset does not establish:

- maker/taker status;
- exact historical orderbook queue position;
- exact hidden trigger logic;
- exact realized fill path;
- BUY/SELL directional persistence (all action rows are BUY).

V21 does not treat these unknowns as facts.

## Execution boundary

`strategy.py` emits a candidate BUY signal only. `execution_simulator.py` independently decides whether and how that resting simulated order fills from public CLOB trade events.
