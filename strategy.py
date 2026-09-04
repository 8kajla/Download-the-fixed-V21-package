from __future__ import annotations

"""V21 empirical trader-behavior strategy.

The strategy models observable behavior only.  It emits BUY signals; it never
assumes a signal is a fill.  Execution is handled by execution_simulator.py.

Evidence encoded here comes from the supplied 778,116-trade history:
- 100% BUY records; no BUY/SELL direction persistence is inferred.
- asset x regime distribution is used for the six configured assets.
- fine price-band distribution is conditioned on regime.
- regime-specific trajectory probabilities are used as a likelihood weight.
- entry-number sizing uses the supplied empirical quantiles/medians.
- intertrade cadence is sampled from the exact observed gap histogram.
"""

import hashlib
import json
import math
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


BANDS: Tuple[Tuple[str, float, float, str], ...] = (
    ("C00_05", 0.00, 0.05, "CHEAP"),
    ("C05_10", 0.05, 0.10, "CHEAP"),
    ("C10_15", 0.10, 0.15, "CHEAP"),
    ("C15_20", 0.15, 0.20, "CHEAP"),
    ("C20_30", 0.20, 0.30, "CHEAP"),
    ("M30_40", 0.30, 0.40, "MID"),
    ("M40_50", 0.40, 0.50, "MID"),
    ("M50_60", 0.50, 0.60, "MID"),
    ("M60_70", 0.60, 0.70, "MID"),
    ("R70_80", 0.70, 0.80, "CORE"),
    ("R80_90", 0.80, 0.90, "CORE"),
    ("H90_95", 0.90, 0.95, "HIGH"),
    ("H95_100", 0.95, 1.00, "HIGH"),
)
BAND_INDEX = {b: i for i, (b, *_rest) in enumerate(BANDS)}

# Verified from the supplied full-scale trader analysis for the six assets
# currently discovered by this bot.  These are trade-count distributions.
ASSET_REGIME_SHARE = {
    "BTC": {"CHEAP": 0.3339, "MID": 0.3993, "CORE": 0.1645, "HIGH": 0.1023},
    "SOL": {"CHEAP": 0.5816, "MID": 0.2457, "CORE": 0.0917, "HIGH": 0.0810},
    "DOGE": {"CHEAP": 0.8592, "MID": 0.0856, "CORE": 0.0181, "HIGH": 0.0372},
    "HYPE": {"CHEAP": 0.7563, "MID": 0.1464, "CORE": 0.0555, "HIGH": 0.0419},
    "ETH": {"CHEAP": 0.5986, "MID": 0.2165, "CORE": 0.0887, "HIGH": 0.0962},
    "BNB": {"CHEAP": 0.4472, "MID": 0.3555, "CORE": 0.1360, "HIGH": 0.0613},
}

ASSET_TRADE_SHARE = {
    "BTC": 164062 / 778116,
    "SOL": 137041 / 778116,
    "DOGE": 134338 / 778116,
    "HYPE": 127532 / 778116,
    "ETH": 125405 / 778116,
    "BNB": 89738 / 778116,
}

TRAJECTORY_SHARE = {
    "CHEAP": {"rising": 0.137582, "falling": 0.539948, "flat": 0.322470},
    "MID": {"rising": 0.341446, "falling": 0.422447, "flat": 0.236107},
    "CORE": {"rising": 0.499717, "falling": 0.280757, "flat": 0.219526},
    "HIGH": {"rising": 0.586489, "falling": 0.133042, "flat": 0.280469},
}
OUTCOME_SIDE_PERSISTENCE_DEFAULT = 0.893
TRAJECTORY_THRESHOLD = 0.005

@dataclass
class Signal:
    side: str
    price: float
    score: float
    notional: float
    reason: str


class EmpiricalTraderProcess:
    """Cadence sampler from the exact observed intertrade histogram."""

    def __init__(self, behavior: dict, seed: int = 20260831):
        self.rng = random.Random(seed)
        rows = behavior.get("intertrade_gap_histogram_seconds") or []
        self.gaps = [float(x["gap_seconds"]) for x in rows]
        self.weights = [float(x["count"]) for x in rows]
        if not self.gaps or not any(self.weights):
            raise ValueError("trader_behavior.json missing intertrade gap distribution")

    def sample_gap(self) -> float:
        return float(self.rng.choices(self.gaps, weights=self.weights, k=1)[0])


class TraderPolicyScheduler:
    """Choose among *available* candidates using empirical joint priors.

    There is deliberately no hard cumulative band quota. A hard quota can make
    the bot manufacture trades in a band simply because it is under target, or
    stop trading when the desired band is not currently available. Real CLOB
    conditions constrain what can actually be placed.
    """

    def __init__(self, behavior: dict, seed: int = 20260831, config: Optional[dict] = None):
        self.rng = random.Random(seed)
        self.behavior = behavior
        self.config = config or {}
        self.fine_targets = {
            str(x["fine_band"]): float(x["trade_share"])
            for x in behavior.get("fine_bands", [])
        }
        regime_totals = {}
        for row in behavior.get("fine_bands", []):
            band = str(row.get("fine_band"))
            if band not in self.fine_targets:
                continue
            regime = str(row.get("regime") or CapitalFirstStrategy.band_regime(band))
            regime_totals[regime] = regime_totals.get(regime, 0.0) + self.fine_targets[band]
        self.fine_within_regime = {
            b: share / max(regime_totals.get(CapitalFirstStrategy.band_regime(b), 1.0), 1e-12)
            for b, share in self.fine_targets.items()
        }
        # Normalize rounded source shares so numerical rounding in the six-asset
        # priors cannot introduce a tiny probability bias.
        raw_asset_trade = self.config.get("asset_trade_share") or ASSET_TRADE_SHARE
        asset_total = sum(max(0.0, float(v)) for v in raw_asset_trade.values())
        self.asset_trade_share = {
            str(k).upper(): max(0.0, float(v)) / asset_total if asset_total else 0.0
            for k, v in raw_asset_trade.items()
        }
        self.asset_regime_share = {}
        for asset, rows in (self.config.get("asset_regime_trade_share") or ASSET_REGIME_SHARE).items():
            total = sum(max(0.0, float(v)) for v in rows.values())
            self.asset_regime_share[str(asset).upper()] = {
                str(k): max(0.0, float(v)) / total if total else 0.0
                for k, v in rows.items()
            }
        self.trade_counts = {b: 0 for b in self.fine_targets}
        self.capital = {b: 0.0 for b in self.fine_targets}
        self.last_selected_candidate = None

    def observe(self, band: str, notional: float):
        if band in self.trade_counts:
            self.trade_counts[band] += 1
            self.capital[band] += max(0.0, float(notional))

    def restore(self, trades, fine_band_fn):
        self.trade_counts = {b: 0 for b in self.fine_targets}
        self.capital = {b: 0.0 for b in self.fine_targets}
        for t in trades or []:
            if t.get("action") != "BUY":
                continue
            try:
                band, _ = fine_band_fn(float(t.get("price")))
                if band in self.trade_counts:
                    self.trade_counts[band] += 1
                    self.capital[band] += max(0.0, float(t.get("notional", t.get("cost", 0.0))))
            except (TypeError, ValueError):
                continue

    def choose_band(self, candidates):
        if not candidates:
            return None
        # Candidate weights are the empirical joint P(asset, regime, band)
        # times the observed trajectory likelihood.  Availability is supplied
        # by the live CLOB, so unavailable historical regions simply disappear.
        weights = []
        for c in candidates:
            asset = str(c.get("asset", "")).upper()
            regime = c["regime"]
            band = c["band"]
            asset_regime = self.asset_regime_share.get(asset, {})
            ar = asset_regime.get(regime)
            if ar is None:
                # Unknown assets fall back to the measured global fine-band
                # distribution rather than receiving an invented asset prior.
                ar = sum(self.fine_targets.get(b, 0.0) for b, *_ in BANDS if self._regime(b) == regime)
            fw = self.fine_within_regime.get(band, 0.0)
            # Trajectory is intentionally not allowed to distort the primary
            # asset/regime/fine-band frequency distribution. It is used only
            # after the empirical price region has been selected.
            aw = self.asset_trade_share.get(asset, 1.0 / max(1, len(self.asset_trade_share)))
            w = max(1e-12, aw * ar * fw)
            weights.append(w)
        selected = self.rng.choices(candidates, weights=weights, k=1)[0]
        self.last_selected_candidate = selected
        return selected["band"]

    @staticmethod
    def _regime(band):
        for b, _, _, r in BANDS:
            if b == band:
                return r
        return ""

    def shares(self):
        total_t = sum(self.trade_counts.values())
        total_c = sum(self.capital.values())
        return {
            "trade": {b: self.trade_counts[b] / total_t if total_t else 0.0 for b in self.trade_counts},
            "capital": {b: self.capital[b] / total_c if total_c else 0.0 for b in self.capital},
        }

    def target_report(self):
        actual = self.shares()
        return {
            b: {
                "target_trade_share": self.fine_targets[b],
                "actual_trade_share": actual["trade"][b],
            }
            for b in self.fine_targets
        }


class CapitalFirstStrategy:
    VERSION = "V21_CLEAN_EMPIRICAL_ACCUMULATION_REALISTIC_CLOB"
    DATA_FILE = Path(__file__).with_name("trader_behavior.json")
    BANDS = BANDS
    HARD_CUTOFF = 60.0

    def __init__(self, bankroll=1000, start_sec=0, stop_sec=240,
                 hard_cutoff_seconds=60, max_total_exposure=300,
                 min_trade_gap_seconds=0, behavior_file=None,
                 seed=20260831, **_):
        self.bankroll = float(bankroll)
        self.start_sec = max(0.0, float(start_sec))
        self.stop_sec = min(300.0, float(stop_sec))
        self.hard_cutoff_seconds = max(60.0, float(hard_cutoff_seconds))
        self.max_total_exposure = max(0.0, float(max_total_exposure))
        self.min_trade_gap_seconds = max(0.0, float(min_trade_gap_seconds))
        self.seed = int(seed)
        self.rng = random.Random(self.seed)
        self._last_trade_at: Optional[float] = None
        path = Path(behavior_file) if behavior_file else self.DATA_FILE
        with path.open(encoding="utf-8") as f:
            self.behavior = json.load(f)
        config_path = path.with_name("v21_strategy_config.json")
        if not config_path.exists():
            raise RuntimeError(f"strategy config missing: {config_path}")
        try:
            with config_path.open(encoding="utf-8") as f:
                self.config = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"strategy config corrupt/unreadable: {exc}") from exc
        self.notional_scale = float(self.config.get("notional_scale", self.behavior.get("notional_scale", 0.4)))
        self.process = EmpiricalTraderProcess(self.behavior, seed=seed)
        self.cadence = self.process
        self.scheduler = TraderPolicyScheduler(self.behavior, seed=seed, config=self.config)
        self.fine_band_trade_share = {
            str(x["fine_band"]): float(x["trade_share"])
            for x in self.behavior.get("fine_bands", [])
        }
        self.entry_stats = self.behavior.get("entry_stats_by_fine_band", {})
        self.trajectory_share = self.config.get("trajectory_share_by_regime") or TRAJECTORY_SHARE
        self.trajectory_threshold = float(self.config.get("trajectory_threshold", TRAJECTORY_THRESHOLD))
        self.market_entries = {}
        self.market_first_signal = {}
        self.market_last_signal = {}
        self.market_last_side = {}
        self.outcome_side_persistence = float((self.behavior.get("confirmed_behavior") or {}).get("side_persistence_same_side_rate", OUTCOME_SIDE_PERSISTENCE_DEFAULT))

    @classmethod
    def band_regime(cls, band):
        for b, _, _, r in cls.BANDS:
            if b == band:
                return r
        return ""

    @classmethod
    def fine_band(cls, price):
        p = float(price)
        for band, lo, hi, regime in cls.BANDS:
            if lo <= p < hi:
                return band, regime
        if math.isclose(p, 1.0, abs_tol=1e-9):
            return "H95_100", "HIGH"
        return None, None

    def restore_policy_state(self, orders):
        """Reconstruct behavioral state exclusively from durable accepted orders.

        Accepted-order metadata is written in the same SQLite transaction as the
        order itself. This removes the old JSON/SQLite atomicity split.
        """
        self.scheduler.trade_counts={b:0 for b in self.scheduler.fine_targets}
        self.scheduler.capital={b:0.0 for b in self.scheduler.fine_targets}
        self.market_entries={}; self.market_first_signal={}; self.market_last_signal={}; self.market_last_side={}
        rows=sorted(list(orders or []), key=lambda x:(float(x.get('submitted_ts',0)),str(x.get('order_id',''))))
        for o in rows:
            meta=o.get('meta') or {}
            band=str(meta.get('strategy_band') or '')
            notional=meta.get('strategy_notional')
            if band not in self.scheduler.trade_counts:
                try: band,_=self.fine_band(float(o.get('price')))
                except (TypeError,ValueError): band=None
            if band in self.scheduler.trade_counts:
                self.scheduler.observe(band,float(notional if notional is not None else float(o.get('price',0))*float(o.get('requested_shares',0))))
            c=str(o.get('condition') or '')
            if c:
                self.market_entries[c]=self.market_entries.get(c,0)+1
                ts=float(o.get('submitted_ts',0)); self.market_first_signal[c]=min(ts,self.market_first_signal.get(c,ts)); self.market_last_signal[c]=max(ts,self.market_last_signal.get(c,0.0))
                side=str(o.get('side') or '')
                if side in ('Up','Down') and ts>=self.market_last_signal.get(c,0.0): self.market_last_side[c]=side

    def observe_signal(self, condition, band, notional, ts=None, side=None):
        """Advance behavioral state when an order is accepted, not when it fills."""
        condition = str(condition)
        self.scheduler.observe(str(band), float(notional))
        self.market_entries[condition] = self.market_entries.get(condition, 0) + 1
        if ts is not None:
            ts=float(ts); self.market_first_signal[condition] = min(ts, self.market_first_signal.get(condition, ts)); self.market_last_signal[condition] = ts
        if side in ("Up", "Down"):
            self.market_last_side[condition] = side
        # Durable accepted-order state is reconstructed from SQLite on restart.

    def forget_condition(self, condition):
        c=str(condition)
        self.market_entries.pop(c,None); self.market_first_signal.pop(c,None); self.market_last_signal.pop(c,None); self.market_last_side.pop(c,None)

    def market_entry_count(self, condition):
        return int(self.market_entries.get(str(condition), 0))

    def market_seconds_since_previous_signal(self, condition, now=None):
        now = time.time() if now is None else float(now)
        last = self.market_last_signal.get(str(condition))
        return None if last is None else max(0.0, now - last)

    def observe_trade_distribution(self, band, notional):
        # Backward-compatible alias. Signals, not fills, drive behavioral state
        # in V21; the bot calls observe_signal at order acceptance.
        return None

    @staticmethod
    def _points(history):
        out = []
        for item in history or []:
            try:
                if isinstance(item, dict):
                    ts = float(item["ts"])
                    px = float(item.get("best_bid", item.get("mid")))
                else:
                    ts, px = float(item[0]), float(item[1])
                if 0.0 < px < 1.0:
                    out.append((ts, px))
            except (TypeError, ValueError, KeyError, IndexError):
                continue
        return sorted(out)

    @classmethod
    def movement(cls, price, history, now):
        points = cls._points(history)
        result = {}
        for seconds in (1, 3, 5, 10, 30):
            previous = [px for ts, px in points if ts <= float(now) - seconds]
            result[f"m{seconds}"] = float(price) - previous[-1] if previous else 0.0
        return result

    def _trajectory_class(self, delta):
        if delta > self.trajectory_threshold:
            return "rising"
        if delta < -self.trajectory_threshold:
            return "falling"
        return "flat"

    def _entry_bucket(self, entry_count):
        n = int(entry_count)
        if n <= 0:
            return "1"
        if n == 1:
            return "2"
        if n == 2:
            return "3"
        if n <= 20:
            return str(n + 1)
        return "21+"

    def _stable_random(self, market, band, entry_count):
        raw = f"{self.seed}|{market}|{band}|{int(entry_count)}".encode()
        return random.Random(int(hashlib.sha256(raw).hexdigest()[:16], 16))

    def entry_target(self, price, market="BTC", entry_count=0):
        band, _ = self.fine_band(price)
        if not band:
            return 0.0
        stats = self.entry_stats.get(band, {})
        row = stats.get(self._entry_bucket(entry_count)) or stats.get("21+")
        if not row:
            return 0.0
        # Empirical quantile sampler. The source supplies p25/median/p75/p90;
        # above p90 we hold at the measured p90 rather than inventing a tail.
        q = self._stable_random(market, band, entry_count).random()
        points = [
            (0.00, 0.0),
            (0.25, float(row.get("p25", 0.0))),
            (0.50, float(row.get("median_notional", 0.0))),
            (0.75, float(row.get("p75", 0.0))),
            (0.90, float(row.get("p90", 0.0))),
            (1.00, float(row.get("p90", 0.0))),
        ]
        for (q0, v0), (q1, v1) in zip(points, points[1:]):
            if q <= q1:
                t = (q - q0) / max(q1 - q0, 1e-12)
                raw = v0 + t * (v1 - v0)
                return round(max(0.0, raw * self.notional_scale), 4)
        return round(float(points[-1][1]) * self.notional_scale, 4)

    capital_target = entry_target

    def _candidate(self, market, side, bid, ask, depth, history, now, entries, burst_age, previous_side=None, condition=None):
        if bid is None:
            return None
        try:
            bid = float(bid)
            ask = None if ask is None else float(ask)
            depth = None if depth is None else float(depth)
        except (TypeError, ValueError):
            return None
        if not 0.0 < bid < 1.0:
            return None
        if ask is not None and not 0.0 < ask <= 1.0:
            return None
        if ask is not None and ask < bid:
            return None
        band, regime = self.fine_band(bid)
        if not regime:
            return None
        mv = self.movement(bid, history, now)
        trajectory = self._trajectory_class(mv["m5"])
        likelihood = float(self.trajectory_share[regime][trajectory])
        target = self.entry_target(bid, market, entries)
        return {
            "asset": str(market).upper(),
            "condition": str(condition) if condition is not None else None,
            "side": side,
            "bid": bid,
            "ask": ask,
            "depth": depth,
            "band": band,
            "regime": regime,
            "trajectory": trajectory,
            "trajectory_likelihood": likelihood,
            "band_prior": self.fine_band_trade_share.get(band, 0.0),
            "same_side": bool(previous_side and side == previous_side),
            "side_likelihood": (self.outcome_side_persistence if previous_side and side == previous_side else (1.0 - self.outcome_side_persistence if previous_side else 1.0)),
            "target": target,
            "movement": mv,
            "entries": int(entries),
            "burst_age": float(burst_age),
            "reason": (
                f"{self.VERSION} asset={str(market).upper()} band={band} regime={regime} "
                f"trajectory={trajectory} trajectory_share={likelihood:.3f} "
                f"fine_band_share={self.fine_band_trade_share.get(band,0.0):.6f} "
                f"BUY_ONLY passive=bid entry_count={int(entries)} "
                f"target_40pct=${target:.4f} burst_age={float(burst_age):.1f}s "
                f"bid={bid:.4f} ask={ask if ask is not None else 0.0:.4f} "
                f"depth={depth if depth is not None else 0.0:.2f} "
                f"m1={mv['m1']:+.4f} m3={mv['m3']:+.4f} m5={mv['m5']:+.4f} "
                f"m10={mv['m10']:+.4f} m30={mv['m30']:+.4f}"
            ),
        }

    def build_candidates_for_market(self, elapsed, up_ask, down_ask, up_bid, down_bid,
                                    up_history, down_history, now, asset=None, market=None,
                                    thesis_side=None, market_entry_count=0,
                                    seconds_since_first_entry=0, up_depth=0, down_depth=0,
                                    previous_side=None, condition=None):
        seconds_since_first_entry=float(seconds_since_first_entry or 0.0)
        elapsed = float(elapsed)
        if elapsed < self.start_sec or elapsed >= self.stop_sec:
            return []
        if self.stop_sec - elapsed <= self.hard_cutoff_seconds:
            return []
        m = str(market or asset or "BTC").upper()
        out = []
        for side, bid, ask, depth, hist in (
            ("Up", up_bid, up_ask, up_depth, up_history),
            ("Down", down_bid, down_ask, down_depth, down_history),
        ):
            burst_age=max(0.0, seconds_since_first_entry)
            burst_position=int(market_entry_count) if burst_age <= float(self.config.get("burst_gap_seconds",18.0)) else 0
            c = self._candidate(m, side, bid, ask, depth, hist, float(now), market_entry_count, burst_age, previous_side=previous_side, condition=condition)
            if c is not None: c["burst_position"]=burst_position
            if c is not None:
                out.append(c)
        return out

    def choose_process_candidate(self, candidates, target_band=None, thesis_side=None):
        del thesis_side
        if not candidates:
            return None
        targeted = [c for c in candidates if target_band is None or c["band"] == target_band]
        if not targeted:
            return None
        if self.scheduler.last_selected_candidate in targeted:
            selected = self.scheduler.last_selected_candidate
            self.scheduler.last_selected_candidate = None
            return selected
        # Sample among actually available candidates rather than taking the
        # maximum trajectory likelihood.  Taking max() would systematically
        # over-select one trajectory and distort the empirical distribution.
        # The trajectory likelihood remains a weight, not a hard trigger.
        weights = [
            max(1e-12, float(c.get("trajectory_likelihood") or 0.0)) * max(1e-12, float(c.get("side_likelihood") or 0.0))
            for c in targeted
        ]
        return self.rng.choices(targeted, weights=weights, k=1)[0]

    def choose_distribution_band(self, candidates):
        return self.scheduler.choose_band(candidates)

    def sample_target_band(self):
        bands = list(self.fine_band_trade_share)
        weights = [self.fine_band_trade_share[b] for b in bands]
        return self.rng.choices(bands, weights=weights, k=1)[0]

    def decide(self, elapsed, up_ask, down_ask, up_bid, down_bid,
               up_history, down_history, now=None, current_exposure=0,
               available_cash=0, asset_exposure=0, thesis_side=None,
               thesis_price=None, market_entry_count=0,
               seconds_since_first_entry=0, up_depth=0, down_depth=0,
               asset=None, market=None, process_target_band=None, previous_side=None):
        del asset_exposure, thesis_price
        now = time.time() if now is None else float(now)
        candidates = self.build_candidates_for_market(
            elapsed, up_ask, down_ask, up_bid, down_bid, up_history, down_history,
            now, asset=asset, market=market, thesis_side=thesis_side,
            market_entry_count=market_entry_count,
            seconds_since_first_entry=seconds_since_first_entry,
            up_depth=up_depth, down_depth=down_depth, previous_side=previous_side,
        )
        if not candidates:
            return None
        target_band = process_target_band or self.choose_distribution_band(candidates)
        best = self.choose_process_candidate(candidates, target_band)
        if best is None:
            return None
        remaining = max(0.0, self.max_total_exposure - float(current_exposure or 0.0))
        target = float(best["target"])
        notion = min(target, max(0.0, float(available_cash)), remaining)
        if notion < 0.10:
            return None
        self._last_trade_at = now
        return Signal(
            best["side"], best["bid"], best["trajectory_likelihood"], round(notion, 4), best["reason"]
        )

    def size(self, price, regime=None, market="BTC", entry_count=0, **_):
        del regime
        return self.entry_target(price, market, entry_count)
