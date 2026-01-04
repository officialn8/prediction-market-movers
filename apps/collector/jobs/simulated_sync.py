from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List
from uuid import UUID

from packages.core.storage.queries import MarketQueries


CATEGORIES = ["Politics", "Sports", "Crypto", "World", "Tech"]


@dataclass
class SimState:
    # market_id (uuid) -> {"YES": token_uuid, "NO": token_uuid}
    market_tokens: Dict[UUID, Dict[str, UUID]]


def _now():
    return datetime.now(timezone.utc)


def seed_simulated_markets(n_markets: int = 30) -> SimState:
    """
    Create N simulated markets with YES/NO tokens.
    Idempotent because we upsert by (source, source_id) and token upsert by (market_id, outcome).
    """
    state: Dict[UUID, Dict[str, UUID]] = {}

    for i in range(n_markets):
        source = "simulated"
        source_id = f"SIM-{i}"
        title = f"Sim Market #{i}: Will event happen?"
        category = random.choice(CATEGORIES)
        status = "active"
        url = f"http://localhost/sim/{i}"

        m = MarketQueries.upsert_market(
            source=source,
            source_id=source_id,
            title=title,
            category=category,
            status=status,
            url=url,
        )

        market_id = m["market_id"]
        if not isinstance(market_id, UUID):
            market_id = UUID(str(market_id))

        yes = MarketQueries.upsert_token(
            market_id=market_id,
            outcome="YES",
            symbol="YES",
            source_token_id=f"{source_id}:YES",
        )
        no = MarketQueries.upsert_token(
            market_id=market_id,
            outcome="NO",
            symbol="NO",
            source_token_id=f"{source_id}:NO",
        )

        yes_token_id = yes["token_id"]
        if not isinstance(yes_token_id, UUID):
            yes_token_id = UUID(str(yes_token_id))

        no_token_id = no["token_id"]
        if not isinstance(no_token_id, UUID):
            no_token_id = UUID(str(no_token_id))

        state[market_id] = {
            "YES": yes_token_id,
            "NO": no_token_id,
        }

    return SimState(market_tokens=state)


def write_simulated_snapshots(sim: SimState) -> int:
    """
    Append one snapshot per token. Prices will jitter a bit so movers exist.
    """
    ts = _now()
    snapshots: List[dict] = []

    for market_id, tokmap in sim.market_tokens.items():
        # market-specific drift so different markets move differently
        drift = ((market_id.int % 10_000) / 10_000) * 0.02  # 0..2%

        for outcome, token_id in tokmap.items():
            base = 0.50 + (0.02 if outcome == "YES" else -0.02)
            price = base + drift + random.uniform(-0.03, 0.03)
            price = max(0.01, min(0.99, price))

            volume_24h = random.randint(1_000, 2_000_000)
            spread = random.uniform(0.001, 0.03)

            snapshots.append(
                {
                    "token_id": token_id,
                    "price": price,
                    "volume_24h": volume_24h,
                    "spread": spread,
                    # snapshots table default ts likely exists; if your insert uses explicit ts, add it
                    # "ts": ts,
                }
            )

    return MarketQueries.insert_snapshots_batch(snapshots)


def run_simulated_loop(
    n_markets: int = 30,
    every_seconds: int = 15,
    stop_flag: callable = None,
) -> None:
    """
    Run simulated sync loop.
    
    Args:
        n_markets: Number of simulated markets to create
        every_seconds: Interval between snapshot writes
        stop_flag: Optional callable that returns True to stop the loop
    """
    sim = seed_simulated_markets(n_markets=n_markets)

    while True:
        # Check stop flag if provided
        if stop_flag is not None and stop_flag():
            print("[simulated] Stop flag set, exiting loop")
            break
            
        inserted = write_simulated_snapshots(sim)
        print(f"[simulated] inserted_snapshots={inserted}")
        time.sleep(every_seconds)
