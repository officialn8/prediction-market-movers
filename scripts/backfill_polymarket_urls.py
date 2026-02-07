#!/usr/bin/env python3
"""
Backfill canonical Polymarket URLs in markets table.

Usage:
  Dry run:
    python scripts/backfill_polymarket_urls.py --max-markets 20000

  Apply updates:
    python scripts/backfill_polymarket_urls.py --max-markets 20000 --apply
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Add project root to path for direct script execution.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.collector.adapters.polymarket import PolymarketAdapter
from packages.core.storage import get_db_pool


logger = logging.getLogger("backfill_polymarket_urls")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill canonical Polymarket market URLs")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=20000,
        help="Maximum active Polymarket markets to fetch from Gamma API",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates to DB (default is dry-run)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def _fetch_canonical_url_map(adapter: PolymarketAdapter, max_markets: int) -> dict[str, str]:
    """Fetch active Polymarket markets and map source_id -> canonical URL."""
    logger.info("Fetching active Polymarket markets (max_markets=%s)...", max_markets)
    markets = adapter.fetch_all_markets(max_markets=max_markets, active=True)
    logger.info("Fetched %s active markets from API.", len(markets))

    url_map: dict[str, str] = {}
    for market in markets:
        canonical_url = (market.url or "").strip()
        if canonical_url:
            url_map[str(market.condition_id)] = canonical_url

    logger.info("Built canonical URL map for %s markets.", len(url_map))
    return url_map


def run(max_markets: int, apply: bool) -> dict[str, int]:
    db = get_db_pool()
    if not db.health_check():
        raise RuntimeError("Database not available")

    adapter = PolymarketAdapter()
    try:
        url_map = _fetch_canonical_url_map(adapter, max_markets=max_markets)

        rows = db.execute(
            """
            SELECT source_id, url
            FROM markets
            WHERE source = 'polymarket'
            """,
            fetch=True,
        ) or []

        stats = {
            "scanned": 0,
            "matched": 0,
            "missing": 0,
            "unchanged": 0,
            "updated": 0,
        }

        update_params: list[tuple[str, str, str]] = []
        for row in rows:
            stats["scanned"] += 1
            source_id = str(row.get("source_id") or "")
            current_url = (row.get("url") or "").strip()

            canonical_url = url_map.get(source_id)
            if not canonical_url:
                stats["missing"] += 1
                continue

            stats["matched"] += 1
            if current_url == canonical_url:
                stats["unchanged"] += 1
                continue

            update_params.append((canonical_url, source_id, canonical_url))

        if apply and update_params:
            updated = db.execute_many(
                """
                UPDATE markets
                SET url = %s, updated_at = NOW()
                WHERE source = 'polymarket'
                  AND source_id = %s
                  AND COALESCE(url, '') <> %s
                """,
                update_params,
            )
            stats["updated"] = max(updated, 0)
        else:
            stats["updated"] = len(update_params)

        mode = "APPLY" if apply else "DRY-RUN"
        logger.info(
            "%s summary: scanned=%s matched=%s missing=%s unchanged=%s %s=%s",
            mode,
            stats["scanned"],
            stats["matched"],
            stats["missing"],
            stats["unchanged"],
            "updated" if apply else "would_update",
            stats["updated"],
        )
        return stats
    finally:
        adapter.close()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    try:
        run(max_markets=args.max_markets, apply=args.apply)
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
