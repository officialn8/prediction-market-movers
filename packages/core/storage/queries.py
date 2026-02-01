"""
Raw SQL queries for market data operations.
Centralized query definitions for consistency and maintainability.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from packages.core.storage.db import get_db_pool


@dataclass
class MarketQueries:
    """
    SQL query methods for market operations.
    Uses raw SQL for performance and explicit control.
    """
    
    # =========================================================================
    # MARKET OPERATIONS
    # =========================================================================
    
    @staticmethod
    def upsert_market(
        source: str,
        source_id: str,
        title: str,
        category: Optional[str] = None,
        end_date: Optional[str] = None,
        status: str = "active",
        url: Optional[str] = None,
    ) -> dict:
        """
        Insert or update a market (upsert by source + source_id).
        
        Returns:
            The upserted market record as a dict
        """
        db = get_db_pool()
        query = """
            INSERT INTO markets (source, source_id, title, category, end_date, status, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, source_id) 
            DO UPDATE SET 
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                end_date = EXCLUDED.end_date,
                status = EXCLUDED.status,
                url = EXCLUDED.url,
                updated_at = NOW()
            RETURNING *
        """
        result = db.execute(
            query,
            (source, source_id, title, category, end_date, status, url),
            fetch=True,
        )
        return result[0] if result else {}
    
    @staticmethod
    def get_market_by_source(source: str, source_id: str) -> Optional[dict]:
        """Get a market by its source and source_id."""
        db = get_db_pool()
        query = """
            SELECT * FROM markets 
            WHERE source = %s AND source_id = %s
        """
        result = db.execute(query, (source, source_id), fetch=True)
        return result[0] if result else None
    
    @staticmethod
    def get_active_markets(
        source: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get active markets with optional filtering."""
        db = get_db_pool()
        
        conditions = ["status = 'active'"]
        params = []
        
        if source:
            conditions.append("source = %s")
            params.append(source)
        if category:
            conditions.append("category = %s")
            params.append(category)
        
        params.append(limit)
        
        query = f"""
            SELECT * FROM markets 
            WHERE {' AND '.join(conditions)}
            ORDER BY updated_at DESC
            LIMIT %s
        """
        return db.execute(query, tuple(params), fetch=True) or []
    
    @staticmethod
    def search_markets(query: str, limit: int = 20) -> list[dict]:
        """Search markets by title or category."""
        db = get_db_pool()
        sql = """
            SELECT m.*
            FROM markets m
            WHERE (m.title ILIKE %s OR m.category ILIKE %s)
            AND m.status = 'active'
            LIMIT %s
        """
        search_term = f"%{query}%"
        return db.execute(sql, (search_term, search_term, limit), fetch=True) or []
    
    # =========================================================================
    # TOKEN OPERATIONS
    # =========================================================================
    
    @staticmethod
    def upsert_token(
        market_id: UUID,
        outcome: str,
        symbol: Optional[str] = None,
        source_token_id: Optional[str] = None,
    ) -> dict:
        """
        Insert or update a market token.
        
        Returns:
            The upserted token record
        """
        db = get_db_pool()
        query = """
            INSERT INTO market_tokens (market_id, outcome, symbol, source_token_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (market_id, outcome)
            DO UPDATE SET 
                symbol = EXCLUDED.symbol,
                source_token_id = EXCLUDED.source_token_id
            RETURNING *
        """
        result = db.execute(
            query,
            (str(market_id), outcome, symbol, source_token_id),
            fetch=True,
        )
        return result[0] if result else {}
    
    @staticmethod
    def get_tokens_for_market(market_id: UUID) -> list[dict]:
        """Get all tokens for a specific market."""
        db = get_db_pool()
        query = """
            SELECT * FROM market_tokens 
            WHERE market_id = %s
            ORDER BY outcome
        """
        return db.execute(query, (str(market_id),), fetch=True) or []
    
    # =========================================================================
    # SNAPSHOT OPERATIONS
    # =========================================================================
    
    @staticmethod
    def insert_snapshot(
        token_id: UUID,
        price: Decimal,
        volume_24h: Optional[Decimal] = None,
        spread: Optional[Decimal] = None,
        ts: Optional[datetime] = None,
    ) -> dict:
        """
        Insert a new price snapshot (append-only).
        
        Returns:
            The inserted snapshot record
        """
        db = get_db_pool()
        
        if ts:
            query = """
                INSERT INTO snapshots (ts, token_id, price, volume_24h, spread)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
            """
            params = (ts, str(token_id), price, volume_24h, spread)
        else:
            query = """
                INSERT INTO snapshots (token_id, price, volume_24h, spread)
                VALUES (%s, %s, %s, %s)
                RETURNING *
            """
            params = (str(token_id), price, volume_24h, spread)
        
        result = db.execute(query, params, fetch=True)
        return result[0] if result else {}
    
    @staticmethod
    def insert_snapshots_batch(
        snapshots: list[dict],
    ) -> int:
        """
        Batch insert multiple snapshots efficiently.
        
        Args:
            snapshots: List of dicts with keys: token_id, price, volume_24h, spread
            
        Returns:
            Number of rows inserted
        """
        if not snapshots:
            return 0
        
        db = get_db_pool()
        query = """
            INSERT INTO snapshots (token_id, price, volume_24h, spread)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        params_seq = [
            (
                str(s["token_id"]),
                s["price"],
                s.get("volume_24h"),
                s.get("spread"),
            )
            for s in snapshots
        ]
        return db.execute_many(query, params_seq)
    
    @staticmethod
    def get_latest_snapshot(token_id: UUID) -> Optional[dict]:
        """Get the most recent snapshot for a token."""
        db = get_db_pool()
        query = """
            SELECT * FROM snapshots 
            WHERE token_id = %s
            ORDER BY ts DESC
            LIMIT 1
        """
        result = db.execute(query, (str(token_id),), fetch=True)
        return result[0] if result else None
    
    @staticmethod
    def get_snapshots_range(
        token_id: UUID,
        start_ts: datetime,
        end_ts: Optional[datetime] = None,
    ) -> list[dict]:
        """Get snapshots for a token within a time range."""
        db = get_db_pool()
        
        if end_ts:
            query = """
                SELECT * FROM snapshots 
                WHERE token_id = %s AND ts >= %s AND ts <= %s
                ORDER BY ts ASC
            """
            params = (str(token_id), start_ts, end_ts)
        else:
            query = """
                SELECT * FROM snapshots 
                WHERE token_id = %s AND ts >= %s
                ORDER BY ts ASC
            """
            params = (str(token_id), start_ts)
        
        return db.execute(query, params, fetch=True) or []
    
    # =========================================================================
    # TOP MOVERS QUERIES
    # =========================================================================
    
    @staticmethod
    def get_top_movers(
        hours: int = 24,
        limit: int = 20,
        source: Optional[str] = None,
        category: Optional[str] = None,
        direction: str = "both",
    ) -> list[dict]:
        """
        Get top price movers over a given time period.
        Uses WSS trade volumes when available, falls back to Gamma API volumes.
        """
        db = get_db_pool()

        source_filter = "AND m.source = %s" if source else ""
        category_filter = "AND m.category = %s" if category else ""

        if direction == "gainers":
            direction_filter = "AND pct_change > 0"
            order = "pct_change DESC"
        elif direction == "losers":
            direction_filter = "AND pct_change < 0"
            order = "pct_change ASC"
        else:
            direction_filter = ""
            order = "ABS(pct_change) DESC"
        
        # Filter out markets expiring very soon
        expiry_filter = "AND (m.end_date IS NULL OR m.end_date > NOW() + INTERVAL '24 hours')"

        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    ts as latest_ts,
                    price as latest_price
                FROM snapshots
                ORDER BY token_id, ts DESC
            ),
            latest_volumes AS (
                -- Get latest volume with WSS preference
                SELECT 
                    token_id,
                    volume_24h as latest_volume,
                    volume_source,
                    wss_trade_count
                FROM v_latest_volumes
                WHERE has_volume_data = true
            ),
            historical AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    price as old_price
                FROM snapshots
                WHERE ts <= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY token_id, ts DESC
            ),
            changes AS (
                SELECT
                    l.token_id,
                    l.latest_ts,
                    l.latest_price,
                    COALESCE(lv.latest_volume, 0) as latest_volume,
                    lv.volume_source,
                    lv.wss_trade_count,
                    h.old_price,
                    -- Use percentage points (pp) instead of percentage change
                    -- PP is more meaningful for prediction markets (bounded -100 to +100)
                    ROUND(((l.latest_price - h.old_price) * 100)::numeric, 2) as pct_change
                FROM latest l
                JOIN historical h ON l.token_id = h.token_id
                LEFT JOIN latest_volumes lv ON l.token_id = lv.token_id
            )
            SELECT
                c.*,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url
            FROM changes c
            JOIN market_tokens mt ON c.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE m.status = 'active'
              {source_filter}
              {category_filter}
              {direction_filter}
              {expiry_filter}
            ORDER BY {order}
            LIMIT %s
        """

        params = [hours]
        if source:
            params.append(source)
        if category:
            params.append(category)
        params.append(limit)

        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_movers_window(
        window_seconds: int,
        limit: int = 20,
        source: Optional[str] = None,
        category: Optional[str] = None,
        direction: str = "both",
    ) -> list[dict]:
        """
        Get top price movers over an arbitrary time window in seconds.
        Uses WSS trade volumes when available, falls back to Gamma API.
        
        Returns raw metrics only - scoring is done in Python via MoverScorer
        for consistency across all code paths (cache, alerts, real-time).
        
        Args:
            window_seconds: Time window in seconds
            limit: Max results to return (applied in SQL)
            source: Optional source filter
            category: Optional category filter  
            direction: 'both', 'gainers', or 'losers'
        """
        db = get_db_pool()

        source_filter = "AND m.source = %s" if source else ""
        category_filter = "AND m.category = %s" if category else ""

        if direction == "gainers":
            direction_filter = "AND pct_change > 0"
            order = "pct_change DESC"
        elif direction == "losers":
            direction_filter = "AND pct_change < 0"
            order = "pct_change ASC"
        else:
            direction_filter = ""
            order = "ABS(pct_change) DESC"
        
        # Filter markets ending very soon
        expiry_filter = "AND (m.end_date IS NULL OR m.end_date > NOW() + INTERVAL '24 hours')"

        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    ts as latest_ts,
                    price as latest_price
                FROM snapshots
                ORDER BY token_id, ts DESC
            ),
            latest_volumes AS (
                -- Get latest volume with WSS preference
                SELECT 
                    token_id,
                    volume_24h as latest_volume,
                    volume_source,
                    wss_trade_count
                FROM v_latest_volumes
                WHERE has_volume_data = true
            ),
            historical AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    price as old_price
                FROM snapshots
                WHERE ts <= NOW() - (%s * INTERVAL '1 second')
                ORDER BY token_id, ts DESC
            ),
            changes AS (
                SELECT
                    l.token_id,
                    l.latest_ts,
                    l.latest_price,
                    COALESCE(lv.latest_volume, 0) as latest_volume,
                    lv.volume_source,
                    lv.wss_trade_count,
                    h.old_price,
                    -- Use percentage points (pp) instead of percentage change
                    ROUND(((l.latest_price - h.old_price) * 100)::numeric, 2) as pct_change
                FROM latest l
                JOIN historical h ON l.token_id = h.token_id
                LEFT JOIN latest_volumes lv ON l.token_id = lv.token_id
            )
            SELECT
                c.*,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url
            FROM changes c
            JOIN market_tokens mt ON c.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE m.status = 'active'
              {source_filter}
              {category_filter}
              {direction_filter}
              {expiry_filter}
            ORDER BY {order}
            LIMIT %s
        """

        params = [window_seconds]
        if source:
            params.append(source)
        if category:
            params.append(category)
        params.append(limit)

        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_markets_batch_with_prices(market_ids: list[str]) -> list[dict]:
        """
        Get details for multiple markets efficiently.
        Returns market details with tokens and latest prices.
        """
        if not market_ids:
            return []
            
        db = get_db_pool()
        query = """
            SELECT 
                m.*,
                json_agg(
                    json_build_object(
                        'token_id', mt.token_id,
                        'outcome', mt.outcome,
                        'symbol', mt.symbol,
                        'latest_price', (
                            SELECT price FROM snapshots 
                            WHERE token_id = mt.token_id 
                            ORDER BY ts DESC LIMIT 1
                        ),
                        'latest_volume', (
                            SELECT volume_24h FROM snapshots 
                            WHERE token_id = mt.token_id 
                              AND volume_24h IS NOT NULL
                            ORDER BY ts DESC LIMIT 1
                        )
                    )
                ) as tokens
            FROM markets m
            LEFT JOIN market_tokens mt ON m.market_id = mt.market_id
            WHERE m.market_id = ANY(%s::uuid[])
            GROUP BY m.market_id
        """
        return db.execute(query, (market_ids,), fetch=True) or []

    @staticmethod
    def get_category_stats(hours: int = 24) -> list[dict]:
        """
        Get aggregated statistics by category.
        """
        db = get_db_pool()
        
        # We reuse the logic from top movers to get moves, but aggregate
        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    price as latest_price,
                    volume_24h as latest_volume
                FROM snapshots
                ORDER BY token_id, ts DESC
            ),
            historical AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    price as old_price
                FROM snapshots
                WHERE ts <= NOW() - (%s * INTERVAL '1 hour')
                ORDER BY token_id, ts DESC
            ),
            changes AS (
                SELECT
                    l.token_id,
                    l.latest_volume,
                    ABS((l.latest_price - h.old_price) / h.old_price * 100) as abs_change
                FROM latest l
                JOIN historical h ON l.token_id = h.token_id
                WHERE h.old_price > 0
            )
            SELECT
                m.category,
                COUNT(DISTINCT m.market_id) as market_count,
                AVG(c.abs_change) as avg_abs_move,
                SUM(c.latest_volume) as total_volume
            FROM changes c
            JOIN market_tokens mt ON c.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE m.status = 'active'
            GROUP BY m.category
            HAVING COUNT(DISTINCT m.market_id) > 2
            ORDER BY avg_abs_move DESC
        """
        return db.execute(query, (hours,), fetch=True) or []
    
    @staticmethod
    def get_market_with_tokens_and_latest_prices(market_id: UUID) -> Optional[dict]:
        """
        Get full market details with tokens and latest prices.
        Used for market detail view.
        """
        db = get_db_pool()
        query = """
            SELECT 
                m.*,
                json_agg(
                    json_build_object(
                        'token_id', mt.token_id,
                        'outcome', mt.outcome,
                        'symbol', mt.symbol,
                        'latest_price', (
                            SELECT price FROM snapshots 
                            WHERE token_id = mt.token_id 
                            ORDER BY ts DESC LIMIT 1
                        ),
                        'latest_volume', (
                            SELECT volume_24h FROM snapshots 
                            WHERE token_id = mt.token_id 
                              AND volume_24h IS NOT NULL
                            ORDER BY ts DESC LIMIT 1
                        )
                    )
                ) as tokens
            FROM markets m
            LEFT JOIN market_tokens mt ON m.market_id = mt.market_id
            WHERE m.market_id = %s
            GROUP BY m.market_id
        """
        result = db.execute(query, (str(market_id),), fetch=True)
        return result[0] if result else None


@dataclass
class AnalyticsQueries:
    """
    SQL query methods for analytics and alerts.
    """
    
    @staticmethod
    def insert_movers_batch(movers: list[dict]) -> int:
        """
        Batch insert precomputed top movers.
        
        Args:
            movers: List of dicts matching MoverCache model fields
        """
        if not movers:
            return 0
            
        db = get_db_pool()
        query = """
            INSERT INTO movers_cache (
                as_of_ts, window_seconds, token_id, 
                price_now, price_then, move_pp, abs_move_pp, 
                rank, quality_score, volume_24h, spike_ratio
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (as_of_ts, window_seconds, rank) DO NOTHING
        """
        
        params_seq = [
            (
                m["as_of_ts"],
                m["window_seconds"],
                str(m["token_id"]),
                m["price_now"],
                m["price_then"],
                m["move_pp"],
                m["abs_move_pp"],
                m["rank"],
                m.get("quality_score"),
                m.get("volume_24h"),
                m.get("spike_ratio"),
            )
            for m in movers
        ]
        
        return db.execute_many(query, params_seq)

    @staticmethod
    def insert_alert(
        token_id: UUID,
        window_seconds: int,
        move_pp: Decimal,
        threshold_pp: Decimal,
        reason: str,
    ) -> dict:
        """Insert a new alert."""
        db = get_db_pool()
        query = """
            INSERT INTO alerts (
                token_id, window_seconds, move_pp, 
                threshold_pp, reason
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
        """
        result = db.execute(
            query,
            (str(token_id), window_seconds, move_pp, threshold_pp, reason),
            fetch=True,
        )
        return result[0] if result else {}

    @staticmethod
    def get_recent_alerts(limit: int = 50, unconverged_only: bool = False) -> list[dict]:
        """Get recent alerts."""
        db = get_db_pool()
        filter_clause = "WHERE acknowledged_at IS NULL" if unconverged_only else ""
        
        query = f"""
            SELECT 
                a.*,
                mt.outcome,
                mt.symbol,
                m.title as market_title,
                m.source
            FROM alerts a
            JOIN market_tokens mt ON a.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            {filter_clause}
            ORDER BY a.created_at DESC
            LIMIT %s
        """
        return db.execute(query, (limit,), fetch=True) or []

    @staticmethod
    def get_recent_alert_for_token(
        token_id: UUID, 
        window_seconds: int = 3600,
        lookback_minutes: int = 30
    ) -> Optional[dict]:
        """
        Check if an alert was generated for this token + window recently.
        
        Args:
            token_id: The token UUID
            window_seconds: The alert window size (e.g. 3600 for 1h movers)
            lookback_minutes: How far back to check for existing alerts (cooldown)
        """
        db = get_db_pool()
        query = """
            SELECT * FROM alerts 
            WHERE token_id = %s 
              AND window_seconds = %s
              AND created_at > NOW() - (%s * INTERVAL '1 minute')
            ORDER BY created_at DESC
            LIMIT 1
        """
        result = db.execute(
            query, 
            (str(token_id), window_seconds, lookback_minutes), 
            fetch=True
        )
        return result[0] if result else None

    @staticmethod
    def get_cached_movers(
        window_seconds: int = 3600,
        limit: int = 20,
        source: Optional[str] = None,
        category: Optional[str] = None,
        direction: str = "both",
    ) -> list[dict]:
        """
        Get top movers from the cache (fast).
        Includes join for market details.
        """
        db = get_db_pool()
        
        source_filter = "AND m.source = %s" if source else ""
        category_filter = "AND m.category = %s" if category else ""
        
        if direction == "gainers":
            direction_filter = "AND mc.move_pp > 0"
        elif direction == "losers":
            direction_filter = "AND mc.move_pp < 0"
        else:
            direction_filter = ""
            
        # Get the latest cached batch for this window
        # Now includes volume_24h and spike_ratio from cache for consistent display
        query = f"""
            WITH latest_batch AS (
                SELECT MAX(as_of_ts) as max_ts
                FROM movers_cache
                WHERE window_seconds = %s
            ),
            latest_volume AS (
                -- Fallback: Get latest NON-NULL volume if not in cache
                SELECT DISTINCT ON (token_id)
                    token_id,
                    volume_24h as latest_volume
                FROM snapshots
                WHERE volume_24h IS NOT NULL
                ORDER BY token_id, ts DESC
            )
            SELECT 
                mc.*,
                mc.move_pp as pct_change, -- Alias for compat
                mc.price_now as latest_price,
                mc.price_then as old_price,
                -- Use cached volume if available, else fallback to snapshot
                COALESCE(mc.volume_24h, lv.latest_volume, 0) as latest_volume,
                -- Expose spike_ratio directly from cache
                mc.spike_ratio as cached_spike_ratio,
                mt.market_id,
                mt.outcome,
                mt.symbol,
                m.title,
                m.source,
                m.category,
                m.url
            FROM movers_cache mc
            JOIN latest_batch lb ON mc.as_of_ts = lb.max_ts
            JOIN market_tokens mt ON mc.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            LEFT JOIN latest_volume lv ON mc.token_id = lv.token_id
            WHERE mc.window_seconds = %s
              {source_filter}
              {category_filter}
              {direction_filter}
            ORDER BY mc.rank ASC -- Pre-calculated rank
            LIMIT %s
        """
        
        params = [window_seconds, window_seconds]
        if source:
            params.append(source)
        if category:
            params.append(category)
        params.append(limit)

        return db.execute(query, tuple(params), fetch=True) or []


@dataclass
class UserAlertsQueries:
    """
    SQL query methods for user-defined custom alerts.
    """

    @staticmethod
    def create_user_alert(
        session_id: str,
        market_id: UUID,
        token_id: UUID,
        condition_type: str,
        threshold: float,
        window_seconds: Optional[int] = None,
        notify_once: bool = False,
    ) -> dict:
        """Create a new user-defined alert."""
        db = get_db_pool()
        query = """
            INSERT INTO user_alerts (
                session_id, market_id, token_id,
                condition_type, threshold, window_seconds, notify_once
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        result = db.execute(
            query,
            (session_id, str(market_id), str(token_id), condition_type, threshold, window_seconds, notify_once),
            fetch=True,
        )
        return result[0] if result else {}

    @staticmethod
    def get_user_alerts(session_id: str, active_only: bool = True) -> list[dict]:
        """Get all alerts for a user session."""
        db = get_db_pool()
        active_filter = "AND ua.is_active = true" if active_only else ""
        query = f"""
            SELECT
                ua.*,
                m.title as market_title,
                m.source,
                mt.outcome,
                (SELECT price FROM snapshots WHERE token_id = ua.token_id ORDER BY ts DESC LIMIT 1) as current_price
            FROM user_alerts ua
            JOIN markets m ON ua.market_id = m.market_id
            JOIN market_tokens mt ON ua.token_id = mt.token_id
            WHERE ua.session_id = %s
              {active_filter}
            ORDER BY ua.created_at DESC
        """
        return db.execute(query, (session_id,), fetch=True) or []

    @staticmethod
    def get_active_user_alerts() -> list[dict]:
        """Get all active user alerts (for background checking)."""
        db = get_db_pool()
        query = """
            SELECT
                ua.*,
                m.title as market_title,
                mt.outcome,
                (SELECT price FROM snapshots WHERE token_id = ua.token_id ORDER BY ts DESC LIMIT 1) as current_price
            FROM user_alerts ua
            JOIN markets m ON ua.market_id = m.market_id
            JOIN market_tokens mt ON ua.token_id = mt.token_id
            WHERE ua.is_active = true
        """
        return db.execute(query, fetch=True) or []

    @staticmethod
    def delete_user_alert(alert_id: UUID, session_id: str) -> bool:
        """Delete a user alert (verifying ownership via session_id)."""
        db = get_db_pool()
        query = """
            DELETE FROM user_alerts
            WHERE alert_id = %s AND session_id = %s
            RETURNING alert_id
        """
        result = db.execute(query, (str(alert_id), session_id), fetch=True)
        return bool(result)

    @staticmethod
    def deactivate_user_alert(alert_id: UUID) -> bool:
        """Deactivate an alert (called when notify_once triggers)."""
        db = get_db_pool()
        query = """
            UPDATE user_alerts
            SET is_active = false
            WHERE alert_id = %s
            RETURNING alert_id
        """
        result = db.execute(query, (str(alert_id),), fetch=True)
        return bool(result)

    @staticmethod
    def record_alert_trigger(alert_id: UUID, current_price: float, threshold: float, message: str) -> dict:
        """Record that an alert was triggered and create notification."""
        db = get_db_pool()
        # Update the alert
        db.execute("""
            UPDATE user_alerts
            SET last_triggered = NOW(), trigger_count = trigger_count + 1
            WHERE alert_id = %s
        """, (str(alert_id),))

        # Create notification
        query = """
            INSERT INTO user_alert_notifications (
                user_alert_id, current_price, threshold_price, message
            )
            VALUES (%s, %s, %s, %s)
            RETURNING *
        """
        result = db.execute(query, (str(alert_id), current_price, threshold, message), fetch=True)
        return result[0] if result else {}

    @staticmethod
    def get_user_notifications(session_id: str, unacknowledged_only: bool = True, limit: int = 50) -> list[dict]:
        """Get notifications for a user's alerts."""
        db = get_db_pool()
        ack_filter = "AND n.acknowledged = false" if unacknowledged_only else ""
        query = f"""
            SELECT
                n.*,
                ua.condition_type,
                ua.threshold,
                m.title as market_title,
                mt.outcome
            FROM user_alert_notifications n
            JOIN user_alerts ua ON n.user_alert_id = ua.alert_id
            JOIN markets m ON ua.market_id = m.market_id
            JOIN market_tokens mt ON ua.token_id = mt.token_id
            WHERE ua.session_id = %s
              {ack_filter}
            ORDER BY n.triggered_at DESC
            LIMIT %s
        """
        return db.execute(query, (session_id, limit), fetch=True) or []

    @staticmethod
    def acknowledge_notification(notification_id: UUID) -> bool:
        """Mark a notification as acknowledged."""
        db = get_db_pool()
        query = """
            UPDATE user_alert_notifications
            SET acknowledged = true
            WHERE notification_id = %s
            RETURNING notification_id
        """
        result = db.execute(query, (str(notification_id),), fetch=True)
        return bool(result)

    @staticmethod
    def acknowledge_all_notifications(session_id: str) -> int:
        """Acknowledge all notifications for a session."""
        db = get_db_pool()
        query = """
            UPDATE user_alert_notifications n
            SET acknowledged = true
            FROM user_alerts ua
            WHERE n.user_alert_id = ua.alert_id
              AND ua.session_id = %s
              AND n.acknowledged = false
        """
        return db.execute(query, (session_id,))


@dataclass
class OHLCQueries:
    """
    SQL query methods for OHLC candle data.
    Used for efficient charting on longer timeframes.
    """

    @staticmethod
    def get_1m_candles(
        token_id: UUID,
        start_ts: Optional[datetime] = None,
        end_ts: Optional[datetime] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Get 1-minute OHLC candles for a token."""
        db = get_db_pool()

        conditions = ["token_id = %s"]
        params = [str(token_id)]

        if start_ts:
            conditions.append("bucket_ts >= %s")
            params.append(start_ts)
        if end_ts:
            conditions.append("bucket_ts <= %s")
            params.append(end_ts)

        params.append(limit)

        query = f"""
            SELECT bucket_ts as ts, open, high, low, close, volume
            FROM ohlc_1m
            WHERE {' AND '.join(conditions)}
            ORDER BY bucket_ts ASC
            LIMIT %s
        """
        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_1h_candles(
        token_id: UUID,
        start_ts: Optional[datetime] = None,
        end_ts: Optional[datetime] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Get 1-hour OHLC candles for a token."""
        db = get_db_pool()

        conditions = ["token_id = %s"]
        params = [str(token_id)]

        if start_ts:
            conditions.append("bucket_ts >= %s")
            params.append(start_ts)
        if end_ts:
            conditions.append("bucket_ts <= %s")
            params.append(end_ts)

        params.append(limit)

        query = f"""
            SELECT bucket_ts as ts, open, high, low, close, volume
            FROM ohlc_1h
            WHERE {' AND '.join(conditions)}
            ORDER BY bucket_ts ASC
            LIMIT %s
        """
        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_candles_for_timeframe(
        token_id: UUID,
        start_ts: datetime,
        end_ts: Optional[datetime] = None,
        hours: int = 24,
    ) -> list[dict]:
        """
        Intelligently select candle resolution based on timeframe.
        - < 6 hours: Use raw snapshots
        - 6-48 hours: Use 1m candles
        - > 48 hours: Use 1h candles
        """
        if hours < 6:
            # Use raw snapshots for short timeframes
            return MarketQueries.get_snapshots_range(token_id, start_ts, end_ts)
        elif hours <= 48:
            # Use 1-minute candles
            return OHLCQueries.get_1m_candles(token_id, start_ts, end_ts)
        else:
            # Use 1-hour candles for longer timeframes
            return OHLCQueries.get_1h_candles(token_id, start_ts, end_ts)


@dataclass
class VolumeQueries:
    """
    SQL query methods for volume analysis and spike detection.
    """
    
    @staticmethod
    def accumulate_trade_volume(token_id: UUID, volume: Decimal, trade_ts: Optional[datetime] = None) -> None:
        """
        Accumulate trade volume for a token using the stored function.
        This updates all rolling windows (5m, 15m, 1h, 24h) in trade_volumes table.
        
        Args:
            token_id: The token UUID
            volume: Trade notional volume (size * price)
            trade_ts: Optional trade timestamp (defaults to NOW())
        """
        db = get_db_pool()
        query = "SELECT accumulate_trade_volume(%s, %s, %s)"
        params = (str(token_id), volume, trade_ts or datetime.utcnow())
        db.execute(query, params)
    
    @staticmethod
    def get_latest_volume(token_id: UUID) -> Optional[dict]:
        """
        Get latest volume for a token, preferring WSS trade volumes over Gamma API.
        
        Returns:
            Dict with volume_24h, volume_source, has_volume_data, etc.
        """
        db = get_db_pool()
        query = """
            SELECT 
                volume_24h,
                volume_source,
                has_volume_data,
                wss_trade_count,
                wss_updated_at,
                gamma_updated_at
            FROM v_latest_volumes
            WHERE token_id = %s
        """
        result = db.execute(query, (str(token_id),), fetch=True)
        return result[0] if result else None
    
    @staticmethod
    def get_latest_volumes_for_tokens(token_ids: list[UUID]) -> list[dict]:
        """
        Get latest volumes for multiple tokens efficiently.
        
        Args:
            token_ids: List of token UUIDs
            
        Returns:
            List of volume data with WSS preference
        """
        if not token_ids:
            return []
            
        db = get_db_pool()
        placeholders = ','.join(['%s'] * len(token_ids))
        query = f"""
            SELECT 
                token_id,
                volume_24h,
                volume_source,
                has_volume_data,
                wss_trade_count,
                wss_updated_at,
                gamma_updated_at
            FROM v_latest_volumes
            WHERE token_id IN ({placeholders})
        """
        params = [str(tid) for tid in token_ids]
        return db.execute(query, tuple(params), fetch=True) or []
    
    @staticmethod
    def get_top_volumes(limit: int = 50, source: Optional[str] = None) -> list[dict]:
        """
        Get tokens with highest 24h volume, preferring WSS data.
        
        Args:
            limit: Max results to return
            source: Optional source filter ('polymarket', 'kalshi')
            
        Returns:
            List of tokens with volume data sorted by volume DESC
        """
        db = get_db_pool()
        
        source_filter = "AND m.source = %s" if source else ""
        params = []
        if source:
            params.append(source)
        params.append(limit)
        
        query = f"""
            SELECT 
                mt.token_id,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url,
                v.volume_24h,
                v.volume_source,
                v.wss_trade_count,
                v.wss_updated_at,
                v.gamma_updated_at
            FROM v_latest_volumes v
            JOIN market_tokens mt ON v.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE v.has_volume_data = true
              AND v.volume_24h > 0
              {source_filter}
            ORDER BY v.volume_24h DESC
            LIMIT %s
        """
        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_volume_averages(token_ids: Optional[list[UUID]] = None) -> list[dict]:
        """
        Get 7-day average volume for tokens.

        Uses the volume_averages view created in migration 007.
        Falls back to direct query if view doesn't exist.

        Args:
            token_ids: Optional list of specific tokens to query

        Returns:
            List of dicts with token_id, avg_volume_7d, stddev_volume_7d, etc.
        """
        db = get_db_pool()

        if token_ids:
            placeholders = ','.join(['%s'] * len(token_ids))
            query = f"""
                SELECT * FROM volume_averages
                WHERE token_id IN ({placeholders})
            """
            params = [str(tid) for tid in token_ids]
        else:
            query = "SELECT * FROM volume_averages"
            params = []

        try:
            return db.execute(query, tuple(params) if params else None, fetch=True) or []
        except Exception:
            # View might not exist yet, use fallback query
            return VolumeQueries._get_volume_averages_fallback(token_ids)

    @staticmethod
    def _get_volume_averages_fallback(token_ids: Optional[list[UUID]] = None) -> list[dict]:
        """Fallback query if volume_averages view doesn't exist."""
        db = get_db_pool()

        token_filter = ""
        params = []
        if token_ids:
            placeholders = ','.join(['%s'] * len(token_ids))
            token_filter = f"AND s.token_id IN ({placeholders})"
            params = [str(tid) for tid in token_ids]

        query = f"""
            WITH daily_volumes AS (
                SELECT DISTINCT ON (token_id, DATE(ts))
                    token_id,
                    volume_24h,
                    ts
                FROM snapshots
                WHERE ts > NOW() - INTERVAL '7 days'
                  AND volume_24h IS NOT NULL
                  AND volume_24h > 0
                  {token_filter}
                ORDER BY token_id, DATE(ts), ts DESC
            )
            SELECT
                token_id,
                AVG(volume_24h) as avg_volume_7d,
                STDDEV(volume_24h) as stddev_volume_7d,
                MAX(volume_24h) as max_volume_7d,
                MIN(volume_24h) as min_volume_7d,
                COUNT(*) as sample_count
            FROM daily_volumes
            GROUP BY token_id
            HAVING COUNT(*) >= 2
        """
        return db.execute(query, tuple(params) if params else None, fetch=True) or []

    @staticmethod
    def get_current_volumes(limit: int = 500) -> list[dict]:
        """
        Get current volume for all active tokens with market context.

        Returns latest volume snapshot for each token along with market info.
        """
        db = get_db_pool()

        query = """
            SELECT DISTINCT ON (mt.token_id)
                mt.token_id,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                s.volume_24h as current_volume,
                s.price as current_price,
                s.ts as snapshot_ts
            FROM market_tokens mt
            JOIN markets m ON mt.market_id = m.market_id
            JOIN snapshots s ON mt.token_id = s.token_id
            WHERE m.status = 'active'
              AND s.volume_24h IS NOT NULL
              AND (m.end_date IS NULL OR m.end_date > NOW() + INTERVAL '24 hours')
            ORDER BY mt.token_id, s.ts DESC
            LIMIT %s
        """
        return db.execute(query, (limit,), fetch=True) or []

    @staticmethod
    def get_volume_spike_candidates(
        min_spike_ratio: float = 2.0,
        min_volume: float = 1000.0,
        limit: int = 100,
    ) -> list[dict]:
        """
        Find tokens with volume significantly above their historical average.

        Args:
            min_spike_ratio: Minimum ratio of current/avg volume (e.g., 2.0 = 2x normal)
            min_volume: Minimum absolute volume to consider
            limit: Max results to return

        Returns:
            List of tokens with spike detected, sorted by spike_ratio DESC
        """
        db = get_db_pool()

        query = """
            WITH current AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    volume_24h as current_volume,
                    price as current_price,
                    ts
                FROM snapshots
                WHERE volume_24h IS NOT NULL
                ORDER BY token_id, ts DESC
            ),
            averages AS (
                SELECT
                    s.token_id,
                    AVG(s.volume_24h) as avg_volume,
                    STDDEV(s.volume_24h) as stddev_volume
                FROM (
                    SELECT DISTINCT ON (token_id, DATE(ts))
                        token_id,
                        volume_24h
                    FROM snapshots
                    WHERE ts > NOW() - INTERVAL '7 days'
                      AND ts < NOW() - INTERVAL '1 day'  -- Exclude last 24h from avg
                      AND volume_24h IS NOT NULL
                      AND volume_24h > 0
                    ORDER BY token_id, DATE(ts), ts DESC
                ) s
                GROUP BY s.token_id
                HAVING COUNT(*) >= 2
            ),
            spikes AS (
                SELECT
                    c.token_id,
                    c.current_volume,
                    c.current_price,
                    a.avg_volume,
                    a.stddev_volume,
                    CASE
                        WHEN a.avg_volume > 0 THEN c.current_volume / a.avg_volume
                        ELSE NULL
                    END as spike_ratio
                FROM current c
                JOIN averages a ON c.token_id = a.token_id
                WHERE c.current_volume >= %s
            )
            SELECT
                sp.*,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url
            FROM spikes sp
            JOIN market_tokens mt ON sp.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE sp.spike_ratio >= %s
              AND m.status = 'active'
              AND (m.end_date IS NULL OR m.end_date > NOW() + INTERVAL '24 hours')
            ORDER BY sp.spike_ratio DESC
            LIMIT %s
        """
        return db.execute(query, (min_volume, min_spike_ratio, limit), fetch=True) or []

    @staticmethod
    def insert_volume_spike(
        token_id: UUID,
        current_volume: Decimal,
        avg_volume: Decimal,
        spike_ratio: Decimal,
        current_price: Optional[Decimal] = None,
        price_change_1h: Optional[Decimal] = None,
        severity: str = "medium",
    ) -> dict:
        """Insert a detected volume spike record."""
        db = get_db_pool()

        query = """
            INSERT INTO volume_spikes (
                token_id, current_volume, avg_volume, spike_ratio,
                current_price, price_change_1h, severity
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        result = db.execute(
            query,
            (str(token_id), current_volume, avg_volume, spike_ratio,
             current_price, price_change_1h, severity),
            fetch=True,
        )
        return result[0] if result else {}

    @staticmethod
    def get_recent_volume_spikes(
        limit: int = 50,
        min_severity: str = "low",
        unacknowledged_only: bool = False,
    ) -> list[dict]:
        """Get recent volume spikes with market context."""
        db = get_db_pool()

        severity_order = {"low": 1, "medium": 2, "high": 3, "extreme": 4}
        min_level = severity_order.get(min_severity, 1)

        # Build severity filter
        valid_severities = [s for s, level in severity_order.items() if level >= min_level]
        severity_placeholders = ','.join(['%s'] * len(valid_severities))

        ack_filter = "AND vs.acknowledged_at IS NULL" if unacknowledged_only else ""

        query = f"""
            SELECT
                vs.*,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url
            FROM volume_spikes vs
            JOIN market_tokens mt ON vs.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE vs.severity IN ({severity_placeholders})
              {ack_filter}
            ORDER BY vs.created_at DESC
            LIMIT %s
        """
        params = valid_severities + [limit]
        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def get_recent_spike_for_token(
        token_id: UUID,
        lookback_minutes: int = 60,
    ) -> Optional[dict]:
        """
        Check if a volume spike was recently recorded for this token.
        Used for deduplication.
        """
        db = get_db_pool()

        query = """
            SELECT * FROM volume_spikes
            WHERE token_id = %s
              AND created_at > NOW() - (%s * INTERVAL '1 minute')
            ORDER BY created_at DESC
            LIMIT 1
        """
        result = db.execute(query, (str(token_id), lookback_minutes), fetch=True)
        return result[0] if result else None

    @staticmethod
    def get_movers_with_volume_context(
        window_seconds: int = 3600,
        limit: int = 50,
        source: Optional[str] = None,
    ) -> list[dict]:
        """
        Get movers with volume context for Python-side scoring.

        Returns raw metrics including avg_volume for spike detection.
        Scoring is done in Python via MoverScorer for consistency.
        """
        db = get_db_pool()

        source_filter = "AND m.source = %s" if source else ""
        expiry_filter = "AND (m.end_date IS NULL OR m.end_date > NOW() + INTERVAL '24 hours')"

        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    ts as latest_ts,
                    price as latest_price,
                    volume_24h as current_volume
                FROM snapshots
                ORDER BY token_id, ts DESC
            ),
            latest_volume AS (
                -- Get latest NON-NULL volume (from REST syncs, not WSS which has NULL volume)
                SELECT DISTINCT ON (token_id)
                    token_id,
                    volume_24h as latest_volume
                FROM snapshots
                WHERE volume_24h IS NOT NULL
                ORDER BY token_id, ts DESC
            ),
            historical AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    price as old_price
                FROM snapshots
                WHERE ts <= NOW() - (%s * INTERVAL '1 second')
                ORDER BY token_id, ts DESC
            ),
            volume_avgs AS (
                SELECT
                    token_id,
                    AVG(volume_24h) as avg_volume
                FROM (
                    SELECT DISTINCT ON (token_id, DATE(ts))
                        token_id,
                        volume_24h
                    FROM snapshots
                    WHERE ts > NOW() - INTERVAL '7 days'
                      AND ts < NOW() - INTERVAL '1 day'
                      AND volume_24h IS NOT NULL
                      AND volume_24h > 0
                    ORDER BY token_id, DATE(ts), ts DESC
                ) daily
                GROUP BY token_id
                HAVING COUNT(*) >= 2
            )
            SELECT
                l.token_id,
                l.latest_ts,
                l.latest_price,
                COALESCE(lv.latest_volume, l.current_volume, 0) as latest_volume,
                h.old_price,
                va.avg_volume,
                -- Use percentage points (pp) instead of percentage change
                ROUND(((l.latest_price - h.old_price) * 100)::numeric, 2) as pct_change,
                mt.market_id,
                mt.outcome,
                m.title,
                m.source,
                m.category,
                m.url
            FROM latest l
            JOIN historical h ON l.token_id = h.token_id
            LEFT JOIN latest_volume lv ON l.token_id = lv.token_id
            LEFT JOIN volume_avgs va ON l.token_id = va.token_id
            JOIN market_tokens mt ON l.token_id = mt.token_id
            JOIN markets m ON mt.market_id = m.market_id
            WHERE m.status = 'active'
              {source_filter}
              {expiry_filter}
            ORDER BY ABS(ROUND(((l.latest_price - h.old_price) * 100)::numeric, 2)) DESC
            LIMIT %s
        """

        params = [window_seconds]
        if source:
            params.append(source)
        params.append(limit)

        return db.execute(query, tuple(params), fetch=True) or []


@dataclass
class WatchlistQueries:
    """
    SQL query methods for user watchlist operations.
    """

    @staticmethod
    def get_all(user_session_id: str) -> list[dict]:
        """Get all watchlist items for a user session."""
        db = get_db_pool()
        query = """
            SELECT 
                uw.*,
                m.title,
                m.source,
                m.category,
                m.url
            FROM user_watchlist uw
            JOIN markets m ON uw.market_id = m.market_id
            WHERE uw.user_session_id = %s
            ORDER BY uw.added_at DESC
        """
        return db.execute(query, (user_session_id,), fetch=True) or []

    @staticmethod
    def add(user_session_id: str, market_id: str) -> None:
        """Add a market to the user's watchlist."""
        db = get_db_pool()
        query = """
            INSERT INTO user_watchlist (user_session_id, market_id)
            VALUES (%s, %s)
            ON CONFLICT (user_session_id, market_id) DO NOTHING
        """
        db.execute(query, (user_session_id, market_id))

    @staticmethod
    def remove(user_session_id: str, market_id: str) -> None:
        """Remove a market from the user's watchlist."""
        db = get_db_pool()
        query = """
            DELETE FROM user_watchlist
            WHERE user_session_id = %s AND market_id = %s
        """
        db.execute(query, (user_session_id, market_id))


@dataclass
class ArbitrageQueries:
    """
    SQL query methods for cross-platform arbitrage detection.
    Identifies opportunities when combined price of YES on one platform
    and NO on another is less than $1.
    """

    @staticmethod
    def upsert_market_pair(
        polymarket_market_id: UUID,
        kalshi_market_id: UUID,
        matching_method: str = "manual",
        similarity_score: Optional[Decimal] = None,
        notes: Optional[str] = None,
    ) -> dict:
        """
        Create or update a market pair for arbitrage detection.
        
        Args:
            polymarket_market_id: UUID of Polymarket market
            kalshi_market_id: UUID of Kalshi market
            matching_method: 'manual', 'fuzzy', or 'exact'
            similarity_score: 0-1 score for fuzzy matches
            notes: Description of the pairing
            
        Returns:
            The upserted market_pair record
        """
        db = get_db_pool()
        query = """
            INSERT INTO market_pairs (
                polymarket_market_id, kalshi_market_id, matching_method,
                similarity_score, notes
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (polymarket_market_id)
            DO UPDATE SET
                kalshi_market_id = EXCLUDED.kalshi_market_id,
                matching_method = EXCLUDED.matching_method,
                similarity_score = EXCLUDED.similarity_score,
                notes = EXCLUDED.notes,
                updated_at = NOW()
            RETURNING *
        """
        result = db.execute(
            query,
            (str(polymarket_market_id), str(kalshi_market_id),
             matching_method, similarity_score, notes),
            fetch=True,
        )
        return result[0] if result else {}

    @staticmethod
    def get_active_pairs() -> list[dict]:
        """Get all active market pairs with latest prices."""
        db = get_db_pool()
        query = """
            SELECT
                mp.pair_id,
                mp.polymarket_market_id,
                mp.kalshi_market_id,
                mp.matching_method,
                mp.similarity_score,
                mp.notes,
                
                -- Polymarket details
                m_poly.title as polymarket_title,
                m_poly.source_id as polymarket_source_id,
                m_poly.url as polymarket_url,
                
                -- Kalshi details
                m_kalshi.title as kalshi_title,
                m_kalshi.source_id as kalshi_source_id,
                m_kalshi.url as kalshi_url,
                
                -- Latest Polymarket prices (YES token)
                (
                    SELECT price FROM snapshots s
                    JOIN market_tokens mt ON s.token_id = mt.token_id
                    WHERE mt.market_id = mp.polymarket_market_id
                      AND mt.outcome = 'Yes'
                    ORDER BY s.ts DESC LIMIT 1
                ) as polymarket_yes_price,
                
                -- Latest Kalshi prices (YES token)
                (
                    SELECT price FROM snapshots s
                    JOIN market_tokens mt ON s.token_id = mt.token_id
                    WHERE mt.market_id = mp.kalshi_market_id
                      AND mt.outcome = 'Yes'
                    ORDER BY s.ts DESC LIMIT 1
                ) as kalshi_yes_price,
                
                -- 24h volumes
                (
                    SELECT volume_24h FROM v_latest_volumes v
                    JOIN market_tokens mt ON v.token_id = mt.token_id
                    WHERE mt.market_id = mp.polymarket_market_id
                      AND mt.outcome = 'Yes'
                ) as polymarket_volume_24h,
                (
                    SELECT volume_24h FROM v_latest_volumes v
                    JOIN market_tokens mt ON v.token_id = mt.token_id
                    WHERE mt.market_id = mp.kalshi_market_id
                      AND mt.outcome = 'Yes'
                ) as kalshi_volume_24h
                
            FROM market_pairs mp
            JOIN markets m_poly ON mp.polymarket_market_id = m_poly.market_id
            JOIN markets m_kalshi ON mp.kalshi_market_id = m_kalshi.market_id
            WHERE mp.active = true
              AND m_poly.status = 'active'
              AND m_kalshi.status = 'active'
        """
        return db.execute(query, fetch=True) or []

    @staticmethod
    def record_opportunity(
        pair_id: UUID,
        arbitrage_type: str,
        polymarket_yes_price: Decimal,
        polymarket_no_price: Decimal,
        kalshi_yes_price: Decimal,
        kalshi_no_price: Decimal,
        total_cost: Decimal,
        profit_margin: Decimal,
        profit_percentage: Decimal,
        polymarket_volume_24h: Optional[Decimal] = None,
        kalshi_volume_24h: Optional[Decimal] = None,
        polymarket_spread: Optional[Decimal] = None,
        kalshi_spread: Optional[Decimal] = None,
        expires_minutes: int = 5,
    ) -> dict:
        """
        Record a detected arbitrage opportunity.
        
        Args:
            pair_id: The market pair UUID
            arbitrage_type: 'YES_NO' or 'NO_YES'
            polymarket_yes_price: Polymarket YES price (0-1)
            polymarket_no_price: Polymarket NO price (0-1)
            kalshi_yes_price: Kalshi YES price (0-1)
            kalshi_no_price: Kalshi NO price (0-1)
            total_cost: Combined cost to buy both positions
            profit_margin: 1 - total_cost
            profit_percentage: (profit_margin / total_cost) * 100
            polymarket_volume_24h: Polymarket 24h volume
            kalshi_volume_24h: Kalshi 24h volume
            polymarket_spread: Polymarket bid-ask spread
            kalshi_spread: Kalshi bid-ask spread
            expires_minutes: Minutes until opportunity expires
            
        Returns:
            The recorded opportunity
        """
        db = get_db_pool()
        min_volume = None
        if polymarket_volume_24h is not None and kalshi_volume_24h is not None:
            min_volume = min(polymarket_volume_24h, kalshi_volume_24h)
        elif polymarket_volume_24h is not None:
            min_volume = polymarket_volume_24h
        elif kalshi_volume_24h is not None:
            min_volume = kalshi_volume_24h

        query = """
            INSERT INTO arbitrage_opportunities (
                pair_id, arbitrage_type,
                polymarket_yes_price, polymarket_no_price,
                kalshi_yes_price, kalshi_no_price,
                total_cost, profit_margin, profit_percentage,
                polymarket_volume_24h, kalshi_volume_24h, min_volume_24h,
                polymarket_spread, kalshi_spread,
                expires_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                NOW() + (%s * INTERVAL '1 minute')
            )
            RETURNING *
        """
        result = db.execute(
            query,
            (str(pair_id), arbitrage_type,
             polymarket_yes_price, polymarket_no_price,
             kalshi_yes_price, kalshi_no_price,
             total_cost, profit_margin, profit_percentage,
             polymarket_volume_24h, kalshi_volume_24h, min_volume,
             polymarket_spread, kalshi_spread,
             expires_minutes),
            fetch=True,
        )
        return result[0] if result else {}

    @staticmethod
    def get_active_opportunities(
        min_profit_pct: Decimal = Decimal("0.2"),
        min_volume: Decimal = Decimal("100"),
        limit: int = 50,
    ) -> list[dict]:
        """
        Get currently active arbitrage opportunities.
        
        Args:
            min_profit_pct: Minimum profit percentage to include
            min_volume: Minimum 24h volume on both platforms
            limit: Max results to return
            
        Returns:
            List of active opportunities sorted by profit percentage
        """
        db = get_db_pool()
        query = """
            SELECT * FROM v_active_arbitrage
            WHERE profit_percentage >= %s
              AND (min_volume_24h IS NULL OR min_volume_24h >= %s)
            ORDER BY profit_percentage DESC
            LIMIT %s
        """
        return db.execute(query, (min_profit_pct, min_volume, limit), fetch=True) or []

    @staticmethod
    def expire_old_opportunities() -> int:
        """
        Mark expired opportunities as expired.
        
        Returns:
            Number of opportunities marked as expired
        """
        db = get_db_pool()
        query = """
            UPDATE arbitrage_opportunities
            SET status = 'expired'
            WHERE status = 'active'
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
            RETURNING opportunity_id
        """
        result = db.execute(query, fetch=True)
        return len(result) if result else 0

    @staticmethod
    def get_opportunity_history(
        pair_id: Optional[UUID] = None,
        hours: int = 24,
        limit: int = 100,
    ) -> list[dict]:
        """
        Get historical arbitrage opportunities.
        
        Args:
            pair_id: Optional filter by market pair
            hours: Lookback period in hours
            limit: Max results to return
            
        Returns:
            List of historical opportunities
        """
        db = get_db_pool()
        
        pair_filter = "AND pair_id = %s" if pair_id else ""
        params = [hours]
        if pair_id:
            params.append(str(pair_id))
        params.append(limit)

        query = f"""
            SELECT
                ao.*,
                m_poly.title as polymarket_title,
                m_kalshi.title as kalshi_title
            FROM arbitrage_opportunities ao
            JOIN market_pairs mp ON ao.pair_id = mp.pair_id
            JOIN markets m_poly ON mp.polymarket_market_id = m_poly.market_id
            JOIN markets m_kalshi ON mp.kalshi_market_id = m_kalshi.market_id
            WHERE ao.detected_at > NOW() - (%s * INTERVAL '1 hour')
              {pair_filter}
            ORDER BY ao.detected_at DESC
            LIMIT %s
        """
        return db.execute(query, tuple(params), fetch=True) or []

    @staticmethod
    def find_similar_markets(
        title: str,
        source: str,
        threshold: float = 0.85,
        limit: int = 10,
    ) -> list[dict]:
        """
        Find markets on the opposite platform with similar titles.
        Uses pg_trgm for fuzzy matching.
        
        Args:
            title: Market title to match
            source: Source to search ('polymarket' or 'kalshi')
            threshold: Minimum similarity score (0-1)
            limit: Max results to return
            
        Returns:
            List of potential matches with similarity scores
        """
        db = get_db_pool()
        query = """
            SELECT
                market_id,
                source,
                source_id,
                title,
                category,
                url,
                SIMILARITY(title, %s) as similarity_score
            FROM markets
            WHERE source = %s
              AND status = 'active'
              AND SIMILARITY(title, %s) >= %s
            ORDER BY SIMILARITY(title, %s) DESC
            LIMIT %s
        """
        return db.execute(
            query,
            (title, source, title, threshold, title, limit),
            fetch=True,
        ) or []

