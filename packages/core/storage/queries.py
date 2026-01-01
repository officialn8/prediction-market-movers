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
            INSERT INTO markets (source, source_id, title, category, status, url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, source_id) 
            DO UPDATE SET 
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                status = EXCLUDED.status,
                url = EXCLUDED.url,
                updated_at = NOW()
            RETURNING *
        """
        result = db.execute(
            query,
            (source, source_id, title, category, status, url),
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

        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (token_id)
                    token_id,
                    ts as latest_ts,
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
                    l.latest_ts,
                    l.latest_price,
                    l.latest_volume,
                    h.old_price,
                    CASE
                        WHEN h.old_price > 0 THEN
                            ROUND(((l.latest_price - h.old_price) / h.old_price * 100)::numeric, 2)
                        ELSE NULL
                    END as pct_change
                FROM latest l
                JOIN historical h ON l.token_id = h.token_id
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
              AND c.pct_change IS NOT NULL
              {source_filter}
              {category_filter}
              {direction_filter}
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
                rank, quality_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    def get_recent_alert_for_token(token_id: UUID, window_minutes: int = 15) -> Optional[dict]:
        """Check if an alert was generated for this token recently."""
        db = get_db_pool()
        query = """
            SELECT * FROM alerts 
            WHERE token_id = %s 
              AND created_at > NOW() - (%s * INTERVAL '1 minute')
            LIMIT 1
        """
        result = db.execute(query, (str(token_id), window_minutes), fetch=True)
        return result[0] if result else None

    @staticmethod
    def get_cached_movers(
        window_seconds: int = 3600,
        limit: int = 20,
        source: Optional[str] = None,
        direction: str = "both",
    ) -> list[dict]:
        """
        Get top movers from the cache (fast).
        Includes join for market details.
        """
        db = get_db_pool()
        
        source_filter = "AND m.source = %s" if source else ""
        
        if direction == "gainers":
            direction_filter = "AND mc.move_pp > 0"
        elif direction == "losers":
            direction_filter = "AND mc.move_pp < 0"
        else:
            direction_filter = ""
            
        # Get the latest cached batch for this window
        query = f"""
            WITH latest_batch AS (
                SELECT MAX(as_of_ts) as max_ts
                FROM movers_cache
                WHERE window_seconds = %s
            )
            SELECT 
                mc.*,
                mc.move_pp as pct_change, -- Alias for compat
                mc.price_now as latest_price,
                mc.price_then as old_price,
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
            WHERE mc.window_seconds = %s
              {source_filter}
              {direction_filter}
            ORDER BY mc.rank ASC -- Pre-calculated rank
            LIMIT %s
        """
        
        params = [window_seconds, window_seconds]
        if source:
            params.append(source)
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
