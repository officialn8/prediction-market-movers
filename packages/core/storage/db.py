"""
Database connection handler with connection pooling.
Provides both sync (psycopg) and async (asyncpg) interfaces.
"""

import logging
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from packages.core.settings import settings

logger = logging.getLogger(__name__)


class DatabasePool:
    """
    Singleton database connection pool manager.
    
    Provides connection pooling for both sync and async operations.
    Uses psycopg3 (psycopg) for modern PostgreSQL features.
    """
    
    _instance: Optional["DatabasePool"] = None
    _pool: Optional[ConnectionPool] = None
    
    def __new__(cls) -> "DatabasePool":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        # Only initialize once
        if self._pool is not None:
            return
    
    def initialize(self, conninfo: Optional[str] = None) -> None:
        """
        Initialize the connection pool.
        
        Args:
            conninfo: PostgreSQL connection string. Defaults to settings.database_url
        """
        if self._pool is not None:
            logger.warning("Connection pool already initialized")
            return
        
        db_url = conninfo or settings.database_url
        
        logger.info("Initializing database connection pool...")
        self._pool = ConnectionPool(
            conninfo=db_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            timeout=settings.db_connection_timeout,
            open=True,
            kwargs={"row_factory": dict_row},
        )
        logger.info(
            f"Database pool initialized (min={settings.db_pool_min_size}, "
            f"max={settings.db_pool_max_size})"
        )
    
    @property
    def pool(self) -> ConnectionPool:
        """Get the connection pool, initializing if needed."""
        if self._pool is None:
            self.initialize()
        return self._pool
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg.Connection, None, None]:
        """
        Get a connection from the pool (context manager).
        
        Usage:
            with db_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM markets")
                    results = cur.fetchall()
        """
        with self.pool.connection() as conn:
            yield conn
    
    @contextmanager
    def get_cursor(self, autocommit: bool = False) -> Generator[psycopg.Cursor, None, None]:
        """
        Get a cursor directly (convenience method).
        
        Args:
            autocommit: If True, each statement commits immediately.
            
        Usage:
            with db_pool.get_cursor() as cur:
                cur.execute("SELECT * FROM markets")
                results = cur.fetchall()
        """
        with self.pool.connection() as conn:
            if autocommit:
                conn.autocommit = True
            with conn.cursor() as cur:
                yield cur
            if not autocommit:
                conn.commit()
    
    def execute(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = False,
    ) -> Optional[list[dict]]:
        """
        Execute a query with optional parameter binding.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters
            fetch: If True, return fetched results
            
        Returns:
            List of dicts if fetch=True, else None
        """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
        return None
    
    def execute_many(self, query: str, params_seq: list[tuple]) -> int:
        """
        Execute a query with multiple parameter sets (batch insert).
        
        Args:
            query: SQL query string with placeholders
            params_seq: List of parameter tuples
            
        Returns:
            Number of rows affected
        """
        with self.get_cursor() as cur:
            cur.executemany(query, params_seq)
            return cur.rowcount
    
    def health_check(self) -> bool:
        """
        Check if the database connection is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            result = self.execute("SELECT 1 as health", fetch=True)
            return result is not None and len(result) > 0
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close the connection pool."""
        if self._pool is not None:
            logger.info("Closing database connection pool...")
            self._pool.close()
            self._pool = None
            DatabasePool._instance = None
            logger.info("Database pool closed")
    
    def get_pool_stats(self) -> dict:
        """Get connection pool statistics."""
        if self._pool is None:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "min_size": self._pool.min_size,
            "max_size": self._pool.max_size,
            "size": self._pool.get_stats().get("pool_size", 0),
            "available": self._pool.get_stats().get("pool_available", 0),
        }


# Module-level singleton instance
_db_pool: Optional[DatabasePool] = None


def get_db_pool() -> DatabasePool:
    """
    Get or create the database pool singleton.
    
    This is the primary entry point for database access throughout the app.
    
    Usage:
        from packages.core.storage import get_db_pool
        
        db = get_db_pool()
        with db.get_cursor() as cur:
            cur.execute("SELECT * FROM markets WHERE status = %s", ("active",))
            markets = cur.fetchall()
    """
    global _db_pool
    if _db_pool is None:
        _db_pool = DatabasePool()
        _db_pool.initialize()
    return _db_pool


def reset_db_pool() -> None:
    """
    Reset the database pool (useful for testing).
    """
    global _db_pool
    if _db_pool is not None:
        _db_pool.close()
        _db_pool = None

