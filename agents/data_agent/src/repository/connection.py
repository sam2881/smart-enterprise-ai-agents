"""
Database connection management for metadata repository.
"""

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session

logger = structlog.get_logger(__name__)


class DatabaseConnection:
    """
    Database connection manager for PostgreSQL.

    Supports both sync and async operations.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initialize database connection.

        Falls back to environment variables if not provided.
        """
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "agentdb")
        self.user = user or os.getenv("POSTGRES_USER", "admin")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "admin123")

        # Connection URLs
        self.sync_url = (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        self.async_url = (
            f"postgresql+asyncpg://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

        # Engines (lazy initialization)
        self._sync_engine = None
        self._async_engine = None
        self._sync_session_factory = None
        self._async_session_factory = None

    @property
    def sync_engine(self):
        """Get sync SQLAlchemy engine."""
        if self._sync_engine is None:
            self._sync_engine = create_engine(
                self.sync_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False,
            )
        return self._sync_engine

    @property
    def async_engine(self):
        """Get async SQLAlchemy engine."""
        if self._async_engine is None:
            self._async_engine = create_async_engine(
                self.async_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False,
            )
        return self._async_engine

    def get_sync_session(self) -> Session:
        """Get synchronous session."""
        if self._sync_session_factory is None:
            self._sync_session_factory = sessionmaker(
                bind=self.sync_engine,
                expire_on_commit=False,
            )
        return self._sync_session_factory()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get asynchronous session as context manager."""
        if self._async_session_factory is None:
            self._async_session_factory = async_sessionmaker(
                bind=self.async_engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )

        session = self._async_session_factory()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error("db_session_error", error=str(e))
            raise
        finally:
            await session.close()

    async def execute_function(self, func_name: str, *args) -> dict:
        """
        Execute a PostgreSQL function.

        Args:
            func_name: Function name (e.g., 'get_pipeline_config')
            *args: Function arguments

        Returns:
            Function result as dictionary
        """
        async with self.get_async_session() as session:
            # Build argument placeholders
            placeholders = ", ".join([f":arg{i}" for i in range(len(args))])
            query = text(f"SELECT {func_name}({placeholders})")

            # Build parameter dict
            params = {f"arg{i}": arg for i, arg in enumerate(args)}

            result = await session.execute(query, params)
            row = result.fetchone()

            if row and row[0]:
                return row[0]
            return {}

    async def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            async with self.get_async_session() as session:
                result = await session.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error("db_health_check_failed", error=str(e))
            return False

    async def close(self):
        """Close all connections."""
        if self._async_engine:
            await self._async_engine.dispose()
        if self._sync_engine:
            self._sync_engine.dispose()


# Global connection instance
_db_connection: Optional[DatabaseConnection] = None


def get_db_connection() -> DatabaseConnection:
    """Get or create global database connection."""
    global _db_connection
    if _db_connection is None:
        _db_connection = DatabaseConnection()
    return _db_connection
