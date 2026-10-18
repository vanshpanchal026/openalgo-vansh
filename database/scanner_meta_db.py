# database/scanner_meta_db.py
"""
Persistence for Scanner Stock Provenance and Research Metadata.

Maintains multi-source stock provenance and isolated research metadata
(company info, IPO details, and future AI agent findings) decoupled from
core OpenAlgo watchlist tables.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from database.watchlist_db import db_session, engine
from utils.logging import get_logger

logger = get_logger(__name__)

Base = declarative_base()
Base.query = db_session.query_property()


class ScannerStockProvenance(Base):
    """
    Tracks provenance of stocks ingested into watchlists across multiple sources.
    Enables source-scoped replacements while preserving manually added stocks
    and stocks shared across multiple screeners.
    """

    __tablename__ = "scanner_stock_provenance"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(80), nullable=False, index=True)
    watchlist_name = Column(String(64), nullable=False, index=True)
    symbol = Column(String(64), nullable=False)
    exchange = Column(String(16), nullable=False)
    source = Column(String(64), nullable=False, index=True)
    first_seen_at = Column(DateTime, server_default=func.now())
    last_seen_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "watchlist_name",
            "symbol",
            "exchange",
            "source",
            name="uq_stock_provenance",
        ),
        Index("idx_prov_lookup", "user_id", "watchlist_name", "source", "is_active"),
        Index("idx_prov_sym_lookup", "user_id", "watchlist_name", "symbol", "exchange", "is_active"),
    )


class StockResearchMeta(Base):
    """
    Isolated storage for company background, IPO fundamentals, subscription stats,
    and future AI agent research notes. Kept strictly separate from the core
    watchlist table to prevent database bloating.
    """

    __tablename__ = "stock_research_meta"

    symbol = Column(String(64), primary_key=True)
    exchange = Column(String(16), primary_key=True)
    company_name = Column(String(255), nullable=True)
    listing_date = Column(String(32), nullable=True)
    issue_price = Column(Float, nullable=True)
    issue_size = Column(Float, nullable=True)
    is_sme = Column(Boolean, nullable=True)
    subscription_stats = Column(Text, nullable=True)  # JSON-encoded
    raw_metadata = Column(Text, nullable=True)        # JSON-encoded
    agent_notes = Column(Text, nullable=True)         # JSON-encoded for AI agents
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db() -> None:
    """Create the scanner provenance and research metadata tables."""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "Scanner Meta DB", logger)


def ensure_scanner_meta_tables_exists() -> None:
    """Startup alias matching the app.py convention."""
    init_db()


def get_active_provenance_symbols(
    user_id: str, watchlist_name: str, source: str
) -> Set[Tuple[str, str]]:
    """Return set of (symbol, exchange) actively managed by the given source in this watchlist."""
    try:
        rows = (
            db_session.query(ScannerStockProvenance.symbol, ScannerStockProvenance.exchange)
            .filter_by(
                user_id=user_id,
                watchlist_name=watchlist_name,
                source=source,
                is_active=True,
            )
            .all()
        )
        return {(r.symbol, r.exchange) for r in rows}
    except Exception:
        logger.exception(
            "Could not load active provenance for %s:%s:%s", user_id, watchlist_name, source
        )
        return set()


def count_other_active_sources_for_symbol(
    user_id: str,
    watchlist_name: str,
    symbol: str,
    exchange: str,
    exclude_source: Optional[str] = None,
) -> int:
    """
    Count how many OTHER active sources claim this symbol in the specified watchlist.
    If count > 0, the stock must NOT be removed from the watchlist when exclude_source is replaced.
    """
    try:
        query = db_session.query(ScannerStockProvenance).filter_by(
            user_id=user_id,
            watchlist_name=watchlist_name,
            symbol=symbol,
            exchange=exchange,
            is_active=True,
        )
        if exclude_source:
            query = query.filter(ScannerStockProvenance.source != exclude_source)
        return query.count()
    except Exception:
        logger.exception(
            "Could not count other active sources for %s:%s in %s", symbol, exchange, watchlist_name
        )
        return 0


def upsert_provenance(
    user_id: str,
    watchlist_name: str,
    symbol: str,
    exchange: str,
    source: str,
    is_active: bool = True,
) -> None:
    """Upsert provenance record for a stock under a specific source."""
    symbol = symbol.strip().upper()
    exchange = exchange.strip().upper()
    watchlist_name = watchlist_name.strip()
    source = source.strip()

    row = (
        db_session.query(ScannerStockProvenance)
        .filter_by(
            user_id=user_id,
            watchlist_name=watchlist_name,
            symbol=symbol,
            exchange=exchange,
            source=source,
        )
        .first()
    )

    now = datetime.utcnow()
    if row:
        row.is_active = is_active
        row.last_seen_at = now
    else:
        new_row = ScannerStockProvenance(
            user_id=user_id,
            watchlist_name=watchlist_name,
            symbol=symbol,
            exchange=exchange,
            source=source,
            first_seen_at=now,
            last_seen_at=now,
            is_active=is_active,
        )
        db_session.add(new_row)
    db_session.flush()


def deactivate_source_provenance(
    user_id: str,
    watchlist_name: str,
    source: str,
    symbols_to_deactivate: List[Tuple[str, str]],
) -> None:
    """Deactivate provenance records for symbols no longer present in a source's latest scan."""
    if not symbols_to_deactivate:
        return

    for sym, exch in symbols_to_deactivate:
        row = (
            db_session.query(ScannerStockProvenance)
            .filter_by(
                user_id=user_id,
                watchlist_name=watchlist_name,
                symbol=sym,
                exchange=exch,
                source=source,
            )
            .first()
        )
        if row:
            row.is_active = False
            row.last_seen_at = datetime.utcnow()
    db_session.flush()


def upsert_research_meta(
    symbol: str,
    exchange: str,
    metadata: Dict[str, Any],
) -> None:
    """Upsert company and fundamental metadata for an instrument into stock_research_meta."""
    if not metadata:
        return

    symbol = symbol.strip().upper()
    exchange = exchange.strip().upper()

    row = db_session.query(StockResearchMeta).filter_by(symbol=symbol, exchange=exchange).first()

    now = datetime.utcnow()
    if not row:
        row = StockResearchMeta(symbol=symbol, exchange=exchange, created_at=now)
        db_session.add(row)

    row.updated_at = now

    if "company_name" in metadata:
        row.company_name = metadata["company_name"]
    if "listing_date" in metadata:
        row.listing_date = str(metadata["listing_date"])
    if "issue_price" in metadata and metadata["issue_price"] is not None:
        try:
            row.issue_price = float(metadata["issue_price"])
        except (ValueError, TypeError):
            pass
    if "issue_size" in metadata and metadata["issue_size"] is not None:
        try:
            row.issue_size = float(metadata["issue_size"])
        except (ValueError, TypeError):
            pass
    if "is_sme" in metadata and metadata["is_sme"] is not None:
        row.is_sme = bool(metadata["is_sme"])
    if "subscription_stats" in metadata and metadata["subscription_stats"] is not None:
        val = metadata["subscription_stats"]
        row.subscription_stats = json.dumps(val) if not isinstance(val, str) else val
    if "raw_metadata" in metadata and metadata["raw_metadata"] is not None:
        val = metadata["raw_metadata"]
        row.raw_metadata = json.dumps(val) if not isinstance(val, str) else val
    if "agent_notes" in metadata and metadata["agent_notes"] is not None:
        val = metadata["agent_notes"]
        row.agent_notes = json.dumps(val) if not isinstance(val, str) else val

    db_session.flush()


def commit() -> None:
    """Commit any pending changes in scanner_meta_db session."""
    db_session.commit()


def rollback() -> None:
    """Roll back any failed transaction in scanner_meta_db session."""
    db_session.rollback()


def get_research_meta(symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
    """Retrieve research metadata for an instrument."""
    row = (
        db_session.query(StockResearchMeta)
        .filter_by(symbol=symbol.strip().upper(), exchange=exchange.strip().upper())
        .first()
    )
    if not row:
        return None

    def _safe_json(val: Optional[str]) -> Any:
        if not val:
            return None
        try:
            return json.loads(val)
        except Exception:
            return val

    return {
        "symbol": row.symbol,
        "exchange": row.exchange,
        "company_name": row.company_name,
        "listing_date": row.listing_date,
        "issue_price": row.issue_price,
        "issue_size": row.issue_size,
        "is_sme": row.is_sme,
        "subscription_stats": _safe_json(row.subscription_stats),
        "raw_metadata": _safe_json(row.raw_metadata),
        "agent_notes": _safe_json(row.agent_notes),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
