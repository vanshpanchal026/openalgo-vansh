# database/watchlist_db.py
"""
Persistence for the charting terminal's watchlists.

The browser is the wrong place for this. A watchlist built up over months is
real work, and in localStorage it dies with a cache clear, does not follow the
user to a second device, and cannot be read by anything server-side. OpenAlgo
allows five concurrent sessions for its single user, so the list lives here and
every device sees the same one.

Mirrors database/scalping_db.py:
- SQLite via NullPool (one connection per op, closed immediately)
- scoped_session registered in utils/db_sessions.py for FD hygiene

Rows are scoped by ``user_id`` -- the session username, the same identity
blueprints/scalping.py uses. OpenAlgo is single-user per deployment, so this is
about keeping the schema honest rather than isolating tenants.
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker

from database.engine_factory import create_db_engine
from utils.logging import get_logger

logger = get_logger(__name__)

# Canonical engine factory enforces the project-wide pooling policy
# (SQLite -> NullPool with check_same_thread=False) for FD hygiene.
engine = create_db_engine()

db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

#: A list is a human-curated thing, not a data dump. The cap is what keeps a
#: runaway import from turning one poll into a thousand-symbol broker request.
MAX_ITEMS_PER_LIST = 500

#: Enough to organise by sector, strategy and expiry several times over.
MAX_LISTS_PER_USER = 50

#: 7 TradingView-style color-coded flagged lists.
FLAG_COLORS = {
    "red": {"name": "Red list", "color": "#ef4444", "position": -7},
    "blue": {"name": "Blue list", "color": "#3b82f6", "position": -6},
    "green": {"name": "Green list", "color": "#22c55e", "position": -5},
    "orange": {"name": "Orange list", "color": "#f59e0b", "position": -4},
    "purple": {"name": "Purple list", "color": "#a855f7", "position": -3},
    "cyan": {"name": "Cyan list", "color": "#06b6d4", "position": -2},
    "pink": {"name": "Pink list", "color": "#ec4899", "position": -1},
}
FLAG_LIST_NAMES = {cfg["name"]: color_key for color_key, cfg in FLAG_COLORS.items()}


def is_flagged_list_name(name: str) -> bool:
    """True if name belongs to one of the 7 system flagged lists."""
    return bool(name and name in FLAG_LIST_NAMES)


class Watchlist(Base):
    """One named list of instruments."""

    __tablename__ = "watchlists"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(80), nullable=False, index=True)
    name = Column(String(64), nullable=False)
    #: Display order in the list picker. Sparse and rewritten on reorder.
    position = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # delete-orphan so removing a list takes its rows with it in one flush
    # rather than leaving them for a FK cascade the SQLite file may not enforce.
    items = relationship(
        "WatchlistItem",
        back_populates="watchlist",
        cascade="all, delete-orphan",
        order_by="WatchlistItem.position",
    )

    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_watchlist_user_name"),)


class WatchlistItem(Base):
    """One instrument inside a list."""

    __tablename__ = "watchlist_items"

    id = Column(Integer, primary_key=True)
    watchlist_id = Column(
        Integer, ForeignKey("watchlists.id", ondelete="CASCADE"), nullable=False, index=True
    )
    symbol = Column(String(64), nullable=False)
    exchange = Column(String(16), nullable=False)
    position = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, server_default=func.now())

    watchlist = relationship("Watchlist", back_populates="items")

    # The same instrument twice in one list is always a mistake, and the poll
    # would fetch it twice. Enforced here rather than in the blueprint so an
    # import cannot route around it.
    __table_args__ = (
        UniqueConstraint("watchlist_id", "symbol", "exchange", name="uq_watchlist_item"),
    )


def init_db():
    """Create the watchlist tables."""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "Watchlist DB", logger)


def _serialize(watchlist: Watchlist) -> dict:
    """One list and its instruments, in the shape the terminal consumes."""
    color_key = FLAG_LIST_NAMES.get(watchlist.name)
    data = {
        "id": watchlist.id,
        "name": watchlist.name,
        "position": watchlist.position,
        "items": [
            {
                "id": item.id,
                "symbol": item.symbol,
                "exchange": item.exchange,
                "position": item.position,
            }
            for item in watchlist.items
        ],
    }
    if color_key:
        data["is_flagged_list"] = True
        data["flag_color"] = color_key
        data["flag_hex"] = FLAG_COLORS[color_key]["color"]
    return data


def ensure_flagged_lists(user_id: str) -> None:
    """Ensure all 7 color-coded flagged lists exist for the user."""
    if not user_id:
        return
    try:
        existing = {
            w[0]
            for w in db_session.query(Watchlist.name)
            .filter_by(user_id=user_id)
            .filter(Watchlist.name.in_(list(FLAG_LIST_NAMES.keys())))
            .all()
        }
        for color_key, cfg in FLAG_COLORS.items():
            if cfg["name"] not in existing:
                watchlist = Watchlist(
                    user_id=user_id,
                    name=cfg["name"],
                    position=cfg["position"],
                )
                db_session.add(watchlist)
        db_session.commit()
    except Exception:
        logger.exception("Could not ensure flagged lists for %s", user_id)
        db_session.rollback()


def get_watchlists(user_id: str) -> list[dict]:
    """Every list for a user, ordered, each with its instruments."""
    try:
        ensure_flagged_lists(user_id)
        rows = (
            db_session.query(Watchlist)
            .filter_by(user_id=user_id)
            .order_by(Watchlist.position, Watchlist.id)
            .all()
        )
        return [_serialize(row) for row in rows]
    except Exception:
        logger.exception("Could not read watchlists for %s", user_id)
        db_session.rollback()
        return []


def create_watchlist(user_id: str, name: str, items: list[dict] | None = None) -> dict | None:
    """Add a list, optionally pre-filled.

    ``items`` carries the duplicate case for both of this function's callers:
    "make a copy" of an existing list, and importing one from a file. Returns
    None when the name is taken -- the caller reports that as a 409 rather than
    silently making a second list with the same name.
    """
    name = (name or "").strip()
    if not name or is_flagged_list_name(name):
        return None

    try:
        existing = db_session.query(Watchlist).filter_by(user_id=user_id, name=name).first()
        if existing:
            return None

        count = (
            db_session.query(Watchlist)
            .filter_by(user_id=user_id)
            .filter(~Watchlist.name.in_(list(FLAG_LIST_NAMES.keys())))
            .count()
        )
        if count >= MAX_LISTS_PER_USER:
            logger.warning("Watchlist cap reached for %s (%d lists)", user_id, count)
            return None

        watchlist = Watchlist(user_id=user_id, name=name, position=count)
        db_session.add(watchlist)
        db_session.flush()  # assign the id the items need

        for position, item in enumerate((items or [])[:MAX_ITEMS_PER_LIST]):
            symbol = (item.get("symbol") or "").strip().upper()
            exchange = (item.get("exchange") or "").strip().upper()
            if symbol and exchange:
                db_session.add(
                    WatchlistItem(
                        watchlist_id=watchlist.id,
                        symbol=symbol,
                        exchange=exchange,
                        position=position,
                    )
                )

        db_session.commit()
        return _serialize(watchlist)
    except Exception:
        logger.exception("Could not create watchlist %s for %s", name, user_id)
        db_session.rollback()
        return None


def rename_watchlist(user_id: str, watchlist_id: int, name: str) -> bool:
    """Rename a list. False when it is missing, system list, or the name is already used."""
    name = (name or "").strip()
    if not name or is_flagged_list_name(name):
        return False

    try:
        watchlist = db_session.query(Watchlist).filter_by(id=watchlist_id, user_id=user_id).first()
        if not watchlist or is_flagged_list_name(watchlist.name):
            return False

        clash = (
            db_session.query(Watchlist)
            .filter(
                Watchlist.user_id == user_id,
                Watchlist.name == name,
                Watchlist.id != watchlist_id,
            )
            .first()
        )
        if clash:
            return False

        watchlist.name = name
        db_session.commit()
        return True
    except Exception:
        logger.exception("Could not rename watchlist %s", watchlist_id)
        db_session.rollback()
        return False


def delete_watchlist(user_id: str, watchlist_id: int) -> bool:
    """Remove a list and everything in it."""
    try:
        watchlist = db_session.query(Watchlist).filter_by(id=watchlist_id, user_id=user_id).first()
        if not watchlist or is_flagged_list_name(watchlist.name):
            return False
        db_session.delete(watchlist)
        db_session.commit()
        return True
    except Exception:
        logger.exception("Could not delete watchlist %s", watchlist_id)
        db_session.rollback()
        return False


def toggle_flag(user_id: str, symbol: str, exchange: str, color: str = "red") -> dict | None:
    """Toggle a symbol's flagged status across the 7 color-coded flagged lists.

    - If the symbol is already in the target color list: remove it (unflag).
    - If it is in another color list: move it to the target color list.
    - If it is not flagged: add it to the target color list.

    Returns dict with { "symbol": symbol, "exchange": exchange, "flagged": bool, "color": str | None, "flagged_list_id": int }
    """
    symbol = (symbol or "").strip().upper()
    exchange = (exchange or "").strip().upper()
    color = (color or "red").strip().lower()
    if not symbol or not exchange:
        return None
    if color not in FLAG_COLORS:
        color = "red"

    target_list_name = FLAG_COLORS[color]["name"]

    try:
        ensure_flagged_lists(user_id)

        flag_lists = (
            db_session.query(Watchlist)
            .filter_by(user_id=user_id)
            .filter(Watchlist.name.in_(list(FLAG_LIST_NAMES.keys())))
            .all()
        )
        flag_lists_by_name = {w.name: w for w in flag_lists}
        target_list = flag_lists_by_name.get(target_list_name)
        if not target_list:
            return None

        # Check existing flag state across all 7 lists
        existing_list = None
        existing_item = None
        for w in flag_lists:
            for item in w.items:
                if item.symbol == symbol and item.exchange == exchange:
                    existing_list = w
                    existing_item = item
                    break
            if existing_item:
                break

        if existing_item and existing_list.id == target_list.id:
            # Same color: toggle OFF (unflag)
            db_session.delete(existing_item)
            db_session.commit()
            return {
                "symbol": symbol,
                "exchange": exchange,
                "flagged": False,
                "color": None,
                "flagged_list_id": target_list.id,
            }

        # If it was in another color list, remove it from that list first
        if existing_item and existing_list.id != target_list.id:
            db_session.delete(existing_item)
            db_session.flush()

        # Add to target list
        next_position = max((i.position for i in target_list.items), default=-1) + 1
        new_item = WatchlistItem(
            watchlist_id=target_list.id,
            symbol=symbol,
            exchange=exchange,
            position=next_position,
        )
        db_session.add(new_item)
        db_session.commit()

        return {
            "symbol": symbol,
            "exchange": exchange,
            "flagged": True,
            "color": color,
            "flagged_list_id": target_list.id,
        }
    except Exception:
        logger.exception("Could not toggle flag for %s:%s (color: %s)", exchange, symbol, color)
        db_session.rollback()
        return None


def clear_watchlist(user_id: str, watchlist_id: int) -> bool:
    """Empty a list without removing the list itself."""
    try:
        watchlist = db_session.query(Watchlist).filter_by(id=watchlist_id, user_id=user_id).first()
        if not watchlist:
            return False
        watchlist.items.clear()
        db_session.commit()
        return True
    except Exception:
        logger.exception("Could not clear watchlist %s", watchlist_id)
        db_session.rollback()
        return False


def add_item(user_id: str, watchlist_id: int, symbol: str, exchange: str) -> dict | None:
    """Append an instrument. Returns the existing row if it is already there.

    Adding a duplicate is a no-op rather than an error: the user's intent
    ("I want this in the list") is already satisfied, and reporting a failure
    for it would be noise.
    """
    symbol = (symbol or "").strip().upper()
    exchange = (exchange or "").strip().upper()
    if not symbol or not exchange:
        return None

    try:
        watchlist = db_session.query(Watchlist).filter_by(id=watchlist_id, user_id=user_id).first()
        if not watchlist:
            return None

        for item in watchlist.items:
            if item.symbol == symbol and item.exchange == exchange:
                return {
                    "id": item.id,
                    "symbol": item.symbol,
                    "exchange": item.exchange,
                    "position": item.position,
                }

        if len(watchlist.items) >= MAX_ITEMS_PER_LIST:
            logger.warning("Watchlist %s is full (%d items)", watchlist_id, MAX_ITEMS_PER_LIST)
            return None

        # max()+1 rather than len(): positions are rewritten on reorder and a
        # deletion leaves a gap, so the count is not the next free slot.
        next_position = max((item.position for item in watchlist.items), default=-1) + 1
        item = WatchlistItem(
            watchlist_id=watchlist.id, symbol=symbol, exchange=exchange, position=next_position
        )
        db_session.add(item)
        db_session.commit()

        # Record manual provenance so external scanner replace operations do not delete user-added items
        try:
            from database.scanner_meta_db import upsert_provenance
            upsert_provenance(user_id, watchlist.name, symbol, exchange, source="manual", is_active=True)
            db_session.commit()
        except Exception as prov_err:
            logger.debug("Manual provenance record skipped: %s", prov_err)

        return {
            "id": item.id,
            "symbol": item.symbol,
            "exchange": item.exchange,
            "position": item.position,
        }
    except Exception:
        logger.exception("Could not add %s:%s to watchlist %s", exchange, symbol, watchlist_id)
        db_session.rollback()
        return None


def remove_item(user_id: str, watchlist_id: int, item_id: int) -> bool:
    """Remove one instrument from a list."""
    try:
        item = (
            db_session.query(WatchlistItem)
            .join(Watchlist, WatchlistItem.watchlist_id == Watchlist.id)
            .filter(
                WatchlistItem.id == item_id,
                WatchlistItem.watchlist_id == watchlist_id,
                Watchlist.user_id == user_id,
            )
            .first()
        )
        if not item:
            return False
        db_session.delete(item)
        db_session.commit()
        return True
    except Exception:
        logger.exception("Could not remove item %s from watchlist %s", item_id, watchlist_id)
        db_session.rollback()
        return False


def reorder_items(user_id: str, watchlist_id: int, item_ids: list[int]) -> bool:
    """Rewrite the display order from a full list of item ids.

    Ids that do not belong to this list are ignored, and any item the caller
    omitted keeps a stable position after the ones it did send, so a reorder
    computed against a stale view cannot drop rows out of the list.
    """
    try:
        watchlist = db_session.query(Watchlist).filter_by(id=watchlist_id, user_id=user_id).first()
        if not watchlist:
            return False

        by_id = {item.id: item for item in watchlist.items}
        position = 0
        for item_id in item_ids:
            item = by_id.pop(item_id, None)
            if item is not None:
                item.position = position
                position += 1
        for item in by_id.values():
            item.position = position
            position += 1

        db_session.commit()
        return True
    except Exception:
        logger.exception("Could not reorder watchlist %s", watchlist_id)
        db_session.rollback()
        return False


def ensure_watchlist_tables_exists():
    """Alias matching the app.py startup pattern."""
    init_db()
