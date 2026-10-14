# OpenAlgo Architecture Customizations & High-Speed Trading Engine
> **Authoritative Technical Specification & Context Document for AI Coding Agents (Claude, GPT, Codex, etc.)**  
> **Workspace Root:** `c:\Users\pc\OneDrive\Desktop\OPEN ALGO\openalgo`  
> **Target Broker:** Angel One SmartAPI  
> **Last Updated:** October 2026  

---

## 1. Executive Summary & Problem Statements

### The Problem
The user operates OpenAlgo as an ultra-fast swing and momentum trading terminal, analyzing **400 to 500 Indian equity stocks** (NSE/BSE) daily. The trading workflow requires flipping through charts within **micro-seconds** using keyboard keys (`Space`, `ArrowDown`, `ArrowUp`), mirroring the desktop experience of TradingView.

In default OpenAlgo, this workflow encountered four critical bottlenecks:
1. **Interactive Latency & Broker Rate Limiting:**
   Every chart view triggered a live HTTP call to Angel One's historical candle API. Because Angel One strictly throttles market data calls to ~3 requests per second, rapidly cycling charts caused the queue to exceed the 10-second ceiling, throwing:
   `BrokerBusyError: History request timed out. Try again in a moment.`
2. **Watchlist Size Limitation:**
   OpenAlgo capped watchlist lists at `MAX_ITEMS_PER_LIST = 250`, throwing `409 Conflict` when importing larger scanners (e.g., 281-stock and 500-stock screener lists).
3. **Symbol Resolution Incompatibilities:**
   - Screener exports often contain BSE 6-digit numeric scrip codes (e.g., `544809`) or NSE SME suffixes (e.g., `GENXAI-SM`, `BLUEWATER-ST`). Default lookups failed to resolve them.
   - The precache service erroneously appended `-EQ` to standard symbols (e.g., `HINDZINC-EQ`), which does not match OpenAlgo's canonical token database.
4. **Windows DuckDB Concurrency Collision:**
   DuckDB requires exclusive file access locks on Windows. When a background precache thread wrote candles to `historify.duckdb` while the Flask web request thread simultaneously queried candles for the chart UI, Windows raised:
   `duckdb.duckdb.IOException: Cannot open file "db/historify.duckdb": The process cannot access the file because it is being used by another process.`
5. **No Keyboard Navigation in Frontend:**
   The UI only supported manual mouse clicking on watchlist items. Pressing `Space` caused page scrolling or unintended button toggles rather than loading the next stock on the active chart pane.

---

## 2. System Architecture Overview

```
                      +---------------------------------------+
                      |         TradingView Chart UI          |
                      |   (React / TypeScript / Vite / GSAP)  |
                      +-------------------+-------------------+
                                          |
                        Space / Up / Down | Keyboard Shortcut
                                          v
                      +---------------------------------------+
                      |       Trading.tsx Event Capture       |
                      |   - Guards against typing in inputs   |
                      |   - Syncs active index with Watchlist |
                      |   - Calls sendToFocusedPane()         |
                      +-------------------+-------------------+
                                          |
                                          | GET /history?symbol=...&interval=D
                                          v
                      +---------------------------------------+
                      |      services/history_service.py      |
                      |                                       |
                      |   [1] Cache-First Check:              |
                      |       get_history_from_db()           |
                      |       - Cache Hit (<15ms) -> Return   |
                      |       - touch_lru(symbol, exchange)   |
                      +-------------------+-------------------+
                                          | Cache Miss Only
                                          v
                      +---------------------------------------+
                      |       Dual-Lane Rate Limiter          |
                      |                                       |
                      |   Interactive Lane:                   |
                      |     Jumps to slot (bounded by 0.35s)  |
                      |   Background Lane:                    |
                      |     Paced sequentially at 0.35s       |
                      +-------------------+-------------------+
                                          |
                                          | Angel One SmartAPI (~3 req/s)
                                          v
                      +---------------------------------------+
                      |       Broker API Candle Fetch         |
                      +-------------------+-------------------+
                                          | Auto-Upsert on Miss
                                          v
                      +---------------------------------------+
                      |       database/historify_db.py        |
                      |                                       |
                      |   - _historify_db_lock (RLock)        |
                      |   - DuckDB connection pooling (5 retries)
                      |   - 90-day LRU cache pruning          |
                      +---------------------------------------+
```

---

## 3. File Manifest & Detailed Responsibilities

| File Path | Type | Responsibility |
|---|---|---|
| `services/watchlist_precache_service.py` | **NEW** | Background daemon thread that auto-precaches 1-year daily candles for imported watchlist symbols at 0.35s intervals, with 48h freshness check and 90-day LRU eviction trigger. |
| `services/history_service.py` | **MODIFIED** | Implemented dual-lane rate limiter (priority fast lane for interactive chart clicks), cache-first DuckDB lookup before broker call, and auto-caching on miss. |
| `database/historify_db.py` | **MODIFIED** | Added process-wide re-entrant lock `_historify_db_lock = threading.RLock()` to prevent Windows file locking crashes, connection retries (5x with exponential backoff), `touch_lru()`, and `evict_lru_older_than(days=90)`. |
| `blueprints/watchlist.py` | **MODIFIED** | Hooked `trigger_watchlist_precache()` to watchlist creation and symbol addition; added status check route `/watchlist/api/precache/status`. |
| `database/watchlist_db.py` | **MODIFIED** | Increased `MAX_ITEMS_PER_LIST` from 250 to 500 to accommodate large scanner dumps. |
| `database/token_db_enhanced.py` | **MODIFIED** | Added fallback search for BSE numeric scrip codes (e.g., `544809`) and NSE series suffixes (`-SM`, `-ST`, `-EQ`, `-RR`, `-IV`). |
| `frontend/src/pages/Trading.tsx` | **MODIFIED** | Installed global capture-phase keyboard listener for `Space`, `Shift+Space`, `ArrowDown`, `ArrowUp`, `PageDown`, `PageUp` to cycle through active watchlist symbols on the focused chart pane. Guarded by `chartMayTakeKey(e)`. Added initial watchlist auto-fetch on mount. |
| `frontend/src/components/trading/WatchlistPanel.tsx` | **MODIFIED** | Exposed `onItemsChange` prop to keep parent in sync with active watchlist, added `rowRefs` to auto-scroll active symbol into view, tagged rows with `data-watchlist-row`, and suppressed default scrolling on arrow/space keys. |
| `screener_all_281_stocks.json` | **NEW** | Clean JSON payload containing all 281 validated stocks from user's multibagger and Q1>Q2>Q3 scanners. |

---

## 4. Deep-Dive: Code Changes & Implementation Diffs

### A. Dual-Lane Priority Rate Limiter & Cache-First Lookup
**File:** `services/history_service.py`

#### Problem
In default OpenAlgo, all requests (background and user interactive) queued in a single monotonic slot variable `_next_history_slot`. When background precache queued 281 symbols, an interactive click was placed at slot 282 (~98 seconds in the future). This triggered `BrokerBusyError` because OpenAlgo caps interactive queue waits at 10 seconds.

#### Implementation Diff
```python
# services/history_service.py

# --- BEFORE ---
def _enforce_rate_limit(*, background: bool = False):
    global _next_history_slot
    with _history_slot_lock:
        now = time.monotonic()
        slot = max(now, _next_history_slot)
        wait = slot - now
        if not background:
            check_queue_wait(wait, kind="data")
        _next_history_slot = slot + _MIN_HISTORY_INTERVAL
    if wait > 0:
        time.sleep(wait)

# --- AFTER ---
def _enforce_rate_limit(*, background: bool = False):
    """Wait for this request's turn (~3 per second).
    Interactive user chart requests (background=False) get priority lane access
    so users clicking charts never get queued behind background batch downloads.
    """
    global _next_history_slot
    with _history_slot_lock:
        now = time.monotonic()
        if not background:
            # Interactive user requests get priority: bounded by broker spacing (0.35s)
            # without waiting behind a long background queue.
            slot = max(now, min(_next_history_slot, now + _MIN_HISTORY_INTERVAL))
            wait = slot - now
            check_queue_wait(wait, kind="data")
            _next_history_slot = slot + _MIN_HISTORY_INTERVAL
        else:
            # Background precache jobs take regular sequential slots
            slot = max(now, _next_history_slot)
            wait = slot - now
            _next_history_slot = slot + _MIN_HISTORY_INTERVAL
    if wait > 0:
        time.sleep(wait)
```

#### Cache-First DuckDB Check in `get_history()`
```python
    # Source: 'api' (default) - Fetch from broker API
    # Cache-first check for interactive chart requests:
    if not background:
        db_success, db_resp, _ = get_history_from_db(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
        if db_success and db_resp.get("data"):
            try:
                from database.historify_db import touch_lru
                touch_lru(symbol, exchange)
            except Exception:
                pass
            return True, db_resp, 200
```

---

### B. Windows DuckDB Concurrency & RLock Protection
**File:** `database/historify_db.py`

#### Problem
On Windows, DuckDB locks database files at the OS level. Concurrent thread access between the background precache worker and Flask web workers caused immediate `IOException: Cannot open file ... used by another process`. Additionally, calling `delete_market_data()` inside `evict_lru_older_than()` while holding a connection created nested connection deadlocks.

#### Implementation Diff
```python
# database/historify_db.py

# Process-wide recursive lock to serialize DuckDB file access on Windows.
# Prevents concurrent thread collisions (IO Error: Cannot open file ... used by another process)
_historify_db_lock = threading.RLock()

@contextmanager
def get_connection(max_retries: int = 5, retry_delay: float = 0.2):
    import time
    with _historify_db_lock:
        ensure_db_directory()
        db_path = get_db_path()
        conn = None
        last_error = None

        for attempt in range(max_retries):
            try:
                import duckdb
                conn = duckdb.connect(db_path)
                break
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    logger.debug(f"DuckDB connection attempt {attempt + 1} failed, retrying: {e}")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.exception(f"Failed to connect to DuckDB after {max_retries} attempts: {e}")

        if conn is None:
            raise last_error or Exception("Failed to connect to DuckDB")

        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass
```

#### 90-Day LRU Tracking & Decoupled Eviction
```python
@_serialized_write
def touch_lru(symbol: str, exchange: str) -> None:
    """Update last accessed timestamp for a symbol in LRU tracker."""
    try:
        with get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lru_cache_tracker (
                    symbol VARCHAR NOT NULL,
                    exchange VARCHAR NOT NULL,
                    last_accessed_at TIMESTAMP DEFAULT current_timestamp,
                    PRIMARY KEY (symbol, exchange)
                )
            """)
            conn.execute("""
                INSERT INTO lru_cache_tracker (symbol, exchange, last_accessed_at)
                VALUES (?, ?, now())
                ON CONFLICT (symbol, exchange) DO UPDATE SET last_accessed_at = now()
            """, [symbol.upper(), exchange.upper()])
    except Exception as e:
        logger.warning(f"Error updating LRU tracker for {symbol}:{exchange}: {e}")


@_serialized_write
def evict_lru_older_than(days: int = 90) -> int:
    """Evict symbols from local cache that have not been accessed in specified days."""
    try:
        # Step 1: Query expired symbols and close connection immediately
        with get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lru_cache_tracker (
                    symbol VARCHAR NOT NULL,
                    exchange VARCHAR NOT NULL,
                    last_accessed_at TIMESTAMP DEFAULT current_timestamp,
                    PRIMARY KEY (symbol, exchange)
                )
            """)
            expired = conn.execute("""
                SELECT symbol, exchange FROM lru_cache_tracker
                WHERE last_accessed_at < now() - INTERVAL (?) DAY
            """, [days]).fetchall()

        if not expired:
            return 0

        # Step 2: Delete market data per symbol (decoupled from query connection)
        count = 0
        for sym, exch in expired:
            delete_market_data(sym, exch)
            count += 1

        # Step 3: Remove records from lru_cache_tracker
        with get_connection() as conn:
            for sym, exch in expired:
                conn.execute(
                    "DELETE FROM lru_cache_tracker WHERE symbol = ? AND exchange = ?",
                    [sym, exch]
                )
        logger.info(f"LRU Eviction: evicted {count} symbols older than {days} days")
        return count
    except Exception as e:
        logger.warning(f"Error during LRU eviction: {e}")
        return 0
```

---

### C. Watchlist Pre-cache Worker Engine
**File:** `services/watchlist_precache_service.py`

Key highlights of the worker:
1. **Thread Safety:** Uses `_precache_lock = threading.Lock()` and a single daemon thread `_precache_thread`. If a user re-triggers import, duplicate threads are discarded.
2. **Freshness Filter (48 Hours):** Calls `get_data_range(symbol, exchange, "D")`. If `(now_epoch - last_ts) < 172800`, it skips broker API call and updates `touch_lru()`.
3. **Pacing:** Strict `time.sleep(0.35)` between loop iterations guarantees compliance with the broker's 3 req/sec rate limit.
4. **LRU Trigger:** Automatically calls `evict_lru_older_than(days=90)` once the entire watchlist precache completes.

---

### D. Watchlist Cap Expansion & Auto-Trigger
**Files:** `database/watchlist_db.py` & `blueprints/watchlist.py`

1. **Cap Expansion:**
   In `database/watchlist_db.py`:
   ```python
   # Increased from 250 to 500
   MAX_ITEMS_PER_LIST = 500
   ```
2. **Auto-Trigger Hook:**
   In `blueprints/watchlist.py`, hooked into `create_list()`, `bulk_add()`, and `add_symbol()`:
   ```python
   if items:
       try:
           from services.watchlist_precache_service import trigger_watchlist_precache
           trigger_watchlist_precache(items, user_id=_user())
       except Exception as e:
           logger.warning(f"Could not auto-trigger watchlist precache: {e}")
   ```

---

### E. Frontend Keyboard Cycling & Viewport Auto-Scroll
**Files:** `frontend/src/pages/Trading.tsx` & `frontend/src/components/trading/WatchlistPanel.tsx`

#### Implementation in `frontend/src/pages/Trading.tsx`
- **Initial Watchlist Load:** Fetches the saved watchlist ID from `localStorage.getItem('oa-trading-watchlist')` on mount to populate `watchlistItems` immediately before the user clicks anything.
- **Capture-Phase Keyboard Listener:**
  Captures `Space`, `Shift+Space`, `ArrowDown`, `ArrowUp`, `PageDown`, `PageUp`.
- **Typing Guard (`chartMayTakeKey(e)`):**
  Checks whether active element is an `<input>`, `<textarea>`, dropdown, or modal dialog. If so, keys type normally without flipping charts.
- **Pane Dispatch:**
  Calculates next or previous index cyclically `(currentIndex ± 1) % items.length` and dispatches via `sendToFocusedPane({ symbol, exchange })`.

#### Implementation in `frontend/src/components/trading/WatchlistPanel.tsx`
- **Parent State Synchronization:**
  Calls `onItemsChange?.(items)` whenever active list changes.
- **Auto-Scroll Tracking:**
  Stores element references in `rowRefs.current[activeSymbol]`. When `activeSymbol` changes, triggers:
  ```typescript
  el.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  ```
- **Suppressed Native Events:**
  `data-watchlist-row` buttons intercept `Space` and `Arrow` keys to prevent browser scroll or button focus toggling.

---

## 5. Build, Compression & Asset Pipeline

OpenAlgo serves its frontend via Flask static files from `frontend/dist/`. After modifying any `.tsx` or `.ts` files:

1. **Vite Production Build:**
   ```powershell
   cd frontend
   npm run build
   ```
2. **Pre-compression of Static Assets:**
   OpenAlgo requires pre-compressed `.gz` and `.br` files for fast serving:
   ```powershell
   cd ..
   .venv\Scripts\python.exe -m utils.precompress_assets
   ```

---

## 6. Verification & Health Check Checklist for AI Agents

When modifying or maintaining this codebase in the future, follow this verification checklist:

1. **DuckDB Mutex Verification:**
   Ensure no new database access method bypasses `get_connection()` or `_historify_db_lock`. DuckDB will fail on Windows if two connections attempt write locks simultaneously.
2. **Rate Limit Priority Integrity:**
   Ensure `services/history_service.py::_enforce_rate_limit(background=False)` preserves the priority calculation:
   `slot = max(now, min(_next_history_slot, now + _MIN_HISTORY_INTERVAL))`
   Never let interactive requests queue behind sequential background batch downloads.
3. **Symbol Suffix Safety:**
   Do not convert NSE symbols using `get_br_symbol()` prior to querying OpenAlgo's token database. Keep standard tickers as `HINDZINC`, not `HINDZINC-EQ`. Only append `-SM` / `-ST` when the security is registered under the NSE SME / Trade-to-Trade segment.
4. **Frontend Keyboard Integrity:**
   Verify `chartMayTakeKey(e)` remains active. Without it, typing spaces or arrows in the Symbol Search bar or Order Ticket will cause the underlying chart to flip stocks.

---
*End of Context Specification & Architecture Reference.*
