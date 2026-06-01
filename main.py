# main.py
# Run with: uvicorn main:app --reload

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import uvicorn
from datetime import datetime
from typing import List, Optional, Dict, Any
import asyncio
import traceback
from contextlib import asynccontextmanager
from app.socket_manager import manager
from app.core.position_monitor import monitor_positions
from app.data_fetcher import fetch_ohlc, fetch_ohlc_multi_timeframe
from engine.strategy  import generate_signal, calculate_sl_tp, check_pyramid_trigger
from engine.executor     import (
    place_order, place_pyramid_order, scale_out,
    close_position, fetch_open_positions, fetch_wallet_balance,
)
from engine.risk_manager import (
    DailyLossTracker, PositionState,
    calculate_position_size, CAPITAL_RISK_PCT,
)

# Correct import path — file lives at app/notifier.py
from app.notifier import (
    notify_signal,
    notify_daily_loss_limit, notify_tp_hit,
    notify_sl_hit,
    notify_pyramid,
)

# ============================================
# CONFIGURATION
# ============================================

BASE_URL          = "https://api.india.delta.exchange"
CACHE_DURATION    = 30
MTF_TIMEFRAMES    = ["5m", "15m"]
PRIMARY_TIMEFRAME = "5m"
AUTO_TRADING_ENABLED = True

# ============================================
# TTL CACHE
# ============================================

class TTLCache(dict):
    def __init__(self, maxsize: int = 0, ttl: int = 30):
        super().__init__()
        self.ttl         = ttl
        self._timestamps = {}

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._timestamps[key] = datetime.now()

    def __getitem__(self, key):
        if key in self._timestamps:
            if (datetime.now() - self._timestamps[key]).total_seconds() > self.ttl:
                self.pop(key, None)
                raise KeyError(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        if key in self._timestamps and \
                (datetime.now() - self._timestamps[key]).total_seconds() > self.ttl:
            self.pop(key, None)
            return False
        return super().__contains__(key)

    def pop(self, key, *args):
        self._timestamps.pop(key, None)
        return super().pop(key, *args)

    def clear(self):
        self._timestamps.clear()
        super().clear()


# ============================================
# GLOBAL STATE
# ============================================

signals_cache:     Dict[str, Any] = {}
positions:         Dict[str, PositionState] = {}
trade_history:     List[Dict] = []
market_cache       = TTLCache(maxsize=1, ttl=CACHE_DURATION)
_background_tasks: set = set()
daily_tracker:     Optional[DailyLossTracker] = None


# ============================================
# SAFE CONVERSION HELPERS
# ============================================

def safe_float(value, default=0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return default


def safe_string(value, default="") -> str:
    return str(value) if value is not None else default


# ============================================
# PROCESS TICKER DATA
# ============================================

def process_ticker_data(item: Dict) -> Optional[Dict]:
    try:
        symbol = safe_string(item.get("symbol")).upper()
        if not symbol:
            return None
        quotes = item.get("quotes")
        best_bid = safe_float(quotes.get("best_bid")) if isinstance(quotes, dict) else 0.0
        best_ask = safe_float(quotes.get("best_ask")) if isinstance(quotes, dict) else 0.0
        return {
            "symbol":        symbol,
            "contract_type": safe_string(item.get("contract_type")),
            "product_id":    item.get("product_id"),
            "mark_price":    safe_float(item.get("mark_price")),
            "spot_price":    safe_float(item.get("spot_price")),
            "open":          safe_float(item.get("open")),
            "high":          safe_float(item.get("high")),
            "low":           safe_float(item.get("low")),
            "close":         safe_float(item.get("close")),
            "volume":        safe_float(item.get("volume")),
            "turnover_usd":  safe_float(item.get("turnover_usd")),
            "oi":            safe_float(item.get("oi")),
            "oi_value_usd":  safe_float(item.get("oi_value_usd")),
            "best_bid":      best_bid,
            "best_ask":      best_ask,
            "timestamp":     item.get("timestamp"),
        }
    except Exception as e:
        print(f"[Main] Warning: Error processing ticker item: {e}")
        return None


# ============================================
# SAMPLE DATA FALLBACK
# ============================================

def get_sample_data() -> List[Dict]:
    return [
        {
            "symbol": "BTCUSD", "contract_type": "perpetual_futures",
            "product_id": 139, "mark_price": 95000.0, "spot_price": 94980.0,
            "open": 93000.0, "high": 96000.0, "low": 92500.0, "close": 94800.0,
            "volume": 125000, "turnover_usd": 11875000000.0,
            "oi": 15000.0, "oi_value_usd": 1425000000.0,
            "best_bid": 94990.0, "best_ask": 95010.0,
            "timestamp": int(datetime.now().timestamp() * 1_000_000),
        },
        {
            "symbol": "ETHUSD", "contract_type": "perpetual_futures",
            "product_id": 1699, "mark_price": 1800.0, "spot_price": 1799.5,
            "open": 1750.0, "high": 1850.0, "low": 1740.0, "close": 1795.0,
            "volume": 85000, "turnover_usd": 153000000.0,
            "oi": 42000.0, "oi_value_usd": 75600000.0,
            "best_bid": 1799.0, "best_ask": 1801.0,
            "timestamp": int(datetime.now().timestamp() * 1_000_000),
        },
    ]


# ============================================
# FETCH MARKET DATA
# ============================================

async def fetch_market_data_fast() -> List[Dict]:
    if "market_data" in market_cache:
        return market_cache["market_data"]

    print("[Main] Fetching fresh ticker data from Delta Exchange...")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                f"{BASE_URL}/v2/tickers",
                params={"contract_types": "perpetual_futures"},
                headers={"User-Agent": "autobot-trading-backend", "Accept": "application/json"},
            )
        if response.status_code == 200:
            raw_data  = response.json().get("result", [])
            processed = [p for item in raw_data if (p := process_ticker_data(item))]
            if processed:
                market_cache["market_data"] = processed
                print(f"[Main] Processed {len(processed)} tickers.")
                return processed
        print(f"[Main] Falling back to sample data (HTTP {response.status_code})")
    except Exception as e:
        print(f"[Main] Error fetching market data: {e}")

    sample = get_sample_data()
    market_cache["market_data"] = sample
    return sample


# ============================================
# BACKGROUND CACHE REFRESH
# ============================================

async def background_cache_refresh():
    while True:
        await asyncio.sleep(CACHE_DURATION)
        try:
            market_cache.pop("market_data", None)
            await fetch_market_data_fast()
        except Exception as e:
            print(f"[Main] Background cache refresh failed: {e}")


# ============================================
# POSITION MONITOR LOOP
# ============================================

async def position_monitor_loop():
    """Background task: checks open positions every 10 seconds."""
    global daily_tracker

    class _Executor:
        @staticmethod
        async def close_position(symbol, side, size):
            return await close_position(symbol, side, size)

        @staticmethod
        async def scale_out(symbol, side, size, tp_level):
            return await scale_out(symbol, side, size, tp_level)

        @staticmethod
        async def place_pyramid_order(symbol, side, size, level):
            return await place_pyramid_order(symbol, side, size, level)

    class _Notifier:
        # Matches app.notifier.notify_sl_hit(symbol, exit_price, pnl)
        @staticmethod
        async def notify_sl_hit(symbol, exit_price, pnl):
            return await notify_sl_hit(symbol, exit_price, pnl)

        # Matches app.notifier.notify_tp_hit(symbol, tp_level, exit_price, pnl)
        @staticmethod
        async def notify_tp_hit(symbol, tp_level, exit_price, pnl):
            return await notify_tp_hit(symbol, tp_level, exit_price, pnl)

        # Matches app.notifier.notify_pyramid(symbol, level, price, size)
        @staticmethod
        async def notify_pyramid(symbol, level, price, size):
            return await notify_pyramid(symbol, level, price, size)

    executor_adapter = _Executor()
    notifier_adapter = _Notifier()

    while True:
        try:
            if positions:
                await monitor_positions(
                    positions=positions,
                    market_data_fetcher=fetch_market_data_fast,
                    executor=executor_adapter,
                    daily_tracker=daily_tracker,
                    notifier=notifier_adapter,
                    auto_trading_enabled=AUTO_TRADING_ENABLED,
                )
        except Exception as e:
            print(f"[PositionMonitor] Fatal error: {e}")
            traceback.print_exc()

        await asyncio.sleep(10)


# ============================================
# AUTO TRADING LOOP
# ============================================

async def auto_trading_loop():
    global daily_tracker

    while True:
        try:
            print("[Main] Running auto trading loop...")

            if daily_tracker:
                loss_status = daily_tracker.check_daily_loss_limit()
                if loss_status["halted"]:
                    print("[Main] Trading halted. Daily loss limit reached. Sleeping 5 minutes...")
                    await asyncio.sleep(300)
                    continue

            market_data = await fetch_market_data_fast()
            top_coins   = sorted(market_data, key=lambda x: x["turnover_usd"], reverse=True)[:20]
            symbols     = [coin["symbol"] for coin in top_coins]
            print(f"[Main] Scanning {len(symbols)} symbols: {symbols}")

            for symbol in symbols:
                try:
                    if symbol in positions:
                        print(f"[{symbol}] Already in position, skipping entry.")
                        continue

                    mtf_data = await fetch_ohlc_multi_timeframe(
                        symbol, timeframes=MTF_TIMEFRAMES, limit=100
                    )
                    primary_candles = mtf_data.get(PRIMARY_TIMEFRAME, [])
                    if not primary_candles:
                        print(f"[{symbol}] No primary candles, skipping.")
                        continue

                    signal_result = generate_signal(primary_candles, mtf_data=mtf_data)
                    if not signal_result:
                        print(f"[{symbol}] No signal generated, skipping.")
                        continue

                    signal     = signal_result["signal"]
                    confidence = signal_result["confidence"]
                    entry      = signal_result["entry_price"]
                    atr        = signal_result["atr"]
                    sl_tp      = signal_result["sl_tp"]

                    signals_cache[symbol] = {
                        "signal":     signal,
                        "confidence": confidence,
                        "buy_score":  signal_result["buy_score"],
                        "sell_score": signal_result["sell_score"],
                        "entry":      entry,
                        "atr":        atr,
                        "sl_tp":      sl_tp,
                        "time":       datetime.now().isoformat(),
                    }

                    print(f"[{symbol}] Signal={signal} | Confidence={confidence}%")
                    await manager.broadcast({
                        "type":        "signal_update",
                        "symbol":      symbol,
                        "signal":      signal,
                        "confidence":  confidence,
                        "buy_score":   signal_result["buy_score"],
                        "sell_score":  signal_result["sell_score"],
                        "entry_price": entry,
                        "atr":         atr,
                        "sl_tp":       sl_tp,
                        "time":        datetime.now().isoformat(),
                    })

                    if signal not in ("BUY", "SELL"):
                        continue

                    mark_price = safe_float(
                        next(
                            (d["mark_price"] for d in market_data if d["symbol"] == symbol),
                            entry,
                        )
                    )

                    if sl_tp:
                        await notify_signal(
                            symbol,
                            signal,
                            confidence,
                            price=mark_price,
                            entry=entry,
                            sl=sl_tp["sl"],
                            tp1=sl_tp["tp1"],
                            tp2=sl_tp["tp2"],
                            tp3=sl_tp["tp3"],
                        )

                    if AUTO_TRADING_ENABLED and sl_tp:
                        side = "buy" if signal == "BUY" else "sell"

                        # CRITICAL: Do NOT fallback to 1000 – skip if balance unavailable
                        balance_raw = await fetch_wallet_balance()
                        if balance_raw is None:
                            print(f"[{symbol}] Cannot fetch wallet balance – skipping trade.")
                            continue
                        balance = balance_raw

                        risk_amount = balance * (CAPITAL_RISK_PCT / 100.0)
                        sl_price = sl_tp["sl"]
                        size = calculate_position_size(
                            balance=balance,
                            risk_amount=risk_amount,
                            entry_price=entry,
                            sl_price=sl_price,
                        )

                        if size <= 0:
                            print(f"[{symbol}] Position size 0 after margin check, skipping.")
                            continue

                        order_result = await place_order(symbol, side, size)
                        if order_result and order_result.get("success"):
                            pos = PositionState(
                                symbol      = symbol,
                                side        = side,
                                entry_price = entry,
                                base_size   = size,
                                atr         = atr,
                                sl          = sl_tp["sl"],
                                tp1         = sl_tp["tp1"],
                                tp2         = sl_tp["tp2"],
                                tp3         = sl_tp["tp3"],
                            )
                            positions[symbol] = pos
                            print(f"[{symbol}] Position opened: {side.upper()} {size} contracts")

                    elif not AUTO_TRADING_ENABLED:
                        print(f"[{symbol}] TEST MODE: Would execute {signal} | Confidence={confidence}%")

                except Exception as symbol_error:
                    print(f"[{symbol}] Error: {symbol_error}")
                    traceback.print_exc()
                    continue

            await asyncio.sleep(60)

        except Exception as e:
            print(f"[AutoTradingLoop] Error: {e}")
            traceback.print_exc()
            await asyncio.sleep(10)


# ============================================
# HELPER: GET CURRENT PUBLIC IP
# ============================================

async def get_current_ip() -> str:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get("https://api.ipify.org?format=text")
            return resp.text.strip()
    except Exception:
        return "unknown (check your IP manually)"


# ============================================
# FASTAPI APP
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global daily_tracker

    print("[Main] Starting AutoBot Trading API...")
    print(f"[Main] Auto trading: {AUTO_TRADING_ENABLED}")
    print(f"[Main] Timeframes: {MTF_TIMEFRAMES} | Primary: {PRIMARY_TIMEFRAME}")

    await fetch_market_data_fast()

    # Retry up to 5 times, then give clear error
    MAX_BALANCE_RETRIES = 5
    retry_count = 0

    while retry_count < MAX_BALANCE_RETRIES:
        balance = await fetch_wallet_balance()
        if balance is not None:
            daily_tracker = DailyLossTracker(starting_balance=balance)
            print(f"[Main] Daily loss tracker initialized. Starting balance: {balance:.2f} USD")
            break

        retry_count += 1
        print(f"[Main] Wallet balance unavailable (attempt {retry_count}/{MAX_BALANCE_RETRIES}).")
        if retry_count < MAX_BALANCE_RETRIES:
            await asyncio.sleep(10)
        else:
            current_ip = await get_current_ip()
            print("\n" + "="*60)
            print("❌ CRITICAL: Could not fetch wallet balance after 5 attempts.")
            print("👉 Most likely cause: API key IP not whitelisted.")
            print("👉 Solution: Log into Delta Exchange → API Keys → Add your current IP:")
            print(f"   {current_ip}")
            print("👉 Or disable IP whitelisting for this API key (less secure).")
            print("👉 Then restart the bot.")
            print("="*60 + "\n")
            raise RuntimeError("Wallet balance unavailable – fix API key IP whitelist")

    cache_task   = asyncio.create_task(background_cache_refresh())
    trading_task = asyncio.create_task(auto_trading_loop())
    monitor_task = asyncio.create_task(position_monitor_loop())

    _background_tasks.update({cache_task, trading_task, monitor_task})
    print("[Main] All background tasks started.")

    yield

    print("[Main] Shutting down — cancelling background tasks...")
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    print("[Main] All background tasks stopped.")


app = FastAPI(
    title="AutoBot Trading API",
    description="Delta Exchange multi-strategy algo trading bot",
    version="3.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)


# ============================================
# API ENDPOINTS
# ============================================

@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/health")
async def health_check():
    loss_status = daily_tracker.check_daily_loss_limit() if daily_tracker else {}
    return {
        "status":               "healthy",
        "timestamp":            datetime.now().isoformat(),
        "auto_trading_enabled": AUTO_TRADING_ENABLED,
        "cache_status":         "cached" if "market_data" in market_cache else "empty",
        "open_positions":       len(positions),
        "daily_loss_status":    loss_status,
    }


@app.get("/signals")
async def get_signals():
    return signals_cache


@app.get("/signals/{symbol}")
async def get_signal(symbol: str):
    symbol = symbol.upper()
    result = signals_cache.get(symbol)
    if not result:
        raise HTTPException(status_code=404, detail=f"No signal found for {symbol}")
    return result


@app.get("/positions")
async def get_positions():
    return {sym: pos.to_dict() for sym, pos in positions.items()}


@app.get("/positions/{symbol}")
async def get_position(symbol: str):
    symbol = symbol.upper()
    pos    = positions.get(symbol)
    if not pos:
        raise HTTPException(status_code=404, detail=f"No open position for {symbol}")
    return pos.to_dict()


@app.get("/market-data")
async def get_market_data():
    try:
        data = await fetch_market_data_fast()
        return JSONResponse(content=data)
    except Exception:
        return JSONResponse(content=get_sample_data())


@app.get("/market-data/page/{page}")
async def get_market_data_paginated(page: int, per_page: int = 20):
    data      = await fetch_market_data_fast()
    start_idx = (page - 1) * per_page
    end_idx   = start_idx + per_page
    return {
        "page":        page,
        "per_page":    per_page,
        "total":       len(data),
        "total_pages": (len(data) + per_page - 1) // per_page,
        "data":        data[start_idx:end_idx],
    }


@app.get("/market-data/search/{query}")
async def search_market_data(query: str):
    data    = await fetch_market_data_fast()
    results = [i for i in data if query.upper() in i["symbol"].upper()]
    return {"query": query, "count": len(results), "results": results[:20]}


@app.get("/market-data/{symbol}")
async def get_market_data_by_symbol(symbol: str):
    data = await fetch_market_data_fast()
    for item in data:
        if item["symbol"].upper() == symbol.upper():
            return item
    raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")


@app.get("/stats")
async def get_stats():
    data = await fetch_market_data_fast()
    return {
        "total_tickers":   len(data),
        "cache_active":    "market_data" in market_cache,
        "open_positions":  len(positions),
        "top_5_by_volume": [
            {
                "symbol":       c["symbol"],
                "mark_price":   c["mark_price"],
                "volume":       c["volume"],
                "turnover_usd": c["turnover_usd"],
            }
            for c in sorted(data, key=lambda x: x["turnover_usd"], reverse=True)[:5]
        ],
    }


@app.get("/pnl")
async def get_pnl():
    total_pnl   = sum(t.get("realized_pnl", 0) for t in trade_history)
    wins        = sum(1 for t in trade_history if t.get("realized_pnl", 0) > 0)
    losses      = sum(1 for t in trade_history if t.get("realized_pnl", 0) < 0)
    win_rate    = (wins / len(trade_history) * 100) if trade_history else 0.0
    loss_status = daily_tracker.check_daily_loss_limit() if daily_tracker else {}
    return {
        "total_trades":      len(trade_history),
        "wins":              wins,
        "losses":            losses,
        "win_rate":          round(win_rate, 2),
        "total_pnl":         round(total_pnl, 2),
        "daily_loss_status": loss_status,
        "recent_trades":     trade_history[-10:],
    }


@app.get("/daily-status")
async def get_daily_status():
    if not daily_tracker:
        return {"error": "Daily tracker not initialized"}
    return daily_tracker.check_daily_loss_limit()


@app.post("/refresh-cache")
async def refresh_cache():
    market_cache.pop("market_data", None)
    await fetch_market_data_fast()
    return {"message": "Cache refreshed successfully"}


@app.get("/clear-cache")
async def clear_cache():
    market_cache.clear()
    return {"message": "Cache cleared successfully"}


@app.get("/")
def home():
    return {"message": "Trading Bot Backend Running", "version": "3.1.0"}


# ============================================
# RUN SERVER
# ============================================

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )