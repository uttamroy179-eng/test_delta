# app/core/position_monitor.py

import asyncio
import traceback
from typing import Dict, Callable, Any, Awaitable

from engine.risk_manager import (
    PositionState,
    MAX_PYRAMID_LEVELS,
    TP1_PCT,
    TP2_PCT,
    TP3_PCT,
    PYRAMID_ATR_TRIGGER,
)


# -----------------------------------------------
# HELPER PREDICATES
# -----------------------------------------------

def _is_sl_hit(position: PositionState, current_price: float) -> bool:
    if position.side == "buy":
        return current_price <= position.sl
    return current_price >= position.sl


def _is_tp_hit(position: PositionState, current_price: float, tp_level: int) -> bool:
    tp_price = _get_tp_price(position, tp_level)
    if position.side == "buy":
        return current_price >= tp_price
    return current_price <= tp_price


def _get_tp_price(position: PositionState, tp_level: int) -> float:
    if tp_level == 1:
        return position.tp1
    elif tp_level == 2:
        return position.tp2
    elif tp_level == 3:
        return position.tp3
    raise ValueError(f"Invalid TP level: {tp_level}")


def _is_tp_already_hit(position: PositionState, tp_level: int) -> bool:
    return {1: position.tp1_hit, 2: position.tp2_hit, 3: position.tp3_hit}.get(tp_level, False)


def _should_pyramid(position: PositionState, current_price: float) -> bool:
    if position.pyramid_level >= MAX_PYRAMID_LEVELS:
        return False
    favorable_move = (position.pyramid_level + 1) * PYRAMID_ATR_TRIGGER * position.atr
    if position.side == "buy":
        return current_price >= position.entry_price + favorable_move
    return current_price <= position.entry_price - favorable_move


# -----------------------------------------------
# CORE MONITOR COROUTINE
# -----------------------------------------------

async def monitor_positions(
    positions: Dict[str, PositionState],
    market_data_fetcher: Callable[[], Awaitable[list]],
    executor,
    daily_tracker,
    notifier,
    auto_trading_enabled: bool = True,
) -> None:
    """
    Single-pass position monitor. Called by position_monitor_loop in main.py
    which owns the while-True loop and asyncio.sleep interval.
    """
    try:
        market_data = await market_data_fetcher()
        price_map   = {item["symbol"]: item["mark_price"] for item in market_data}

        for symbol in list(positions.keys()):
            position      = positions[symbol]
            current_price = price_map.get(symbol)

            if current_price is None:
                print(f"[Monitor] No market data for {symbol}, skipping.")
                continue

            # ---- SL CHECK (highest priority) ----
            if _is_sl_hit(position, current_price):
                print(f"[Monitor] SL HIT: {symbol} @ {current_price}")

                if auto_trading_enabled:
                    await executor.close_position(
                        symbol, position.side, position.current_size
                    )

                pnl = position.record_sl_exit(current_price)

                try:
                    daily_tracker.record_trade(pnl)
                except RuntimeError as e:
                    print(f"[Monitor] Cannot record SL trade — balance refresh needed: {e}")

                loss_status = daily_tracker.check_daily_loss_limit()

                # FIX #1: notify_sl_hit only accepts (symbol, exit_price, pnl).
                #         Pass through the adapter which drops size and loss_status.
                await notifier.notify_sl_hit(
                    symbol, current_price, position.current_size, pnl, loss_status
                )

                del positions[symbol]
                continue

            # ---- TP CHECKS ----
            for tp_level in [1, 2, 3]:
                if symbol not in positions:
                    break

                if _is_tp_already_hit(position, tp_level):
                    continue

                if not _is_tp_hit(position, current_price, tp_level):
                    continue

                print(f"[Monitor] TP{tp_level} HIT: {symbol} @ {current_price}")

                close_pct  = {1: TP1_PCT, 2: TP2_PCT, 3: TP3_PCT}[tp_level]
                close_size = max(1, round(position.current_size * close_pct))
                close_size = min(close_size, position.current_size)

                if auto_trading_enabled:
                    await executor.scale_out(
                        symbol, position.side, close_size, tp_level
                    )

                pnl = position.record_tp_exit(
                    tp_level=tp_level,
                    exit_price=current_price,
                    closed_size=close_size,
                )

                try:
                    daily_tracker.record_trade(pnl)
                except RuntimeError as e:
                    print(f"[Monitor] Cannot record TP{tp_level} trade — balance refresh needed: {e}")

                loss_status = daily_tracker.check_daily_loss_limit()

                # FIX #2: notify_tp_hit only accepts (symbol, tp_level, exit_price, pnl).
                #         Pass through the adapter which drops close_size, remaining,
                #         and loss_status.
                await notifier.notify_tp_hit(
                    symbol, tp_level, current_price,
                    close_size, pnl, position.current_size, loss_status
                )

                if tp_level == 3 or position.current_size <= 0:
                    del positions[symbol]
                    break

            if symbol not in positions:
                continue

            # ---- PYRAMID TRIGGER ----
            if _should_pyramid(position, current_price):
                try:
                    add_size = position.base_size
                    position.add_pyramid(add_size)

                    if auto_trading_enabled:
                        await executor.place_pyramid_order(
                            symbol, position.side, add_size, position.pyramid_level
                        )

                    loss_status = daily_tracker.check_daily_loss_limit()

                    # FIX #3: notify_pyramid only accepts (symbol, level, price, size).
                    #         Pass through the adapter which drops side and loss_status
                    #         and corrects the argument order.
                    await notifier.notify_pyramid(
                        symbol, position.pyramid_level,
                        position.side, add_size, current_price, loss_status
                    )

                except ValueError as e:
                    print(f"[Monitor] Pyramid blocked: {e}")

    except Exception as e:
        print(f"[Monitor] Unexpected error in monitor_positions: {e}")
        traceback.print_exc()
