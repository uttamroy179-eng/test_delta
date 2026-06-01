# engine/risk_manager.py
# Single source of truth for all risk constants, position state, and daily loss tracking.

import threading
from datetime import datetime, date
from typing import Dict, Optional, Any

# -----------------------------------------------
# CONFIGURATION - SINGLE SOURCE OF TRUTH
# -----------------------------------------------

MAX_DAILY_LOSS_PCT = 20.0    # Stop trading if daily loss exceeds 20% of starting balance
CAPITAL_RISK_PCT   = 2.0     # Risk 2% of balance per trade
ATR_SL_MULTIPLIER  = 1.5
MAX_CONTRACTS      = 50      # Hard cap on position size
MAX_PYRAMID_LEVELS = 2       # Max pyramid add-on levels

# Scale-out percentages — imported by strategy.py and position_monitor.py
TP1_PCT = 0.20
TP2_PCT = 0.30
TP3_PCT = 0.50

# Pyramid add-on trigger: price must move this many ATRs in favour per level
PYRAMID_ATR_TRIGGER = 0.5

EXCHANGE_MIN_SIZE = 1  # Minimum contracts per order on Delta Exchange


# -----------------------------------------------
# DAILY LOSS TRACKER — THREAD-SAFE
# -----------------------------------------------

class DailyLossTracker:
    """
    Tracks realized PnL for the current trading day.
    Resets automatically at the start of each new day.

    Thread-safe via RLock (allows same thread to re-acquire).
    """

    def __init__(self, starting_balance: float):
        self._lock              = threading.RLock()
        self.starting_balance   = starting_balance
        self.realized_pnl       = 0.0
        self.trade_count        = 0
        self.last_reset_date    = date.today()
        self.trading_halted     = False
        self.halt_reason: Optional[str] = None
        self._balance_needs_refresh     = False

    def _check_date_reset(self):
        """Reset tracker if a new trading day has started."""
        today = date.today()
        if today != self.last_reset_date:
            print(
                f"[RiskManager] New trading day ({self.last_reset_date} -> {today}). "
                f"Resetting daily PnL tracker."
            )
            self.realized_pnl           = 0.0
            self.trade_count            = 0
            self.trading_halted         = False
            self.halt_reason            = None
            self.last_reset_date        = today
            self._balance_needs_refresh = True

    def record_trade(self, pnl: float):
        """
        Record a completed trade's PnL.
        Raises RuntimeError if balance has not been refreshed for a new day.
        Callers MUST wrap this in try/except RuntimeError.
        """
        with self._lock:
            self._check_date_reset()
            if self._balance_needs_refresh:
                raise RuntimeError(
                    "[RiskManager] Starting balance must be updated via "
                    "update_starting_balance() before recording trades on a new day."
                )
            self.realized_pnl += pnl
            self.trade_count  += 1
            print(
                f"[RiskManager] Trade recorded. PnL: {pnl:.2f} USDT | "
                f"Daily PnL: {self.realized_pnl:.2f} USDT | "
                f"Trades today: {self.trade_count}"
            )

    def update_starting_balance(self, balance: float):
        """Update the starting balance reference for the current day."""
        with self._lock:
            self._check_date_reset()
            if self._balance_needs_refresh or self.trade_count == 0:
                self.starting_balance       = balance
                self._balance_needs_refresh = False
                print(f"[RiskManager] Starting balance updated: {balance:.2f} USDT")
            else:
                print(
                    f"[RiskManager] Starting balance NOT updated: "
                    f"{self.trade_count} trade(s) already recorded today."
                )

    def check_daily_loss_limit(self) -> Dict[str, Any]:
        """Check if the daily loss limit has been breached. Returns status dict."""
        with self._lock:
            self._check_date_reset()
            daily_loss_pct = 0.0
            if self.starting_balance > 0:
                daily_loss_pct = (self.realized_pnl / self.starting_balance) * 100

            limit_breached = daily_loss_pct <= -MAX_DAILY_LOSS_PCT
            if limit_breached and not self.trading_halted:
                self.trading_halted = True
                self.halt_reason    = (
                    f"Daily loss limit of {MAX_DAILY_LOSS_PCT}% breached. "
                    f"Current loss: {abs(daily_loss_pct):.2f}%"
                )
                print(f"[RiskManager] TRADING HALTED: {self.halt_reason}")

            return {
                "halted":           self.trading_halted,
                "daily_pnl":        round(self.realized_pnl, 2),
                "daily_loss_pct":   round(daily_loss_pct, 2),
                "limit_pct":        MAX_DAILY_LOSS_PCT,
                "trade_count":      self.trade_count,
                "starting_balance": round(self.starting_balance, 2),
                "message": (
                    self.halt_reason if self.trading_halted
                    else f"Trading active. Daily PnL: {self.realized_pnl:.2f} USDT "
                         f"({daily_loss_pct:.2f}%)"
                ),
            }

    def is_trading_allowed(self) -> bool:
        """Returns True if trading is allowed (daily loss limit not breached)."""
        return not self.check_daily_loss_limit()["halted"]

    def resume_trading(self, reason: str = "Manual override"):
        """Manually resume trading with audit logging. Use with caution."""
        with self._lock:
            if not self.trading_halted:
                print("[RiskManager] resume_trading called but trading not halted.")
                return
            print(
                f"[RiskManager] TRADING RESUMED at {datetime.now().isoformat()} | "
                f"Reason: {reason} | "
                f"Daily PnL: {self.realized_pnl:.2f} USDT | "
                f"Loss: {abs((self.realized_pnl / self.starting_balance) * 100):.2f}%"
            )
            self.trading_halted = False
            self.halt_reason    = None


# -----------------------------------------------
# POSITION SIZER — WITH MARGIN CHECKS
# -----------------------------------------------

def calculate_position_size(
    balance: float,
    risk_amount: float,
    entry_price: float,
    sl_price: float,                  # FIX #2: sl_price is now a required parameter
    contract_value: float = 1.0,
    leverage: float = 1.0,
) -> int:
    """
    Calculate position size in contracts with margin validation.

    Args:
        balance:        Available USDT balance
        risk_amount:    USDT amount to risk on this trade (e.g. 2% of balance)
        entry_price:    Entry price of the trade
        sl_price:       Stop loss price (used to derive SL distance)
        contract_value: Delta Exchange contract multiplier (default 1.0)
        leverage:       Leverage multiple (default 1.0)

    Returns:
        Position size in contracts, or 0 if margin check fails.
    """
    if balance <= 0 or entry_price <= 0:
        print(f"[RiskManager] Invalid balance ({balance}) or price ({entry_price})")
        return 0

    sl_distance = abs(entry_price - sl_price)
    if sl_distance <= 0:
        print("[RiskManager] SL distance is zero. Cannot calculate size.")
        return 0

    # risk_amount = size * sl_distance * contract_value  =>  size = risk_amount / (sl_distance * contract_value)
    size = risk_amount / (sl_distance * contract_value)
    size = max(1, int(size))
    size = min(size, MAX_CONTRACTS)

    # Validate margin requirement
    required_margin = (entry_price * size * contract_value) / leverage
    if required_margin > balance:
        print(
            f"[RiskManager] Position size {size} exceeds available margin. "
            f"Required: {required_margin:.2f} USDT, Available: {balance:.2f} USDT. "
            f"Returning 0."
        )
        return 0

    print(
        f"[RiskManager] Position Size: {size} | "
        f"Risk: {risk_amount:.2f} USDT | "
        f"SL distance: {sl_distance:.4f} | "
        f"Required Margin: {required_margin:.2f} USDT | "
        f"Available: {balance:.2f} USDT"
    )
    return size


# -----------------------------------------------
# POSITION STATE TRACKER — THREAD-SAFE
# -----------------------------------------------

class PositionState:
    """
    Tracks the full lifecycle of an open position including partial TP exits,
    pyramid add-ons, and final SL/TP3 close.
    """

    def __init__(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        base_size: int,
        atr: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        contract_value: float = 1.0,
    ):
        self._lock          = threading.RLock()
        self.symbol         = symbol
        self.side           = side
        self.entry_price    = entry_price
        self.base_size      = base_size
        self.current_size   = base_size
        self.atr            = atr
        self.sl             = sl
        self.tp1            = tp1
        self.tp2            = tp2
        self.tp3            = tp3
        self.pyramid_level  = 0
        self.tp1_hit        = False
        self.tp2_hit        = False
        self.tp3_hit        = False
        self.entry_time     = datetime.now().isoformat()
        self.realized_pnl   = 0.0
        self.contract_value = contract_value

    def add_pyramid(self, size: int):
        """
        Record a pyramid add-on.
        Raises ValueError if MAX_PYRAMID_LEVELS is already reached.
        """
        with self._lock:
            if self.pyramid_level >= MAX_PYRAMID_LEVELS:
                raise ValueError(
                    f"[RiskManager] Cannot pyramid {self.symbol}: "
                    f"already at level {self.pyramid_level}/{MAX_PYRAMID_LEVELS}"
                )
            self.pyramid_level += 1
            self.current_size  += size
            print(
                f"[RiskManager] Pyramid level {self.pyramid_level} added. "
                f"Size: +{size} | Total: {self.current_size} {self.symbol}"
            )

    def record_tp_exit(self, tp_level: int, exit_price: float, closed_size: int) -> float:
        """
        Record a partial TP exit.
        Guards against duplicate hits and clamps closed_size to current_size.
        Returns realized PnL for this exit in USDT.
        """
        with self._lock:
            tp_hit_flags = {1: self.tp1_hit, 2: self.tp2_hit, 3: self.tp3_hit}
            if tp_level not in tp_hit_flags:
                raise ValueError(f"[RiskManager] Invalid tp_level: {tp_level}. Must be 1-3.")

            if tp_hit_flags[tp_level]:
                print(f"[RiskManager] WARNING: TP{tp_level} already hit for {self.symbol}. Ignoring.")
                return 0.0

            if closed_size > self.current_size:
                print(
                    f"[RiskManager] WARNING: closed_size {closed_size} > "
                    f"current_size {self.current_size}. Clamping."
                )
                closed_size = self.current_size

            if closed_size <= 0:
                print(f"[RiskManager] WARNING: closed_size 0 for {self.symbol}.")
                return 0.0

            if self.side == "buy":
                pnl = (exit_price - self.entry_price) * closed_size * self.contract_value
            else:
                pnl = (self.entry_price - exit_price) * closed_size * self.contract_value

            self.realized_pnl += pnl
            self.current_size  = max(0, self.current_size - closed_size)

            if tp_level == 1:
                self.tp1_hit = True
            elif tp_level == 2:
                self.tp2_hit = True
            elif tp_level == 3:
                self.tp3_hit = True

            print(
                f"[RiskManager] TP{tp_level} {self.symbol}: "
                f"Closed {closed_size} @ {exit_price}. PnL: {pnl:.2f} USDT. "
                f"Remaining: {self.current_size}"
            )
            return pnl

    def record_sl_exit(self, exit_price: float) -> float:
        """
        Record a full SL exit.
        Guards against closing an already-closed position.
        Returns realized PnL in USDT.
        """
        with self._lock:
            if self.current_size == 0:
                print(
                    f"[RiskManager] WARNING: SL exit for {self.symbol} "
                    f"but position already closed. Ignoring."
                )
                return 0.0

            if self.side == "buy":
                pnl = (exit_price - self.entry_price) * self.current_size * self.contract_value
            else:
                pnl = (self.entry_price - exit_price) * self.current_size * self.contract_value

            self.realized_pnl += pnl
            closed_size        = self.current_size
            self.current_size  = 0

            print(
                f"[RiskManager] SL {self.symbol}: "
                f"Closed {closed_size} @ {exit_price}. PnL: {pnl:.2f} USDT"
            )
            return pnl

    def to_dict(self) -> Dict[str, Any]:
        """Return position state as a dict for API responses."""
        with self._lock:
            return {
                "symbol":         self.symbol,
                "side":           self.side,
                "entry_price":    self.entry_price,
                "base_size":      self.base_size,
                "current_size":   self.current_size,
                "atr":            self.atr,
                "sl":             self.sl,
                "tp1":            self.tp1,
                "tp2":            self.tp2,
                "tp3":            self.tp3,
                "pyramid_level":  self.pyramid_level,
                "tp1_hit":        self.tp1_hit,
                "tp2_hit":        self.tp2_hit,
                "tp3_hit":        self.tp3_hit,
                "entry_time":     self.entry_time,
                "realized_pnl":   round(self.realized_pnl, 2),
                "contract_value": self.contract_value,
            }
