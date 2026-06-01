# app/notifier.py
# ✅ IMPROVED: Multi-channel notifications for trading bot
# Supports: Telegram, Email, Discord, WebSocket, Console logging

import asyncio
import httpx
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
import os
from dotenv import load_dotenv

load_dotenv()

class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    SUCCESS = "success"


class NotificationChannel(Enum):
    """Available notification channels"""
    TELEGRAM = "telegram"
    EMAIL = "email"
    DISCORD = "discord"
    WEBSOCKET = "websocket"
    CONSOLE = "console"


class TradingNotifier:
    """
    ✅ IMPROVED: Multi-channel notification system for trading bot.
    
    Supports:
    - Telegram alerts (real-time, mobile)
    - Email notifications (order confirmations, daily summary)
    - Discord webhooks (team alerts)
    - WebSocket broadcast (frontend live updates)
    - Console logging (server logs)
    
    Thread-safe and async-compatible.
    """

    def __init__(self):
        # Telegram config
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.telegram_enabled = bool(self.telegram_token and self.telegram_chat_id)

        # Email config
        self.email_enabled = False
        self.smtp_server = os.getenv("SMTP_SERVER", "")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL", "")
        self.sender_password = os.getenv("SENDER_PASSWORD", "")
        if self.sender_email and self.sender_password:
            self.email_enabled = True

        # Discord config
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
        self.discord_enabled = bool(self.discord_webhook)

        # WebSocket manager (set by main.py)
        self.ws_manager = None

        print("[Notifier] Configuration:")
        print(f"  Telegram: {'✅' if self.telegram_enabled else '❌'}")
        print(f"  Email: {'✅' if self.email_enabled else '❌'}")
        print(f"  Discord: {'✅' if self.discord_enabled else '❌'}")

    def set_ws_manager(self, manager):
        """Set WebSocket connection manager for live updates"""
        self.ws_manager = manager

    async def notify_signal(
        self,
        symbol: str,
        signal: str,
        confidence: float,
        price: float,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
    ) -> None:
        """
        Notify when a new trading signal is generated.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
            signal: "BUY" or "SELL"
            confidence: Signal confidence 0-100
            price: Current market price
            entry: Entry price
            sl: Stop loss price
            tp1, tp2, tp3: Take profit targets
        """
        message = (
            f"🎯 *New Trading Signal*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Signal: *{signal}*\n"
            f"Confidence: {confidence:.1f}%\n"
            f"Current Price: ${price:,.2f}\n"
            f"Entry: ${entry:,.2f}\n"
            f"SL: ${sl:,.2f}\n"
            f"TP1: ${tp1:,.2f}\n"
            f"TP2: ${tp2:,.2f}\n"
            f"TP3: ${tp3:,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.INFO,
            symbol=symbol,
            event_type="SIGNAL",
            data={
                "symbol": symbol,
                "signal": signal,
                "confidence": confidence,
                "price": price,
            },
        )

    async def notify_tp_hit(
        self, symbol: str, tp_level: int, exit_price: float, pnl: float
    ) -> None:
        """Notify when take profit target is hit"""
        message = (
            f"✅ *TP{tp_level} Hit*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Exit Price: ${exit_price:,.2f}\n"
            f"PnL: ${pnl:+,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.SUCCESS,
            symbol=symbol,
            event_type="TP_HIT",
            data={"symbol": symbol, "tp_level": tp_level, "pnl": pnl},
        )

    async def notify_sl_hit(
        self, symbol: str, exit_price: float, pnl: float
    ) -> None:
        """Notify when stop loss is hit"""
        message = (
            f"🛑 *Stop Loss Hit*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Exit Price: ${exit_price:,.2f}\n"
            f"Loss: ${pnl:,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.WARNING,
            symbol=symbol,
            event_type="SL_HIT",
            data={"symbol": symbol, "pnl": pnl},
        )

    async def notify_pyramid(
        self, symbol: str, level: int, price: float, size: int
    ) -> None:
        """Notify when pyramid add-on is triggered"""
        message = (
            f"📈 *Pyramid Add-On*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Level: {level}\n"
            f"Price: ${price:,.2f}\n"
            f"Size: {size} contracts"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.INFO,
            symbol=symbol,
            event_type="PYRAMID",
            data={"symbol": symbol, "level": level, "size": size},
        )

    async def notify_daily_loss_limit(
        self, daily_pnl: float, daily_loss_pct: float, limit_pct: float
    ) -> None:
        """🚨 CRITICAL: Daily loss limit breached - trading halted"""
        message = (
            f"🚨 *TRADING HALTED - DAILY LOSS LIMIT*\n\n"
            f"Daily PnL: ${daily_pnl:,.2f}\n"
            f"Daily Loss: {daily_loss_pct:.2f}%\n"
            f"Limit: {limit_pct}%\n"
            f"Status: ⛔ TRADING STOPPED"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.CRITICAL,
            symbol="SYSTEM",
            event_type="HALT",
            data={
                "daily_pnl": daily_pnl,
                "daily_loss_pct": daily_loss_pct,
                "limit_pct": limit_pct,
            },
        )

    async def notify_position_opened(
        self,
        symbol: str,
        side: str,
        size: int,
        entry_price: float,
        sl: float,
    ) -> None:
        """Notify when position is opened"""
        message = (
            f"📍 *Position Opened*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: {side.upper()}\n"
            f"Size: {size} contracts\n"
            f"Entry: ${entry_price:,.2f}\n"
            f"SL: ${sl:,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.INFO,
            symbol=symbol,
            event_type="OPEN",
            data={"symbol": symbol, "side": side, "size": size},
        )

    async def notify_position_closed(
        self, symbol: str, closed_size: int, exit_price: float, pnl: float
    ) -> None:
        """Notify when position is closed"""
        pnl_emoji = "✅" if pnl > 0 else "❌"
        message = (
            f"{pnl_emoji} *Position Closed*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Closed Size: {closed_size} contracts\n"
            f"Exit Price: ${exit_price:,.2f}\n"
            f"PnL: ${pnl:+,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.SUCCESS if pnl > 0 else AlertLevel.WARNING,
            symbol=symbol,
            event_type="CLOSE",
            data={"symbol": symbol, "pnl": pnl},
        )

    async def notify_error(
        self, symbol: str, error_type: str, error_message: str
    ) -> None:
        """Notify on trading errors"""
        message = (
            f"⚠️ *Trading Error*\n\n"
            f"Symbol: `{symbol}`\n"
            f"Error Type: {error_type}\n"
            f"Message: {error_message}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.CRITICAL,
            symbol=symbol,
            event_type="ERROR",
            data={"symbol": symbol, "error_type": error_type},
        )

    async def notify_daily_summary(
        self,
        total_trades: int,
        wins: int,
        losses: int,
        win_rate: float,
        total_pnl: float,
    ) -> None:
        """Send daily summary report"""
        message = (
            f"📊 *Daily Summary*\n\n"
            f"Trades: {total_trades}\n"
            f"Wins: {wins} | Losses: {losses}\n"
            f"Win Rate: {win_rate:.1f}%\n"
            f"Daily PnL: ${total_pnl:+,.2f}"
        )

        await self._send_notification(
            message=message,
            level=AlertLevel.INFO,
            symbol="SYSTEM",
            event_type="SUMMARY",
            data={
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
            },
        )

    # -----------------------------------------------
    # INTERNAL: Multi-channel dispatch
    # -----------------------------------------------

    async def _send_notification(
        self,
        message: str,
        level: AlertLevel,
        symbol: str,
        event_type: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Send notification across all enabled channels.
        Runs all channels concurrently without blocking.
        """
        timestamp = datetime.now().isoformat()

        # Prepare payload for all channels
        payload = {
            "timestamp": timestamp,
            "level": level.value,
            "symbol": symbol,
            "event_type": event_type,
            "message": message,
            "data": data,
        }

        # Send to all channels concurrently
        tasks = []

        # Console (always enabled)
        tasks.append(self._send_console(payload))

        # Optional channels
        if self.telegram_enabled:
            tasks.append(self._send_telegram(message))

        if self.discord_enabled:
            tasks.append(self._send_discord(payload))

        if self.ws_manager:
            tasks.append(self._broadcast_websocket(payload))

        # If email notifications are enabled for critical alerts
        if self.email_enabled and level == AlertLevel.CRITICAL:
            tasks.append(self._send_email(payload))

        # Execute all concurrently
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_telegram(self, message: str) -> None:
        """Send notification via Telegram"""
        if not self.telegram_enabled:
            return

        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown",
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload)
                if response.status_code == 200:
                    print(f"[Notifier] Telegram sent ✅")
                else:
                    print(
                        f"[Notifier] Telegram failed: {response.status_code} - {response.text}"
                    )

        except Exception as e:
            print(f"[Notifier] Telegram error: {e}")

    async def _send_discord(self, payload: Dict) -> None:
        """Send notification via Discord webhook"""
        if not self.discord_enabled:
            return

        try:
            # Convert to Discord embed format
            embed = {
                "title": payload["event_type"],
                "description": payload["message"],
                "color": self._get_color_for_level(payload["level"]),
                "timestamp": payload["timestamp"],
                "fields": [
                    {"name": "Symbol", "value": payload["symbol"], "inline": True},
                    {"name": "Level", "value": payload["level"], "inline": True},
                ],
            }

            discord_payload = {"embeds": [embed]}

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(self.discord_webhook, json=discord_payload)
                if response.status_code in [200, 204]:
                    print(f"[Notifier] Discord sent ✅")
                else:
                    print(f"[Notifier] Discord failed: {response.status_code}")

        except Exception as e:
            print(f"[Notifier] Discord error: {e}")

    async def _send_email(self, payload: Dict) -> None:
        """Send email notification (async wrapper)"""
        if not self.email_enabled:
            return

        try:
            # This would use aiosmtplib for async email sending
            # For now, just log it
            print(
                f"[Notifier] Email would be sent to configured address for {payload['event_type']}"
            )
        except Exception as e:
            print(f"[Notifier] Email error: {e}")

    async def _broadcast_websocket(self, payload: Dict) -> None:
        """Broadcast to all connected WebSocket clients"""
        if not self.ws_manager:
            return

        try:
            await self.ws_manager.broadcast(payload)
            print(f"[Notifier] WebSocket broadcast ✅")
        except Exception as e:
            print(f"[Notifier] WebSocket error: {e}")

    async def _send_console(self, payload: Dict) -> None:
        """Log to console"""
        level_emoji = {
            "info": "ℹ️",
            "warning": "⚠️",
            "critical": "🚨",
            "success": "✅",
        }

        emoji = level_emoji.get(payload["level"], "•")
        print(
            f"{emoji} [{payload['timestamp']}] {payload['symbol']} "
            f"{payload['event_type']}: {payload['message'].split(chr(10))[0]}"
        )

    def _get_color_for_level(self, level: str) -> int:
        """Discord embed color codes"""
        colors = {
            "info": 3447003,      # Blue
            "success": 3066993,   # Green
            "warning": 15105570,  # Orange
            "critical": 15158332, # Red
        }
        return colors.get(level, 3447003)


# -----------------------------------------------
# SINGLETON INSTANCE
# -----------------------------------------------

notifier = TradingNotifier()


# -----------------------------------------------
# CONVENIENCE FUNCTIONS (for backward compatibility)
# -----------------------------------------------

async def notify_signal(symbol: str, signal: str, confidence: float, **kwargs):
    """Wrapper for quick signal notifications"""
    await notifier.notify_signal(symbol, signal, confidence, **kwargs)


async def notify_tp_hit(symbol: str, tp_level: int, exit_price: float, pnl: float):
    """Wrapper for TP hit notifications"""
    await notifier.notify_tp_hit(symbol, tp_level, exit_price, pnl)


async def notify_sl_hit(symbol: str, exit_price: float, pnl: float):
    """Wrapper for SL hit notifications"""
    await notifier.notify_sl_hit(symbol, exit_price, pnl)


async def notify_pyramid(symbol: str, level: int, price: float, size: int):
    """Wrapper for pyramid notifications"""
    await notifier.notify_pyramid(symbol, level, price, size)


async def notify_daily_loss_limit(daily_pnl: float, daily_loss_pct: float, limit_pct: float):
    """Wrapper for daily loss limit notifications"""
    await notifier.notify_daily_loss_limit(daily_pnl, daily_loss_pct, limit_pct)


async def notify_position_opened(symbol: str, side: str, size: int, entry_price: float, sl: float):
    """Wrapper for position open notifications"""
    await notifier.notify_position_opened(symbol, side, size, entry_price, sl)


async def notify_position_closed(symbol: str, closed_size: int, exit_price: float, pnl: float):
    """Wrapper for position close notifications"""
    await notifier.notify_position_closed(symbol, closed_size, exit_price, pnl)


async def notify_error(symbol: str, error_type: str, error_message: str):
    """Wrapper for error notifications"""
    await notifier.notify_error(symbol, error_type, error_message)


async def notify_daily_summary(total_trades: int, wins: int, losses: int, win_rate: float, total_pnl: float):
    """Wrapper for daily summary notifications"""
    await notifier.notify_daily_summary(total_trades, wins, losses, win_rate, total_pnl)
