# engine/executor.py

import httpx
import hashlib
import hmac
import json
import time
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# -----------------------------------------------
# CREDENTIALS & CONFIG
# -----------------------------------------------

project_root = Path(__file__).parent.parent
env_path = project_root / ".env"

if not env_path.exists():
    raise FileNotFoundError(
        f".env file not found at: {env_path}\n"
        f"Please create a .env file in the project root with:\n"
        f"API_KEY=your_api_key\n"
        f"API_SECRET=your_api_secret"
    )

load_dotenv(env_path)

_api_key = os.getenv("API_KEY")
_api_secret = os.getenv("API_SECRET")

if not _api_key or not _api_secret:
    raise ValueError(
        f"Credentials not loaded from {env_path}.\n"
        f"API_KEY: {'set' if _api_key else 'missing'}, "
        f"API_SECRET: {'set' if _api_secret else 'missing'}"
    )

# Explicitly typed as str
API_KEY: str = _api_key
API_SECRET: str = _api_secret

BASE_URL = "https://api.india.delta.exchange"

# -----------------------------------------------
# SIGNATURE HELPER
# -----------------------------------------------

def generate_signature(secret: str, message: str) -> str:
    # FIX #1: Renamed local variable from `hash` to `mac` to avoid
    # shadowing the Python built-in hash() function.
    mac = hmac.new(
        bytes(secret, "utf-8"),
        bytes(message, "utf-8"),
        hashlib.sha256,
    )
    return mac.hexdigest()


def get_auth_headers(
    method: str,
    path: str,
    query_string: str = "",
    payload: str = "",
) -> Dict[str, str]:
    """
    Build authenticated request headers for Delta Exchange API.

    Signature formula: method + timestamp + path + query_string + payload
    For GET requests with query params, pass the query string manually
    e.g. query_string="?product_id=27&state=open"
    """
    timestamp      = str(int(time.time()))
    signature_data = method + timestamp + path + query_string + payload
    signature      = generate_signature(API_SECRET, signature_data)
    return {
        "api-key":      API_KEY,
        "timestamp":    timestamp,
        "signature":    signature,
        "Content-Type": "application/json",
        "User-Agent":   "autobot-trading-backend",
    }


# -----------------------------------------------
# INTERNAL: SAFE JSON PARSER
# -----------------------------------------------

def _safe_json(response: httpx.Response, context: str) -> Optional[Dict]:
    """
    FIX #8: Parse JSON only after confirming a parseable response.
    Logs HTTP status and raw body on non-2xx or non-JSON responses
    instead of silently returning None.
    """
    if response.status_code == 429:
        print(f"[Executor] {context}: Rate limited (HTTP 429). Retry after back-off.")
        return None

    try:
        return response.json()
    except Exception:
        print(
            f"[Executor] {context}: Could not parse JSON. "
            f"HTTP {response.status_code} | Body: {response.text[:200]}"
        )
        return None


# -----------------------------------------------
# PLACE SINGLE ORDER
# -----------------------------------------------

async def place_order(
    symbol: str,
    side: str,
    size: int,
    order_type: str = "market_order",
    limit_price: Optional[float] = None,
    reduce_only: bool = False,
) -> Optional[Dict]:
    """
    Place a single order on Delta Exchange.

    Args:
        symbol      : Trading symbol e.g. BTCUSD
        side        : "buy" or "sell"
        size        : Number of contracts (integer, no fractional lots)
        order_type  : "market_order" or "limit_order"
        limit_price : Required if order_type is "limit_order"
        reduce_only : If True, order will only reduce an existing position.
                      Always pass True for SL closes and TP scale-outs.

    Returns:
        API response dict or None on failure.
    """
    try:
        path: str        = "/v2/orders"
        body: Dict[str, Any] = {
            "product_symbol": symbol,
            "side":           side,
            "size":           size,
            "order_type":     order_type,
            # FIX #4/#5: reduce_only is passed as a string per the API schema.
            "reduce_only":    "true" if reduce_only else "false",
        }

        if order_type == "limit_order" and limit_price is not None:
            body["limit_price"] = str(limit_price)

        payload = json.dumps(body)
        headers = get_auth_headers("POST", path, payload=payload)

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{BASE_URL}{path}",
                content=payload,
                headers=headers,
            )

        # FIX #8: Use safe JSON parser
        result = _safe_json(response, f"place_order({symbol} {side} {size})")
        if result is None:
            return None

        if result.get("success"):
            order_id = result.get("result", {}).get("id", "N/A")
            print(
                f"[Executor] Order placed: {side.upper()} {size} {symbol} "
                f"({order_type}) | reduce_only={reduce_only} | Order ID: {order_id}"
            )
        else:
            print(
                f"[Executor] Order failed for {symbol}: "
                f"{result.get('error', 'Unknown error')}"
            )

        return result

    except Exception as e:
        print(f"[Executor] Exception placing order for {symbol}: {e}")
        return None


# -----------------------------------------------
# PYRAMID IN — PLACE ADD-ON ENTRY
# -----------------------------------------------

async def place_pyramid_order(
    symbol: str,
    side: str,
    base_size: int,
    level: int,
) -> Optional[Dict]:
    """
    Place a pyramid add-on order at the same size as the base position.
    Pyramid orders are new entries (not reduce_only).

    Args:
        symbol    : Trading symbol
        side      : "buy" or "sell"
        base_size : Size of the initial order
        level     : Pyramid level (1 or 2)

    Returns:
        API response dict or None on failure.
    """
    print(f"[Executor] Pyramid Level {level} add-on: {side.upper()} {base_size} {symbol}")
    # Pyramid orders open additional exposure — reduce_only must be False
    return await place_order(symbol, side, base_size, reduce_only=False)


# -----------------------------------------------
# SCALE OUT — PARTIAL EXIT AT TP LEVEL
# -----------------------------------------------

async def scale_out(
    symbol: str,
    side: str,
    close_size: int,
    tp_level: int,
) -> Optional[Dict]:
    """
    Close a specific number of contracts at a given TP level.

    FIX #2: The caller (position_monitor.py) now pre-calculates close_size
    and passes it directly. This function no longer re-applies the TP
    percentage — doing so would double-apply the percentage and result in
    a much smaller close than intended.

    Args:
        symbol     : Trading symbol
        side       : Original position side ("buy" -> close with "sell")
        close_size : Exact number of contracts to close (pre-calculated by caller)
        tp_level   : 1, 2, or 3 (used for logging only)

    Returns:
        API response dict or None on failure.
    """
    # Closing side is opposite of entry side
    close_side = "sell" if side == "buy" else "buy"

    print(
        f"[Executor] Scale Out TP{tp_level}: {close_side.upper()} "
        f"{close_size} {symbol}"
    )

    # FIX #5: reduce_only=True prevents accidentally opening a reverse position
    return await place_order(symbol, close_side, close_size, reduce_only=True)


# -----------------------------------------------
# CLOSE FULL POSITION (SL HIT OR DAILY LOSS LIMIT)
# -----------------------------------------------

async def close_position(symbol: str, side: str, size: int) -> Optional[Dict]:
    """
    Close the entire remaining position (SL hit or forced close).

    FIX #4: reduce_only=True ensures this order can only reduce the
    existing position and never accidentally open a new reverse position.

    Args:
        symbol : Trading symbol
        side   : Original position side
        size   : Remaining open size to close
    """
    close_side = "sell" if side == "buy" else "buy"
    print(f"[Executor] Closing full position: {close_side.upper()} {size} {symbol}")
    # FIX #4: Always reduce_only for SL/forced closes
    return await place_order(symbol, close_side, size, reduce_only=True)


# -----------------------------------------------
# FETCH OPEN POSITIONS
# -----------------------------------------------

async def fetch_open_positions() -> list:
    """
    Fetch all open margined positions from Delta Exchange.

    Returns:
        List of position dicts or empty list on failure.
    """
    try:
        path    = "/v2/positions/margined"
        headers = get_auth_headers("GET", path)

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BASE_URL}{path}",
                headers=headers,
            )

        # FIX #8: Use safe JSON parser
        result = _safe_json(response, "fetch_open_positions")
        if result is None:
            return []

        if result.get("success"):
            return result.get("result", [])

        print(f"[Executor] Failed to fetch positions: {result.get('error')}")
        return []

    except Exception as e:
        print(f"[Executor] Exception fetching positions: {e}")
        return []


# -----------------------------------------------
# FETCH WALLET BALANCE (USDT)
# -----------------------------------------------

async def fetch_wallet_balance() -> Optional[float]:
    """
    Fetch available USDT wallet balance from Delta Exchange.
    """
    try:
        path    = "/v2/wallet/balances"
        headers = get_auth_headers("GET", path)

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BASE_URL}{path}",
                headers=headers,
            )

        data = _safe_json(response, "fetch_wallet_balance")
        if data is None:
            return None

        if not data.get("success"):
            print(f"[Executor] Failed to fetch wallet balances: {data.get('error')}")
            return None

        wallets = data.get("result", [])

        # DEBUG: Print all asset symbols present in the response
        # Remove this block once the correct symbol is confirmed
        all_symbols = [w.get("asset_symbol") for w in wallets]
        print(f"[Executor] DEBUG - Asset symbols in wallet response: {all_symbols}")

        for wallet in wallets:
            asset_symbol = wallet.get("asset_symbol")

            # Check for both "USDT" and "USD" to handle account variants
            if asset_symbol in ("USDT", "USD"):
                balance = float(wallet.get("available_balance") or "0")
                print(f"[Executor] Available {asset_symbol} Balance: {balance:.2f}")
                return balance

        print("[Executor] USDT/USD wallet not found in balance response.")
        print(f"[Executor] Full wallet response: {wallets}")
        return None

    except Exception as e:
        print(f"[Executor] Exception fetching wallet balance: {e}")
        return None

