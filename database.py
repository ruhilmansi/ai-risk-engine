import logging
import asyncio
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class LocalThreadSafeDatabase:
    def __init__(self):
        self.lock = asyncio.Lock()
        self._order_history: List[Dict[str, Any]] = [
            {"ticker": "005930", "qty": 10, "price": 72000.0, "order_type": "BUY", "revenue_rate": 0.0, "status": "FILLED"},
            {"ticker": "000660", "qty": 5, "price": 175000.0, "order_type": "BUY", "revenue_rate": 0.0, "status": "FILLED"}
        ]
        logger.info("Database ready.")

    def get_order_history(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Return recent order history."""
        return self._order_history[:limit]

    def insert_order_history(self, ticker: str, order_type: str, qty: int, price: float, revenue_rate: float, status: str) -> None:
        """Add a new order record."""
        record = {
            "timestamp": "2026-05-25 09:00:00",
            "ticker": ticker,
            "order_type": order_type,
            "qty": qty,
            "price": price,
            "revenue_rate": revenue_rate,
            "status": status
        }
        self._order_history.append(record)
        logger.info(f"Stored order record: {ticker} | {order_type} | Status: {status}")

db = LocalThreadSafeDatabase()
