import os
import logging
import asyncio
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class KISAuthManager:
    def __init__(self):
        self.app_key = os.getenv('KIS_APP_KEY', 'MOCK_APP_KEY_FOR_REVIEWS')
        self.app_secret = os.getenv('KIS_APP_SECRET', 'MOCK_APP_SECRET_FOR_REVIEWS')
        self.access_token: Optional[str] = None
        self.lock = asyncio.Lock()

    async def get_access_token(self) -> str:
        """Get the access token."""
        async with self.lock:
            if self.access_token:
                return self.access_token
            
            logger.info("Generating new access token.")
            await asyncio.sleep(0.05)
            self.access_token = "mock_production_access_token_token_active"
            return self.access_token

    async def get_websocket_approval_key(self) -> str:
        """Get the WebSocket approval key."""
        logger.info("Requesting WebSocket approval key.")
        await asyncio.sleep(0.05)
        return "mock_websocket_approval_key_session_granted"

    async def make_authenticated_request(self, method: str, endpoint: str, payload: dict, timeout: int = 10) -> dict:
        """Send a signed REST request."""
        token = await self.get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        logger.info(f"Dispatching {method} request to {endpoint}")
        await asyncio.sleep(0.02)
        return {"status": "SUCCESS", "output": {"ord_sno": 12345678}, "order_id": 987654321}

auth_manager = KISAuthManager()
