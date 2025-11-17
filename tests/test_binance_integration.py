import asyncio
import pytest

from backend.server.binance_integration import BinanceOrderbookManager

@pytest.mark.asyncio
async def test_binance_integration():
    manager = BinanceOrderbookManager()
    updates = []

    async def collect_updates(data):
        updates.append(data)

    await manager.subscribe_to_symbol("BTCUSDT", collect_updates)

    try:
        await asyncio.sleep(5)
    finally:
        await manager.unsubscribe_from_symbol("BTCUSDT", collect_updates)

    assert updates, "No updates received from Binance WebSocket"