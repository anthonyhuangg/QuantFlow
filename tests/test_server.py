import time

import pytest

from backend.server.server import Instrument
from backend.server.binance_integration import RealDataOrderbook


class FakeBinanceManager:
    def __init__(self):
        self.subscriptions = {}  # symbol -> list[callback]

    async def subscribe_to_symbol(self, symbol: str, callback):
        self.subscriptions.setdefault(symbol.lower(), []).append(callback)

    async def unsubscribe_from_symbol(self, symbol: str, callback):
        callbacks = self.subscriptions.get(symbol.lower(), [])
        if callback in callbacks:
            callbacks.remove(callback)


@pytest.mark.asyncio
async def test_realdata_orderbook_snapshot():
    inst = Instrument(instrument_id=1, symbol="BTC", depth=3)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    raw_data = {
        "symbol": "BTCUSDT",
        "bids": [(100.0, 1.0), (99.5, 2.0), (99.0, 3.0), (98.5, 4.0), (98.0, 5.0)],
        "asks": [(100.5, 1.1), (101.0, 2.2), (101.5, 3.3), (102.0, 4.4), (102.5, 5.5)],
        "timestamp": int(time.time() * 1000),
        "last_update_id": 12345,
    }

    await ob._handle_binance_update(raw_data)

    snap = ob.get_current_snapshot()
    assert snap is not None

    assert snap["instrument_id"] == inst.id
    assert snap["symbol"] == inst.symbol

    assert len(snap["bids"]) == inst.depth
    assert len(snap["asks"]) == inst.depth

    assert snap["bids"][0] == raw_data["bids"][0]
    assert snap["asks"][0] == raw_data["asks"][0]
    assert snap["timestamp"] == raw_data["timestamp"]


@pytest.mark.asyncio
async def test_realdata_orderbook_subscriber_notified():
    inst = Instrument(instrument_id=2, symbol="ETH", depth=2)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    received = []

    async def subscriber_cb(data):
        received.append(data)

    ob.add_subscriber(subscriber_cb)

    raw_data = {
        "symbol": "ETHUSDT",
        "bids": [(200.0, 1.0)],
        "asks": [(201.0, 1.5)],
        "timestamp": int(time.time() * 1000),
        "last_update_id": 999,
    }

    await ob._handle_binance_update(raw_data)

    assert len(received) == 1
    assert received[0]["symbol"] == "ETHUSDT"
    assert received[0]["bids"][0][0] == 200.0


@pytest.mark.asyncio
async def test_realdata_orderbook_full_cycle():
    inst = Instrument(instrument_id=3, symbol="NVDA", depth=2)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    await ob.start_real_data_feed()
    await ob.stop_real_data_feed()

    sym = ob.binance_symbol.lower()
    assert sym in fake_manager.subscriptions

