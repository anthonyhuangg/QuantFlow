import asyncio
import time

import pytest

from backend.server.server import Instrument, MarketDataServicer
from backend.server.binance_integration import RealDataOrderbook, BinanceOrderbookManager
from backend.server.metrics import MetricsCollector, LatencyTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeBinanceManager:
    def __init__(self):
        self.subscriptions = {}  # symbol -> list[callback]

    async def subscribe_to_symbol(self, symbol: str, callback):
        self.subscriptions.setdefault(symbol.lower(), []).append(callback)

    async def unsubscribe_from_symbol(self, symbol: str, callback):
        callbacks = self.subscriptions.get(symbol.lower(), [])
        if callback in callbacks:
            callbacks.remove(callback)


def _make_raw(symbol="BTCUSDT", n_levels=5, bid_start=100.0, ask_start=100.5):
    """Generate synthetic orderbook data."""
    return {
        "symbol": symbol,
        "bids": [(bid_start - i * 0.5, float(i + 1)) for i in range(n_levels)],
        "asks": [(ask_start + i * 0.5, float(i + 1) * 1.1) for i in range(n_levels)],
        "timestamp": int(time.time() * 1000),
        "last_update_id": 12345,
        "binance_receipt_ts": time.time() * 1000,
    }


# ---------------------------------------------------------------------------
# RealDataOrderbook Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_realdata_orderbook_snapshot():
    """Snapshot should limit depth to instrument config."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=3)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    raw_data = _make_raw(n_levels=5)
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
    """Subscribers should receive updates on new data."""
    inst = Instrument(instrument_id=2, symbol="ETH", depth=2)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    received = []

    async def subscriber_cb(data):
        received.append(data)

    ob.add_subscriber(subscriber_cb)

    raw_data = _make_raw(symbol="ETHUSDT", n_levels=2, bid_start=200.0, ask_start=201.0)
    await ob._handle_binance_update(raw_data)

    assert len(received) == 1
    assert received[0]["symbol"] == "ETHUSDT"
    assert received[0]["bids"][0][0] == 200.0


@pytest.mark.asyncio
async def test_realdata_orderbook_full_cycle():
    """Start and stop feed should register/unregister with binance manager."""
    inst = Instrument(instrument_id=3, symbol="NVDA", depth=2)
    fake_manager = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake_manager)

    await ob.start_real_data_feed()
    await ob.stop_real_data_feed()

    sym = ob.binance_symbol.lower()
    assert sym in fake_manager.subscriptions


@pytest.mark.asyncio
async def test_snapshot_returns_none_before_data():
    """Snapshot should return None before any data arrives."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())
    assert ob.get_current_snapshot() is None


@pytest.mark.asyncio
async def test_snapshot_depth_equals_one():
    """Edge case: depth=1 should return exactly 1 level per side."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=1)
    ob = RealDataOrderbook(inst, FakeBinanceManager())
    await ob._handle_binance_update(_make_raw(n_levels=10))
    snap = ob.get_current_snapshot()
    assert len(snap["bids"]) == 1
    assert len(snap["asks"]) == 1


@pytest.mark.asyncio
async def test_snapshot_depth_exceeds_available():
    """When depth > available levels, return all available."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=20)
    ob = RealDataOrderbook(inst, FakeBinanceManager())
    await ob._handle_binance_update(_make_raw(n_levels=3))
    snap = ob.get_current_snapshot()
    assert len(snap["bids"]) == 3
    assert len(snap["asks"]) == 3


@pytest.mark.asyncio
async def test_multiple_subscribers():
    """Multiple subscribers should all receive updates."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())

    results_a, results_b = [], []

    async def cb_a(data):
        results_a.append(data)

    async def cb_b(data):
        results_b.append(data)

    ob.add_subscriber(cb_a)
    ob.add_subscriber(cb_b)

    await ob._handle_binance_update(_make_raw())
    assert len(results_a) == 1
    assert len(results_b) == 1


@pytest.mark.asyncio
async def test_remove_subscriber():
    """Removed subscriber should not receive further updates."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())

    received = []

    async def cb(data):
        received.append(data)

    ob.add_subscriber(cb)
    await ob._handle_binance_update(_make_raw())
    assert len(received) == 1

    ob.remove_subscriber(cb)
    await ob._handle_binance_update(_make_raw())
    assert len(received) == 1  # no new messages


@pytest.mark.asyncio
async def test_subscriber_error_does_not_break_others():
    """A failing subscriber should not prevent other subscribers from receiving data."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())

    good_results = []

    async def bad_cb(data):
        raise RuntimeError("subscriber crash")

    async def good_cb(data):
        good_results.append(data)

    ob.add_subscriber(bad_cb)
    ob.add_subscriber(good_cb)

    await ob._handle_binance_update(_make_raw())
    assert len(good_results) == 1


@pytest.mark.asyncio
async def test_start_feed_idempotent():
    """Calling start_real_data_feed twice should not double-subscribe."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    fake = FakeBinanceManager()
    ob = RealDataOrderbook(inst, fake)

    await ob.start_real_data_feed()
    await ob.start_real_data_feed()

    sym = ob.binance_symbol.lower()
    assert len(fake.subscriptions.get(sym, [])) == 1


@pytest.mark.asyncio
async def test_stop_feed_idempotent():
    """Calling stop_real_data_feed when not started should be safe."""
    inst = Instrument(instrument_id=1, symbol="BTC", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())
    await ob.stop_real_data_feed()  # should not raise


@pytest.mark.asyncio
async def test_symbol_mapping():
    """Internal symbols should map correctly to Binance symbols."""
    fake = FakeBinanceManager()
    for sym, expected in [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT"), ("ADA", "ADAUSDT")]:
        inst = Instrument(instrument_id=1, symbol=sym, depth=5)
        ob = RealDataOrderbook(inst, fake)
        assert ob.binance_symbol == expected


@pytest.mark.asyncio
async def test_unknown_symbol_defaults_to_btcusdt():
    """Unknown symbols should default to BTCUSDT."""
    inst = Instrument(instrument_id=1, symbol="UNKNOWN", depth=5)
    ob = RealDataOrderbook(inst, FakeBinanceManager())
    assert ob.binance_symbol == "BTCUSDT"


# ---------------------------------------------------------------------------
# BinanceOrderbookManager Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_binance_manager_process_valid_data():
    """Valid Binance data should be parsed and stored."""
    mgr = BinanceOrderbookManager()
    data = {
        "bids": [["100.5", "1.0"], ["100.0", "2.0"]],
        "asks": [["101.0", "1.5"], ["101.5", "2.5"]],
        "lastUpdateId": 999,
    }
    await mgr._process_orderbook_update("btcusdt", data)
    ob = mgr.orderbooks.get("btcusdt")
    assert ob is not None
    assert ob["bids"][0][0] == 100.5  # highest bid first
    assert ob["asks"][0][0] == 101.0  # lowest ask first


@pytest.mark.asyncio
async def test_binance_manager_ignores_malformed_data():
    """Data without bids/asks should be silently ignored."""
    mgr = BinanceOrderbookManager()
    await mgr._process_orderbook_update("btcusdt", {"foo": "bar"})
    assert "btcusdt" not in mgr.orderbooks


@pytest.mark.asyncio
async def test_binance_manager_notifies_subscribers():
    """Subscribers should be notified on new data."""
    mgr = BinanceOrderbookManager()
    received = []

    async def cb(data):
        received.append(data)

    mgr.subscribers["btcusdt"] = [cb]

    data = {"bids": [["50000", "1"]], "asks": [["50001", "1"]], "lastUpdateId": 1}
    await mgr._process_orderbook_update("btcusdt", data)
    assert len(received) == 1
    assert received[0]["symbol"] == "BTCUSDT"


# ---------------------------------------------------------------------------
# Metrics Tests
# ---------------------------------------------------------------------------

def test_latency_tracker_empty():
    """Empty tracker should return None for all percentiles."""
    lt = LatencyTracker()
    stats = lt.stats()
    assert stats["count"] == 0
    assert stats["p50"] is None


def test_latency_tracker_percentiles():
    """Percentiles should be calculated correctly."""
    lt = LatencyTracker()
    for i in range(100):
        lt.record(float(i))

    stats = lt.stats()
    assert stats["count"] == 100
    assert stats["p50"] == 50.0
    assert stats["p95"] == 95.0
    assert stats["p99"] == 99.0
    assert stats["min"] == 0.0
    assert stats["max"] == 99.0
    assert stats["mean"] == 49.5


def test_latency_tracker_max_samples():
    """Tracker should evict old samples when max is reached."""
    lt = LatencyTracker(max_samples=10)
    for i in range(20):
        lt.record(float(i))
    stats = lt.stats()
    assert stats["count"] == 10
    assert stats["min"] == 10.0  # oldest samples evicted


def test_latency_tracker_reset():
    """Reset should clear all samples."""
    lt = LatencyTracker()
    lt.record(1.0)
    lt.record(2.0)
    lt.reset()
    assert lt.stats()["count"] == 0


def test_metrics_throughput():
    """Throughput counters should increment correctly."""
    m = MetricsCollector()
    m.record_message(1)
    m.record_message(1)
    m.record_message(2)

    tp = m.get_throughput()
    assert tp["total_messages"] == 3
    assert tp["per_instrument"][1] == 2
    assert tp["per_instrument"][2] == 1


def test_metrics_queue_drops():
    """Queue drop tracking should record drops and totals."""
    m = MetricsCollector()
    m.record_queue_attempt(1, dropped=False)
    m.record_queue_attempt(1, dropped=False)
    m.record_queue_attempt(1, dropped=True)

    qs = m.get_queue_stats()
    assert qs["total_enqueued"] == 3
    assert qs["total_dropped"] == 1
    assert 33.0 < qs["overall_drop_rate_pct"] < 34.0


def test_metrics_queue_zero_drops():
    """Zero drops should show 0% drop rate."""
    m = MetricsCollector()
    for _ in range(100):
        m.record_queue_attempt(1, dropped=False)

    qs = m.get_queue_stats()
    assert qs["overall_drop_rate_pct"] == 0


def test_metrics_connections():
    """Connection tracking should count active, peak, total."""
    m = MetricsCollector()
    m.record_connection(1)
    m.record_connection(1)
    m.record_connection(2)

    cs = m.get_connection_stats()
    assert cs["active_total"] == 3
    assert cs["peak_concurrent"] == 3
    assert cs["total_served"] == 3

    m.record_disconnection(1)
    cs = m.get_connection_stats()
    assert cs["active_total"] == 2
    assert cs["peak_concurrent"] == 3  # peak should not decrease


def test_metrics_reconnections():
    """Reconnection stats should track recovery times."""
    m = MetricsCollector()
    m.record_reconnection("btcusdt", 5200.0)
    m.record_reconnection("btcusdt", 5800.0)
    m.record_reconnection("ethusdt", 6000.0)

    rs = m.get_reconnection_stats()
    assert rs["btcusdt"]["count"] == 2
    assert rs["btcusdt"]["avg_recovery_ms"] == 5500.0
    assert rs["ethusdt"]["count"] == 1


def test_metrics_full_report():
    """Full report should contain all sections."""
    m = MetricsCollector()
    m.record_message(1)
    m.record_queue_attempt(1, dropped=False)
    m.record_connection(1)

    report = m.full_report()
    assert "latency" in report
    assert "throughput" in report
    assert "queue" in report
    assert "connections" in report
    assert "memory" in report
    assert "reconnections" in report
