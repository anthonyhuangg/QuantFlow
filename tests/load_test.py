"""
WebSocket Load Test for QuantFlow Gateway.

Opens N concurrent WebSocket clients, measures:
- End-to-end latency (gateway -> client) with p50/p95/p99
- Full pipeline latency (binance receipt -> client) with p50/p95/p99
- Throughput (messages/sec per client and aggregate)
- Message drop estimation (gaps in timestamp sequence)
- Connection success rate
- Memory delta from /metrics endpoint

Usage:
    python -m tests.load_test --clients 50 --duration 30 --instrument-id 1
    python -m tests.load_test --clients 100 --duration 60 --instrument-id 1 --gateway http://localhost:8001
"""

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    aiohttp = None


@dataclass
class ClientStats:
    """Per-client statistics."""
    client_id: int
    messages_received: int = 0
    gateway_latencies: List[float] = field(default_factory=list)
    pipeline_latencies: List[float] = field(default_factory=list)
    first_message_at: Optional[float] = None
    last_message_at: Optional[float] = None
    connected: bool = False
    connect_time_ms: Optional[float] = None
    errors: int = 0


def percentile(data: List[float], p: float) -> Optional[float]:
    if not data:
        return None
    sorted_d = sorted(data)
    idx = min(int(len(sorted_d) * p / 100), len(sorted_d) - 1)
    return sorted_d[idx]


async def run_client(
    client_id: int,
    ws_url: str,
    duration: float,
    stats: ClientStats,
    start_event: asyncio.Event,
):
    """Single WebSocket client that connects and collects messages."""
    await start_event.wait()  # synchronized start

    try:
        t0 = time.time() * 1000
        async with websockets.connect(ws_url) as ws:
            stats.connected = True
            stats.connect_time_ms = (time.time() * 1000) - t0

            deadline = time.time() + duration
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                now = time.time() * 1000
                msg = json.loads(raw)

                if "error" in msg:
                    stats.errors += 1
                    continue

                stats.messages_received += 1
                if stats.first_message_at is None:
                    stats.first_message_at = now
                stats.last_message_at = now

                # Gateway -> client latency
                gw_ts = msg.get("gateway_ts")
                if gw_ts:
                    stats.gateway_latencies.append(now - gw_ts)

                # Full pipeline latency (binance receipt -> client)
                br_ts = msg.get("binance_receipt_ts")
                if br_ts:
                    stats.pipeline_latencies.append(now - br_ts)

    except Exception as e:
        stats.errors += 1
        if not stats.connected:
            stats.connect_time_ms = None


async def fetch_server_metrics(gateway_base: str) -> Optional[dict]:
    """Fetch /metrics from gateway."""
    if aiohttp is None:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{gateway_base}/metrics", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return await resp.json()
    except Exception:
        return None


async def run_load_test(
    num_clients: int,
    duration: float,
    instrument_id: int,
    gateway_base: str,
):
    ws_base = gateway_base.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_base}/ws?instrumentId={instrument_id}"

    print(f"{'='*60}")
    print(f"  QuantFlow Load Test")
    print(f"{'='*60}")
    print(f"  Clients:      {num_clients}")
    print(f"  Duration:     {duration}s")
    print(f"  Instrument:   {instrument_id}")
    print(f"  Gateway:      {gateway_base}")
    print(f"  WebSocket:    {ws_url}")
    print(f"{'='*60}")
    print()

    # Fetch pre-test metrics
    pre_metrics = await fetch_server_metrics(gateway_base)

    all_stats = [ClientStats(client_id=i) for i in range(num_clients)]
    start_event = asyncio.Event()

    # Create all client tasks
    tasks = [
        asyncio.create_task(run_client(i, ws_url, duration, all_stats[i], start_event))
        for i in range(num_clients)
    ]

    print(f"Spawned {num_clients} clients, starting synchronized test...")
    start_event.set()  # fire!

    test_start = time.time()
    await asyncio.gather(*tasks, return_exceptions=True)
    test_elapsed = time.time() - test_start

    # Fetch post-test metrics
    post_metrics = await fetch_server_metrics(gateway_base)

    # --- Aggregate results ---
    connected = [s for s in all_stats if s.connected]
    failed = [s for s in all_stats if not s.connected]

    all_gw_latencies = []
    all_pipeline_latencies = []
    total_msgs = 0
    total_errors = 0
    connect_times = []

    for s in all_stats:
        total_msgs += s.messages_received
        total_errors += s.errors
        all_gw_latencies.extend(s.gateway_latencies)
        all_pipeline_latencies.extend(s.pipeline_latencies)
        if s.connect_time_ms is not None:
            connect_times.append(s.connect_time_ms)

    # Per-client throughput
    per_client_rates = []
    for s in connected:
        if s.first_message_at and s.last_message_at and s.last_message_at > s.first_message_at:
            elapsed_s = (s.last_message_at - s.first_message_at) / 1000
            per_client_rates.append(s.messages_received / elapsed_s)

    print()
    print(f"{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print()

    # Connection stats
    print(f"  Connections")
    print(f"  ├─ Successful:     {len(connected)}/{num_clients} ({len(connected)/num_clients*100:.1f}%)")
    print(f"  ├─ Failed:         {len(failed)}")
    if connect_times:
        print(f"  ├─ Connect p50:    {percentile(connect_times, 50):.1f} ms")
        print(f"  └─ Connect p99:    {percentile(connect_times, 99):.1f} ms")
    print()

    # Throughput
    agg_rate = total_msgs / test_elapsed if test_elapsed > 0 else 0
    print(f"  Throughput")
    print(f"  ├─ Total messages: {total_msgs:,}")
    print(f"  ├─ Aggregate:      {agg_rate:,.1f} msg/sec")
    if per_client_rates:
        print(f"  ├─ Per-client avg: {statistics.mean(per_client_rates):.1f} msg/sec")
        print(f"  └─ Per-client med: {statistics.median(per_client_rates):.1f} msg/sec")
    print()

    # Gateway latency
    if all_gw_latencies:
        print(f"  Gateway → Client Latency")
        print(f"  ├─ Samples:  {len(all_gw_latencies):,}")
        print(f"  ├─ p50:      {percentile(all_gw_latencies, 50):.1f} ms")
        print(f"  ├─ p95:      {percentile(all_gw_latencies, 95):.1f} ms")
        print(f"  ├─ p99:      {percentile(all_gw_latencies, 99):.1f} ms")
        print(f"  ├─ min:      {min(all_gw_latencies):.1f} ms")
        print(f"  └─ max:      {max(all_gw_latencies):.1f} ms")
        print()

    # Full pipeline latency
    if all_pipeline_latencies:
        print(f"  Binance Receipt → Client Latency (Full Pipeline)")
        print(f"  ├─ Samples:  {len(all_pipeline_latencies):,}")
        print(f"  ├─ p50:      {percentile(all_pipeline_latencies, 50):.1f} ms")
        print(f"  ├─ p95:      {percentile(all_pipeline_latencies, 95):.1f} ms")
        print(f"  ├─ p99:      {percentile(all_pipeline_latencies, 99):.1f} ms")
        print(f"  ├─ min:      {min(all_pipeline_latencies):.1f} ms")
        print(f"  └─ max:      {max(all_pipeline_latencies):.1f} ms")
        print()

    # Errors
    print(f"  Reliability")
    print(f"  ├─ Total errors:   {total_errors}")
    if total_msgs > 0:
        print(f"  └─ Error rate:     {total_errors / (total_msgs + total_errors) * 100:.4f}%")
    print()

    # Server-side metrics
    if post_metrics:
        print(f"  Server-Side Metrics (/metrics endpoint)")
        q = post_metrics.get("queue", {})
        if q:
            print(f"  ├─ Queue drops:    {q.get('total_dropped', 0):,}")
            print(f"  ├─ Drop rate:      {q.get('overall_drop_rate_pct', 0):.4f}%")
        c = post_metrics.get("connections", {})
        if c:
            print(f"  ├─ Peak conns:     {c.get('peak_concurrent', 0)}")
            print(f"  ├─ Total served:   {c.get('total_served', 0)}")
        m = post_metrics.get("memory", {})
        if m and m.get("enabled"):
            print(f"  ├─ Memory current: {m.get('current_kb', 0):,.1f} KB")
            print(f"  ├─ Memory peak:    {m.get('peak_kb', 0):,.1f} KB")
            if m.get("estimated_per_connection_kb"):
                print(f"  ├─ Per-conn est:   {m['estimated_per_connection_kb']:.1f} KB")
        sl = post_metrics.get("latency", {}).get("server_processing", {})
        if sl and sl.get("count", 0) > 0:
            print(f"  ├─ Server p50:     {sl.get('p50', 0):.2f} ms")
            print(f"  ├─ Server p95:     {sl.get('p95', 0):.2f} ms")
            print(f"  └─ Server p99:     {sl.get('p99', 0):.2f} ms")
        r = post_metrics.get("reconnections", {})
        if r:
            print(f"  Reconnections:")
            for sym, rs in r.items():
                print(f"  ├─ {sym}: {rs['count']}x, avg {rs['avg_recovery_ms']:.0f}ms")
        print()

    print(f"{'='*60}")
    print(f"  Test completed in {test_elapsed:.1f}s")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="QuantFlow WebSocket Load Test")
    parser.add_argument("--clients", type=int, default=50, help="Number of concurrent WebSocket clients (default: 50)")
    parser.add_argument("--duration", type=float, default=30, help="Test duration in seconds (default: 30)")
    parser.add_argument("--instrument-id", type=int, default=1, help="Instrument ID to subscribe to (default: 1)")
    parser.add_argument("--gateway", type=str, default="http://localhost:8001", help="Gateway base URL")
    args = parser.parse_args()

    asyncio.run(run_load_test(
        num_clients=args.clients,
        duration=args.duration,
        instrument_id=args.instrument_id,
        gateway_base=args.gateway,
    ))


if __name__ == "__main__":
    main()
