"""
Server-side metrics collection for QuantFlow.

Tracks latency percentiles, throughput, queue drops, active connections,
memory footprint, and reconnection recovery times.
"""

import time
import tracemalloc
import threading
from collections import defaultdict
from typing import Dict, List, Optional


class LatencyTracker:
    """Sliding-window latency tracker with percentile support."""

    def __init__(self, max_samples: int = 10000):
        self._samples: List[float] = []
        self._max = max_samples
        self._lock = threading.Lock()

    def record(self, latency_ms: float):
        with self._lock:
            self._samples.append(latency_ms)
            if len(self._samples) > self._max:
                self._samples = self._samples[-self._max:]

    def percentile(self, p: float) -> Optional[float]:
        with self._lock:
            if not self._samples:
                return None
            sorted_s = sorted(self._samples)
            idx = int(len(sorted_s) * p / 100)
            idx = min(idx, len(sorted_s) - 1)
            return sorted_s[idx]

    def stats(self) -> Dict:
        with self._lock:
            if not self._samples:
                return {"count": 0, "p50": None, "p95": None, "p99": None, "min": None, "max": None, "mean": None}
            sorted_s = sorted(self._samples)
            n = len(sorted_s)
            return {
                "count": n,
                "p50": sorted_s[int(n * 0.50)],
                "p95": sorted_s[int(n * 0.95)] if n > 1 else sorted_s[0],
                "p99": sorted_s[int(n * 0.99)] if n > 1 else sorted_s[0],
                "min": sorted_s[0],
                "max": sorted_s[-1],
                "mean": round(sum(sorted_s) / n, 2),
            }

    def reset(self):
        with self._lock:
            self._samples.clear()


class MetricsCollector:
    """Central metrics collector for the entire pipeline."""

    def __init__(self):
        # Latency: binance receipt -> grpc send (server processing latency)
        self.server_latency = LatencyTracker()
        # Latency: binance receipt -> gateway websocket send (end-to-end backend)
        self.e2e_backend_latency = LatencyTracker()

        # Throughput counters
        self._msg_count: Dict[int, int] = defaultdict(int)  # instrument_id -> count
        self._msg_count_total: int = 0
        self._start_time: float = time.time()
        self._window_start: float = time.time()
        self._window_count: int = 0

        # Queue drop tracking
        self._queue_drops: Dict[int, int] = defaultdict(int)  # instrument_id -> drops
        self._queue_total: Dict[int, int] = defaultdict(int)  # instrument_id -> total attempts

        # Connection tracking
        self._active_connections: Dict[int, int] = defaultdict(int)  # instrument_id -> count
        self._total_connections: int = 0
        self._peak_connections: int = 0

        # Memory tracking
        self._mem_tracking_enabled = False
        self._mem_baseline: Optional[int] = None
        self._mem_per_connection_samples: List[float] = []

        # Reconnection tracking
        self._reconnections: Dict[str, List[Dict]] = defaultdict(list)  # symbol -> [{time, recovery_ms}]

        self._lock = threading.Lock()

    # --- Throughput ---

    def record_message(self, instrument_id: int):
        with self._lock:
            self._msg_count[instrument_id] += 1
            self._msg_count_total += 1
            self._window_count += 1

    def get_throughput(self) -> Dict:
        with self._lock:
            elapsed = time.time() - self._start_time
            window_elapsed = time.time() - self._window_start
            current_rate = self._window_count / window_elapsed if window_elapsed > 0 else 0
            avg_rate = self._msg_count_total / elapsed if elapsed > 0 else 0
            return {
                "total_messages": self._msg_count_total,
                "per_instrument": dict(self._msg_count),
                "avg_msg_per_sec": round(avg_rate, 2),
                "current_msg_per_sec": round(current_rate, 2),
                "uptime_seconds": round(elapsed, 1),
            }

    def reset_throughput_window(self):
        with self._lock:
            self._window_start = time.time()
            self._window_count = 0

    # --- Queue Drops ---

    def record_queue_attempt(self, instrument_id: int, dropped: bool):
        with self._lock:
            self._queue_total[instrument_id] += 1
            if dropped:
                self._queue_drops[instrument_id] += 1

    def get_queue_stats(self) -> Dict:
        with self._lock:
            stats = {}
            for iid in set(list(self._queue_total.keys()) + list(self._queue_drops.keys())):
                total = self._queue_total[iid]
                drops = self._queue_drops[iid]
                stats[iid] = {
                    "total_enqueued": total,
                    "dropped": drops,
                    "drop_rate_pct": round(drops / total * 100, 4) if total > 0 else 0,
                }
            total_all = sum(self._queue_total.values())
            drops_all = sum(self._queue_drops.values())
            return {
                "per_instrument": stats,
                "total_enqueued": total_all,
                "total_dropped": drops_all,
                "overall_drop_rate_pct": round(drops_all / total_all * 100, 4) if total_all > 0 else 0,
            }

    # --- Connections ---

    def record_connection(self, instrument_id: int):
        with self._lock:
            self._active_connections[instrument_id] += 1
            self._total_connections += 1
            current = sum(self._active_connections.values())
            if current > self._peak_connections:
                self._peak_connections = current

    def record_disconnection(self, instrument_id: int):
        with self._lock:
            self._active_connections[instrument_id] = max(0, self._active_connections[instrument_id] - 1)

    def get_connection_stats(self) -> Dict:
        with self._lock:
            return {
                "active": dict(self._active_connections),
                "active_total": sum(self._active_connections.values()),
                "peak_concurrent": self._peak_connections,
                "total_served": self._total_connections,
            }

    # --- Memory ---

    def enable_memory_tracking(self):
        if not self._mem_tracking_enabled:
            tracemalloc.start()
            self._mem_tracking_enabled = True
            self._mem_baseline = tracemalloc.get_traced_memory()[0]

    def snapshot_memory(self) -> Dict:
        if not self._mem_tracking_enabled:
            return {"enabled": False}
        current, peak = tracemalloc.get_traced_memory()
        active = sum(self._active_connections.values())
        per_conn = None
        if active > 0 and self._mem_baseline is not None:
            per_conn = round((current - self._mem_baseline) / active / 1024, 2)  # KB
            self._mem_per_connection_samples.append(per_conn)
        return {
            "enabled": True,
            "current_kb": round(current / 1024, 2),
            "peak_kb": round(peak / 1024, 2),
            "baseline_kb": round(self._mem_baseline / 1024, 2) if self._mem_baseline else None,
            "estimated_per_connection_kb": per_conn,
            "active_connections": active,
        }

    # --- Reconnections ---

    def record_reconnection(self, symbol: str, recovery_ms: float):
        with self._lock:
            self._reconnections[symbol].append({
                "time": time.time(),
                "recovery_ms": round(recovery_ms, 2),
            })

    def get_reconnection_stats(self) -> Dict:
        with self._lock:
            stats = {}
            for symbol, events in self._reconnections.items():
                recoveries = [e["recovery_ms"] for e in events]
                stats[symbol] = {
                    "count": len(events),
                    "avg_recovery_ms": round(sum(recoveries) / len(recoveries), 2) if recoveries else None,
                    "max_recovery_ms": max(recoveries) if recoveries else None,
                    "min_recovery_ms": min(recoveries) if recoveries else None,
                }
            return stats

    # --- Full Report ---

    def full_report(self) -> Dict:
        return {
            "latency": {
                "server_processing": self.server_latency.stats(),
                "e2e_backend": self.e2e_backend_latency.stats(),
            },
            "throughput": self.get_throughput(),
            "queue": self.get_queue_stats(),
            "connections": self.get_connection_stats(),
            "memory": self.snapshot_memory(),
            "reconnections": self.get_reconnection_stats(),
        }


# Singleton instance for the entire server process
metrics = MetricsCollector()
