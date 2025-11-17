"use client";
import { useEffect, useMemo, useState } from "react";
import DepthChart from "@/components/DepthChart";
import { useWebSocket } from "@/hooks/useWebSocket";
import { useOrderbook } from "@/hooks/useOrderbook";

const GATEWAY = process.env.NEXT_PUBLIC_GATEWAY_URL || "http://localhost:8001";

function StatusBadge({ connected }: { connected: boolean }) {
  return (
    <span className={`badge ${connected ? "badge-ok" : "badge-warn"}`}>
      <span className={`dot ${connected ? "dot-ok" : "dot-warn"}`} />
      {connected ? "Connected" : "Disconnected"}
    </span>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
    </div>
  );
}

export default function Page() {
  const [instruments, setInstruments] = useState<{ id: number; symbol: string; depth: number }[]>([]);
  const [instId, setInstId] = useState<string>("");
  const instIdNum = instId ? parseInt(instId, 10) : undefined;

  useEffect(() => {
    fetch(`${GATEWAY}/instruments`)
      .then((r) => r.json())
      .then(setInstruments)
      .catch((e) => console.error("Failed to load instruments:", e));
  }, []);

  const wsBase = GATEWAY.replace(/^http/, "ws");
  const wsUrl = instIdNum != null ? `${wsBase}/ws?instrumentId=${instIdNum}` : undefined;

  const { messages, connected } = useWebSocket(wsUrl);
  const { book, spread, latencyMs, dropped } = useOrderbook(messages);
  const current = useMemo(
    () => (instIdNum != null ? instruments.find((i) => i.id === instIdNum) : undefined),
    [instruments, instIdNum]
  );

  return (
    <div className="page">
      <header className="header">
        <div className="header-left">
          <h1 className="title">QuantFlow Dashboard</h1>
          <StatusBadge connected={!!connected} />
        </div>
        <div className="header-right">
          <div className="select-wrap">
            <label htmlFor="instrument">Instrument</label>
            <select
              id="instrument"
              className="select"
              value={instId}
              onChange={(e) => setInstId(e.target.value)}
              disabled={!instruments.length}
            >
              <option value="">Select Instrument</option>
              {instruments.map((i) => (
                <option key={i.id} value={String(i.id)}>
                  {i.symbol} (id {i.id})
                </option>
              ))}
            </select>
          </div>
          {current && <div className="meta">Depth: {current.depth}</div>}
        </div>
      </header>
      <section className="grid-4">
        <StatCard label="Spread" value={spread ? spread.value.toFixed(4) : "—"} />
        <StatCard label="Mid Price" value={spread ? spread.mid.toFixed(4) : "—"} />
        <StatCard label="Latency" value={`${latencyMs} ms`} />
        <StatCard label="Dropped" value={String(dropped)} />
      </section>
      <section>
        <DepthChart bids={book.bids.slice(0, 10)} asks={book.asks.slice(0, 10)} />
      </section>
      {!instId && (
        <div className="hint">
          <p>Select an instrument to start streaming live orderbook data.</p>
        </div>
      )}
    </div>
  );
}
