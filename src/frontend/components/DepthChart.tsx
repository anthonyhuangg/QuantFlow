"use client";

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import { useMemo, useRef, useState } from "react";

function buildSide(
  levels: [number, number][],
  side: "bid" | "ask"
): { price: number; cumulative: number; side: string }[] {
  const sorted = levels
    .slice()
    .sort((a, b) => (side === "bid" ? b[0] - a[0] : a[0] - b[0]));

  let cumulative = 0;

  const result = sorted.map(([price, qty]) => {
    cumulative += qty;
    return { price, cumulative, side };
  });

  if (side === "bid") result.reverse();

  return result;
}

type SideRow = { price: number; cumulative: number; side: string };

export default function DepthChart({
  bids,
  asks,
}: {
  bids: [number, number][];
  asks: [number, number][];
}) {
  const [paused, setPaused] = useState(false);
  const frozenRef = useRef<{ bidData: SideRow[]; askData: SideRow[] } | null>(null);

  const liveData = useMemo(
    () => ({
      bidData: buildSide(bids, "bid"),
      askData: buildSide(asks, "ask"),
    }),
    [bids, asks]
  );

  const { bidData, askData } = paused && frozenRef.current ? frozenRef.current : liveData;

  const togglePause = () => {
    if (!paused) frozenRef.current = liveData;
    else frozenRef.current = null;
    setPaused((p) => !p);
  };

  const allPrices = [
    ...bidData.map((b) => b.price),
    ...askData.map((a) => a.price),
  ];
  const hasData = allPrices.length > 0;
  const minPrice = hasData ? Math.min(...allPrices) : 0;
  const maxPrice = hasData ? Math.max(...allPrices) : 1;
  const padding = (maxPrice - minPrice) * 0.12 || 1;

  return (
    <div className="card">
      <div className="card-header">
        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          <h3>Depth</h3>
          <div className="legend">
            <span className="legend-item">
              <span className="legend-dot" style={{ background: "#0ecb81" }} />
              Bids
            </span>
            <span className="legend-item">
              <span className="legend-dot" style={{ background: "#f6465d" }} />
              Asks
            </span>
          </div>
        </div>
        <button
          onClick={togglePause}
          className={`btn ${paused ? "btn-paused" : "btn-active"}`}
        >
          {paused ? "● Resume" : "❚❚ Pause"}
        </button>
      </div>

      <div className="card-body" style={{ height: 420, position: "relative", padding: "16px 12px 8px 4px" }}>
        {paused && (
          <div
            style={{
              position: "absolute",
              top: 14,
              right: 18,
              background: "rgba(246, 70, 93, 0.1)",
              border: "1px solid rgba(246, 70, 93, 0.3)",
              color: "#f6465d",
              padding: "3px 10px",
              borderRadius: 3,
              fontSize: 11,
              fontWeight: 500,
              zIndex: 5,
            }}
          >
            Paused — snapshot
          </div>
        )}

        <ResponsiveContainer width="100%" height="100%">
          <AreaChart margin={{ top: 10, right: 12, left: 12, bottom: 20 }}>
            <CartesianGrid vertical={false} />

            <XAxis
              type="number"
              dataKey="price"
              domain={[minPrice - padding, maxPrice + padding]}
              tick={{ fontSize: 11, fill: "#848e9c" }}
              tickFormatter={(v) => v.toFixed(2)}
              stroke="#2b3139"
              tickLine={false}
              axisLine={{ stroke: "#2b3139" }}
            />

            <YAxis
              type="number"
              dataKey="cumulative"
              domain={[0, "auto"]}
              tick={{ fontSize: 11, fill: "#848e9c" }}
              tickFormatter={(v) => v.toFixed(2)}
              stroke="#2b3139"
              tickLine={false}
              axisLine={false}
              width={56}
              orientation="right"
            />

            <Tooltip
              formatter={(v: any) => Number(v).toFixed(4)}
              labelFormatter={(v: any) => `${Number(v).toFixed(2)}`}
              contentStyle={{
                background: "#1e2329",
                border: "1px solid #2b3139",
                borderRadius: 4,
                color: "#eaecef",
                fontFamily: "IBM Plex Mono, monospace",
                fontSize: 12,
                padding: "6px 10px",
                boxShadow: "0 4px 12px rgba(0,0,0,0.5)",
              }}
              itemStyle={{ color: "#eaecef", padding: 0 }}
              labelStyle={{ color: "#fcd535", marginBottom: 4, fontSize: 11, fontWeight: 600 }}
              cursor={{ stroke: "#474d57", strokeWidth: 1, strokeDasharray: "3 3" }}
            />

            <Area
              data={bidData}
              type="stepAfter"
              dataKey="cumulative"
              stroke="#0ecb81"
              strokeWidth={1.5}
              fill="rgba(14, 203, 129, 0.15)"
              name="Bids"
              isAnimationActive={false}
            />

            <Area
              data={askData}
              type="stepAfter"
              dataKey="cumulative"
              stroke="#f6465d"
              strokeWidth={1.5}
              fill="rgba(246, 70, 93, 0.15)"
              name="Asks"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
