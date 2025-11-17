"use client";

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Label,
} from "recharts";
import { useMemo, useState } from "react";

// Build cumulative sides (bids descending, asks ascending)
function buildSide(
  levels: [number, number][],
  side: "bid" | "ask"
): { price: number; cumulative: number; side: string }[] {
  const sorted = levels
    .slice()
    .sort((a, b) => (side === "bid" ? b[0] - a[0] : a[0] - b[0]));

  let cumulative = 0;

  return sorted.map(([price, qty]) => {
    cumulative += qty;
    return { price, cumulative, side };
  });
}

export default function DepthChart({
  bids,
  asks,
}: {
  bids: [number, number][];
  asks: [number, number][];
}) {
  const [paused, setPaused] = useState(false);
  const [frozenData, setFrozenData] = useState<{
    bidData: any[];
    askData: any[];
  } | null>(null);

  // If paused, use frozen snapshot else update live
  const { bidData, askData } = useMemo(() => {
    if (paused && frozenData) return frozenData;

    const newData = {
      bidData: buildSide(bids, "bid"),
      askData: buildSide(asks, "ask"),
    };

    if (!paused) setFrozenData(newData);
    return newData;
  }, [bids, asks, paused]);

  // Compute global price range
  const allPrices = [
    ...bidData.map((b) => b.price),
    ...askData.map((a) => a.price),
  ];

  const minPrice = Math.min(...allPrices);
  const maxPrice = Math.max(...allPrices);
  const padding = (maxPrice - minPrice) * 0.12;

  return (
    <div className="card">
      <div
        className="card-header"
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <h3>Orderbook Depth</h3>
        <button
          onClick={() => setPaused((p) => !p)}
          style={{
            background: paused ? "#ef4444" : "#16a34a",
            color: "white",
            border: "none",
            borderRadius: 6,
            padding: "4px 10px",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {paused ? "Resume" : "Pause"}
        </button>
      </div>

      <div className="card-body" style={{ height: 380, position: "relative" }}>
        {paused && (
          <div
            style={{
              position: "absolute",
              top: 8,
              right: 12,
              background: "rgba(0,0,0,0.4)",
              color: "white",
              padding: "2px 8px",
              borderRadius: 4,
              fontSize: 12,
            }}
          >
            ⏸ Paused — showing snapshot
          </div>
        )}

        <ResponsiveContainer width="100%" height="100%">
          <AreaChart>
            <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />

            <XAxis
              type="number"
              dataKey="price"
              domain={[minPrice - padding, maxPrice + padding]}
              tick={{ fontSize: 12 }}
              tickFormatter={(v) => v.toFixed(2)}
              stroke="#aaa"
            >
              <Label
                value="Price"
                offset={-5}
                position="insideBottom"
                style={{ fill: "#aaa", fontSize: 13 }}
              />
            </XAxis>

            <YAxis
              type="number"
              dataKey="cumulative"
              domain={[0, "auto"]}
              tick={{ fontSize: 12 }}
              stroke="#aaa"
            >
              <Label
                value="Cumulative Quantity"
                angle={-90}
                position="insideLeft"
                style={{
                  fill: "#aaa",
                  fontSize: 13,
                  textAnchor: "middle",
                }}
              />
            </YAxis>

            <Tooltip
              formatter={(v: any) => Number(v).toFixed(3)}
              contentStyle={{
                background: "#121722",
                border: "1px solid rgba(255,255,255,0.1)",
                color: "#fff",
                fontFamily: "Inter, system-ui, sans-serif",
              }}
            />

            <Area
              data={bidData}
              type="stepAfter"
              dataKey="cumulative"
              stroke="#16a34a"
              fill="#16a34a33"
              name="Bids"
              isAnimationActive={false}
            />

            <Area
              data={askData}
              type="stepAfter"
              dataKey="cumulative"
              stroke="#ef4444"
              fill="#ef444433"
              name="Asks"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
