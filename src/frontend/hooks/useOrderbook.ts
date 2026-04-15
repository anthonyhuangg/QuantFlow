"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import type { Subscribe, WsMsg } from "./useWebSocket";

type Book = { bids: [number, number][]; asks: [number, number][] };

export function useOrderbook(subscribe: Subscribe) {
  const [book, setBook] = useState<Book>({ bids: [], asks: [] });
  const [latencyMs, setLatencyMs] = useState(0);
  const latencyArr = useRef<number[]>([]);

  useEffect(() => {
    return subscribe((msg: WsMsg) => {
      const gwTs = msg.gateway_ts;
      if (gwTs) {
        latencyArr.current.push(Date.now() - gwTs);
        if (latencyArr.current.length > 200) latencyArr.current.shift();
        const sum = latencyArr.current.reduce((a, b) => a + b, 0);
        setLatencyMs(Math.round(sum / latencyArr.current.length));
      }

      if (msg.type === "snapshot") {
        setBook({ bids: msg.bids, asks: msg.asks });
        return;
      }

      setBook((prev) => {
        const [price, qty] = msg.level;
        const side = msg.is_bid ? "bids" : "asks";
        let arr = prev[side].slice();

        if (msg.update_type === 0 || msg.update_type === 2) {
          const i = arr.findIndex(([p]) => p === price);
          if (i >= 0) arr[i] = [price, qty];
          else arr.push([price, qty]);
        } else if (msg.update_type === 1) {
          arr = arr.filter(([p]) => p !== price);
        }

        arr.sort((a, b) => (msg.is_bid ? b[0] - a[0] : a[0] - b[0]));
        return { ...prev, [side]: arr.slice(0, 50) };
      });
    });
  }, [subscribe]);

  const spread = useMemo(() => {
    const bestBid = book.bids[0]?.[0];
    const bestAsk = book.asks[0]?.[0];
    if (bestBid == null || bestAsk == null) return null;
    return { value: bestAsk - bestBid, mid: (bestAsk + bestBid) / 2 };
  }, [book]);

  return { book, spread, latencyMs };
}
