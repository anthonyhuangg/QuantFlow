"use client";
import { useEffect, useMemo, useRef, useState } from "react";

type Book = { bids: [number, number][], asks: [number, number][], lastSeq?: number };

export type SnapshotMsg = {
  type: "snapshot";
  instrument_id: number;
  timestamp: number;
  bids: [number, number][];
  asks: [number, number][];
  gateway_ts?: number;
  seq?: number;
};

export type IncrementalMsg = {
  type: "incremental";
  instrument_id: number;
  timestamp: number;
  is_bid: boolean;
  update_type: number;
  level: [number, number];
  gateway_ts?: number;
  seq?: number;
};

export type WsMsg = SnapshotMsg | IncrementalMsg;

export function useOrderbook(messages: WsMsg[]) {
  const [book, setBook] = useState<Book>({ bids: [], asks: [] });
  const latency = useRef<number[]>([]);
  const dropped = useRef<number>(0);

  useEffect(() => {
    if (messages.length === 0) return;
    const last = messages[messages.length - 1];

    // latency from exchange browser
    latency.current.push(Date.now() - last.timestamp);
    if (latency.current.length > 200) latency.current.shift();

    if (last.type === "snapshot") {
      setBook({ bids: last.bids, asks: last.asks });
      return;
    }

    // incremental
    setBook(prev => {
      const next = { ...prev };
      const [price, qty] = last.level;
      const side = last.is_bid ? "bids" : "asks";
      let arr = next[side].slice();

      if (last.update_type === 0 || last.update_type === 2) {
        const i = arr.findIndex(([p]) => p === price);
        if (i >= 0) arr[i] = [price, qty];
        else arr.push([price, qty]);
      } else if (last.update_type === 1) {
        arr = arr.filter(([p]) => p !== price);
      }

      arr.sort((a, b) => last.is_bid ? b[0] - a[0] : a[0] - b[0]);
      next[side] = arr.slice(0, 50);
      return next;
    });
  }, [messages]);

  const spread = useMemo(() => {
    const bestBid = book.bids[0]?.[0];
    const bestAsk = book.asks[0]?.[0];
    if (bestBid == null || bestAsk == null) return null;
    return { value: bestAsk - bestBid, mid: (bestAsk + bestBid) / 2 };
  }, [book]);

  const latencyMs = useMemo(() => {
    const arr = latency.current;
    if (arr.length === 0) return 0;
    return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
  }, [messages]);

  return { book, spread, latencyMs, dropped: dropped.current };
}
