"use client";
import { useEffect, useRef, useState } from "react";

export type SnapMsg = {
  type: "snapshot";
  instrument_id: number;
  timestamp: number;
  bids: [number, number][];
  asks: [number, number][];
  gateway_ts?: number;
};

export type IncMsg = {
  type: "incremental";
  instrument_id: number;
  timestamp: number;
  is_bid: boolean;
  update_type: number;
  level: [number, number];
  gateway_ts?: number;
};
export type WsMsg = SnapMsg | IncMsg;

export function useWebSocket(url?: string) {
  const wsRef = useRef<WebSocket | null>(null);
  const [messages, setMessages] = useState<WsMsg[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    if (!url) return;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);
      setMessages((prev) =>
        prev.length > 1000 ? prev.slice(-800).concat(data) : prev.concat(data)
      );
    };
    return () => ws.close();
  }, [url]);

  return { connected, messages };
}
