"use client";
import { useCallback, useEffect, useRef, useState } from "react";

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

export type Subscribe = (cb: (m: WsMsg) => void) => () => void;

export function useWebSocket(url?: string): { connected: boolean; subscribe: Subscribe } {
  const [connected, setConnected] = useState(false);
  const listenersRef = useRef<Set<(m: WsMsg) => void>>(new Set());

  const subscribe = useCallback<Subscribe>((cb) => {
    listenersRef.current.add(cb);
    return () => {
      listenersRef.current.delete(cb);
    };
  }, []);

  useEffect(() => {
    if (!url) return;
    let ws: WebSocket;
    let attempt = 0;
    let disposed = false;
    let reconnectTimer: ReturnType<typeof setTimeout>;

    function connect() {
      if (disposed) return;
      ws = new WebSocket(url!);

      ws.onopen = () => {
        setConnected(true);
        attempt = 0;
      };
      ws.onclose = () => {
        setConnected(false);
        if (!disposed) {
          const delay = Math.min(1000 * 2 ** attempt, 30000);
          attempt++;
          reconnectTimer = setTimeout(connect, delay);
        }
      };
      ws.onmessage = (e) => {
        let data: WsMsg;
        try {
          data = JSON.parse(e.data);
        } catch (err) {
          console.error("WS parse error", err);
          return;
        }
        listenersRef.current.forEach((cb) => cb(data));
      };
    }

    connect();

    return () => {
      disposed = true;
      clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, [url]);

  return { connected, subscribe };
}
