import asyncio
import json
import time
import os
from typing import Dict, Any
from contextlib import asynccontextmanager

import grpc
from fastapi import FastAPI, WebSocketDisconnect, WebSocket, Request
from fastapi.middleware.cors import CORSMiddleware

import server.market_data_pb2 as pb
import server.market_data_pb2_grpc as pb_grpc

GRPC_HOST = os.getenv("GRPC_HOST", "localhost")
GRPC_PORT = int(os.getenv("GRPC_PORT", "14000"))

@asynccontextmanager
async def lifespan(app):
    channel = grpc.aio.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
    stub = pb_grpc.MarketDataServiceStub(channel)

    app.state.channel = channel
    app.state.stub = stub

    try:
        yield
    finally:
        if channel:
            await channel.close()

app = FastAPI(title="QuantFlow Gateway", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# List available instruments
@app.get("/instruments")
async def instruments(request: Request):
    stub = request.app.state.stub
    resp = await stub.GetInstruments(pb.Empty())
    return [{"id": i.id, "symbol": i.symbol, "depth": i.depth} for i in resp.instruments]

# Health check
@app.get("/health")
async def health():
    return {"status": "ok"}

# Stream updates as JSON
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    stream_call = None
    try:
        qs = dict(ws.query_params)
        if "instrumentId" not in qs:
            await ws.send_text(json.dumps({"error": "instrumentId required"}))
            await ws.close()
            return

        instrument_id = int(qs["instrumentId"])
        stub = ws.app.state.stub
        req = pb.SubscriptionRequest(instrument_id=instrument_id)

        # Subscribe to gRPC stream
        stream_call = stub.SubscribeOrderbook(req)

        # Forward updates to WebSocket client
        async for update in stream_call:
            payload: Dict[str, Any] = {"gateway_ts": int(time.perf_counter() * 1000)}
            if update.HasField("snapshot"):
                s = update.snapshot
                payload.update({
                    "type": "snapshot",
                    "instrument_id": s.instrument_id,
                    "timestamp": s.timestamp,
                    "bids": [[b.price, b.quantity] for b in s.bids],
                    "asks": [[a.price, a.quantity] for a in s.asks],
                })
            else:
                inc = update.incremental
                payload.update({
                    "type": "incremental",
                    "instrument_id": inc.instrument_id,
                    "timestamp": inc.timestamp,
                    "is_bid": inc.is_bid,
                    "update_type": inc.update_type,
                    "level": [inc.level.price, inc.level.quantity],
                })
            await ws.send_text(json.dumps(payload))

    except WebSocketDisconnect:
        # client closed the socket
        pass
    except asyncio.CancelledError:
        # server shutdown / task cancel
        pass
    except grpc.RpcError as e:
        try:
            await ws.send_text(json.dumps({"error": str(e)}))
        except Exception:
            pass
    finally:
        try:
            if stream_call is not None:
                await stream_call.cancel()
        except Exception:
            pass
        try:
            await ws.close()
        except Exception:
            pass
