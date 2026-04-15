import asyncio
import grpc
import json
import os
import time
from collections import defaultdict

from . import market_data_pb2, market_data_pb2_grpc

from .binance_integration import BinanceOrderbookManager, RealDataOrderbook
from .metrics import metrics

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.json")

class Instrument:
    def __init__(self, instrument_id, symbol, depth):
        self.id = instrument_id
        self.symbol = symbol
        self.depth = depth

class ServerConfig:
    def __init__(self, filename):
        with open(filename, "r") as f:
            self.config = json.load(f)

    def get_port(self):
        return self.config.get("Port", 14000)

    def get_instruments(self):
        return self.config.get("Instruments", [])

    def use_real_data(self):
        return self.config.get("UseRealData", False)

class MarketDataServicer(market_data_pb2_grpc.MarketDataServiceServicer):
    def __init__(self, config):
        self.config = config
        self.orderbooks = {}
        self._active_streams = defaultdict(set)
        self._stream_queues = defaultdict(dict)  # instrument_id -> {stream_context -> queue}
        self._instrument_callbacks = {}  # instrument_id -> single callback

        self.use_real_data = config.use_real_data()
        if not self.use_real_data:
            raise NotImplementedError(
                "Simulated market data mode is not implemented. "
                "Set UseRealData=true in config.json."
            )

        self.binance_manager = BinanceOrderbookManager()
        metrics.enable_memory_tracking()
        print("Using market data from Binance")

        for instrument in self.config.get_instruments():
            inst = Instrument(instrument['Id'], instrument['Symbol'], instrument['Specifications']['Depth'])
            self.orderbooks[inst.id] = RealDataOrderbook(inst, self.binance_manager)

    async def GetInstruments(self, request, context):
        """Return all available instruments"""
        instruments = []
        for instrument_config in self.config.get_instruments():
            instruments.append(
                market_data_pb2.Instrument(
                    id=instrument_config['Id'],
                    symbol=instrument_config['Symbol'],
                    depth=instrument_config['Specifications']['Depth']
                )
            )

        return market_data_pb2.InstrumentsResponse(instruments=instruments)

    async def SubscribeOrderbook(self, request, context):
        """Stream orderbook updates for a specific instrument"""
        instrument_id = request.instrument_id

        if instrument_id not in self.orderbooks:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'Instrument {instrument_id} not found')
            return

        update_queue = asyncio.Queue(maxsize=100)
        self._stream_queues[instrument_id][context] = update_queue
        self._active_streams[instrument_id].add(context)
        metrics.record_connection(instrument_id)

        orderbook = self.orderbooks[instrument_id]

        # Register a single shared callback per instrument, not per stream
        if instrument_id not in self._instrument_callbacks:
            async def update_callback(data, iid=instrument_id):
                await self._queue_update_for_streams(iid, data)
            self._instrument_callbacks[instrument_id] = update_callback
            orderbook.add_subscriber(update_callback)

        try:
            await orderbook.start_real_data_feed()

            snapshot_data = orderbook.get_current_snapshot()
            if not snapshot_data:
                await asyncio.sleep(2)
                snapshot_data = orderbook.get_current_snapshot()
            if snapshot_data:
                update, _ = self._create_snapshot_update(snapshot_data)
                yield update

            while not context.cancelled():
                try:
                    item = await asyncio.wait_for(update_queue.get(), timeout=1.0)
                    update, binance_ts = item
                    yield update
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        except Exception as e:
            print(f"Error in subscription for instrument {instrument_id}: {e}")
        finally:
            self._active_streams[instrument_id].discard(context)
            self._stream_queues[instrument_id].pop(context, None)
            metrics.record_disconnection(instrument_id)

            if not self._active_streams[instrument_id]:
                cb = self._instrument_callbacks.pop(instrument_id, None)
                if cb:
                    orderbook.remove_subscriber(cb)
                await orderbook.stop_real_data_feed()

    def _create_snapshot_update(self, snapshot_data):
        """Create a protobuf snapshot update from orderbook data.
        Returns (protobuf_update, binance_receipt_ts) tuple.
        """
        update = market_data_pb2.OrderbookUpdate(
            snapshot=market_data_pb2.OrderbookSnapshot(
                instrument_id=snapshot_data['instrument_id'],
                bids=[market_data_pb2.PriceLevel(price=price, quantity=qty)
                      for price, qty in snapshot_data['bids']],
                asks=[market_data_pb2.PriceLevel(price=price, quantity=qty)
                      for price, qty in snapshot_data['asks']],
                timestamp=snapshot_data['timestamp']
            )
        )
        return update, snapshot_data.get('binance_receipt_ts')

    async def _queue_update_for_streams(self, instrument_id, orderbook_data):
        """Queue updates for all active streams of an instrument"""
        if instrument_id not in self._stream_queues:
            return

        # Measure server processing latency (binance receipt -> queue fanout)
        binance_receipt_ts = orderbook_data.get('binance_receipt_ts')
        if binance_receipt_ts:
            server_latency = (time.time() * 1000) - binance_receipt_ts
            metrics.server_latency.record(server_latency)

        snapshot_data = {
            'instrument_id': instrument_id,
            'bids': orderbook_data['bids'],
            'asks': orderbook_data['asks'],
            'timestamp': orderbook_data['timestamp'],
            'binance_receipt_ts': binance_receipt_ts,
        }

        item = self._create_snapshot_update(snapshot_data)  # (update, binance_receipt_ts)
        metrics.record_message(instrument_id)

        # Add to all stream queues for this instrument (drop oldest if full)
        for stream_context, queue in list(self._stream_queues[instrument_id].items()):
            dropped = False
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                dropped = True
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(item)
                except asyncio.QueueFull:
                    pass
            metrics.record_queue_attempt(instrument_id, dropped)

async def serve():
    config = ServerConfig(CONFIG_PATH)
    server = grpc.aio.server()

    servicer = MarketDataServicer(config)
    market_data_pb2_grpc.add_MarketDataServiceServicer_to_server(servicer, server)

    port = config.get_port()
    server.add_insecure_port(f'[::]:{port}')

    data_source = "Binance" if config.use_real_data() else "simulated"
    print(f"Starting gRPC server on port {port} with {data_source} data...")
    await server.start()

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down server...")
        await server.stop(grace=5)

if __name__ == '__main__':
    asyncio.run(serve())