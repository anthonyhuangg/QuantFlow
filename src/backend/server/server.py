import asyncio
import grpc
import json
from concurrent import futures
from collections import defaultdict

from . import market_data_pb2, market_data_pb2_grpc

from .binance_integration import BinanceOrderbookManager, RealDataOrderbook

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

        self.use_real_data = config.use_real_data()
        if self.use_real_data:
            self.binance_manager = BinanceOrderbookManager()
            print("Using market data from Binance")
        else:
            print("Using simulated market data")

        # Initialise order books for all instruments
        for instrument in self.config.get_instruments():
            inst = Instrument(instrument['Id'], instrument['Symbol'], instrument['Specifications']['Depth'])

            if self.use_real_data:
                orderbook = RealDataOrderbook(inst, self.binance_manager)
                self.orderbooks[inst.id] = orderbook

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

        # Create a queue for this specific stream
        update_queue = asyncio.Queue()
        self._stream_queues[instrument_id][context] = update_queue
        self._active_streams[instrument_id].add(context)

        try:
            orderbook = self.orderbooks[instrument_id]

            if self.use_real_data:
                # Start real data feed and add this stream as subscriber
                await orderbook.start_real_data_feed()
                orderbook.add_subscriber(lambda data: self._queue_update_for_streams(instrument_id, data))

                # Send initial snapshot if available
                snapshot_data = orderbook.get_current_snapshot()
                if snapshot_data:
                    snapshot_update = self._create_snapshot_update(snapshot_data)
                    yield snapshot_update
                else:
                    await asyncio.sleep(2)
                    snapshot_data = orderbook.get_current_snapshot()
                    if snapshot_data:
                        snapshot_update = self._create_snapshot_update(snapshot_data)
                        yield snapshot_update

            # Stream updates from the queue
            while not context.cancelled():
                try:
                    update = await asyncio.wait_for(update_queue.get(), timeout=1.0)
                    yield update
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        except Exception as e:
            print(f"Error in subscription for instrument {instrument_id}: {e}")
        finally:
            self._active_streams[instrument_id].discard(context)
            if context in self._stream_queues[instrument_id]:
                del self._stream_queues[instrument_id][context]
            
            if self.use_real_data and instrument_id in self.orderbooks:
                if not self._active_streams[instrument_id]:
                    await self.orderbooks[instrument_id].stop_real_data_feed()

    def _create_snapshot_update(self, snapshot_data):
        """Create a protobuf snapshot update from orderbook data"""
        return market_data_pb2.OrderbookUpdate(
            snapshot=market_data_pb2.OrderbookSnapshot(
                instrument_id=snapshot_data['instrument_id'],
                bids=[market_data_pb2.PriceLevel(price=price, quantity=qty) 
                      for price, qty in snapshot_data['bids']],
                asks=[market_data_pb2.PriceLevel(price=price, quantity=qty) 
                      for price, qty in snapshot_data['asks']],
                timestamp=snapshot_data['timestamp']
            )
        )

    async def _queue_update_for_streams(self, instrument_id, orderbook_data):
        """Queue updates for all active streams of an instrument"""
        if instrument_id not in self._stream_queues:
            return

        snapshot_data = {
            'instrument_id': instrument_id,
            'bids': orderbook_data['bids'],
            'asks': orderbook_data['asks'],
            'timestamp': orderbook_data['timestamp']
        }

        update = self._create_snapshot_update(snapshot_data)

        # Add to all stream queues for this instrument
        for stream_context, queue in self._stream_queues[instrument_id].items():
            try:
                await queue.put(update)
            except Exception as e:
                print(f"Error queuing update for stream: {e}")

async def serve():
    config = ServerConfig("config.json")
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

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