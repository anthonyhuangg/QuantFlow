import asyncio
import grpc
import signal
import time

import server.market_data_pb2_grpc as market_data_pb2_grpc
import server.market_data_pb2 as market_data_pb2


class MarketDataClient:
    def __init__(self, server_address='localhost:14000'):
        self.server_address = server_address
        self.channel = None
        self.stub = None

        # runtime flags
        self._running = True
        self._printing_enabled = True

        # stream control
        self._current_instrument_id = None
        self._subscription_task = None

        # throttling prints
        self._last_incremental_print = 0
        self._last_snapshot_print = 0
        self._print_interval = 1.0  # seconds

    async def connect(self):
        self.channel = grpc.aio.insecure_channel(self.server_address)
        self.stub = market_data_pb2_grpc.MarketDataServiceStub(self.channel)
        print(f"Connected to market data server at {self.server_address}")

    async def disconnect(self):
        if self.channel:
            await self.channel.close()
            print("Disconnected from server")

    async def get_instruments(self):
        try:
            request = market_data_pb2.Empty()
            response = await self.stub.GetInstruments(request)
            return list(response.instruments)
        except grpc.RpcError as e:
            print(f"Error getting instruments: {e}")
            return []

    async def subscribe_to_orderbook(self, instrument_id):
        """Runs until cancelled or client stopped"""
        try:
            request = market_data_pb2.SubscriptionRequest(instrument_id=instrument_id)
            print(f"Subscribing to orderbook for instrument {instrument_id}...")
            async for update in self.stub.SubscribeOrderbook(request):
                if not self._running or instrument_id != self._current_instrument_id:
                    break
                self._handle_orderbook_update(update)
        except asyncio.CancelledError:
            print(f"Subscription to instrument {instrument_id} cancelled")
        except grpc.RpcError as e:
            print(f"Error subscribing to orderbook: {e}")

    def start_stream(self, instrument_id):
        """Cancel existing stream (if any) and start a new one"""
        self.stop_stream()
        self._current_instrument_id = instrument_id
        self._subscription_task = asyncio.create_task(
            self.subscribe_to_orderbook(instrument_id)
        )

    def stop_stream(self):
        """Stop current stream if running"""
        if self._subscription_task and not self._subscription_task.done():
            self._subscription_task.cancel()
        self._subscription_task = None
        self._current_instrument_id = None

    def _handle_orderbook_update(self, update):
        if not self._printing_enabled:
            return
        if update.HasField('snapshot'):
            self._handle_snapshot(update.snapshot)
        elif update.HasField('incremental'):
            self._handle_incremental(update.incremental)

    def _handle_snapshot(self, snapshot):
        now = time.time()
        if now - self._last_snapshot_print < self._print_interval:
            return
        self._last_snapshot_print = now

        print(f"\n=== SNAPSHOT for Instrument {snapshot.instrument_id} ===")
        print(f"Timestamp: {snapshot.timestamp}")
        print("BIDS:")
        for i, bid in enumerate(snapshot.bids[:5]):
            print(f"  {i+1}: {bid.price:.2f} @ {bid.quantity:.2f}")
        print("ASKS:")
        for i, ask in enumerate(snapshot.asks[:5]):
            print(f"  {i+1}: {ask.price:.2f} @ {ask.quantity:.2f}")
        print("=" * 50)
    
    # 
    def _handle_incremental(self, incremental):
        now = time.time()
        if now - self._last_incremental_print < self._print_interval:
            return
        self._last_incremental_print = now

        update_type_str = {
            market_data_pb2.ADD: "ADD",
            market_data_pb2.REMOVE: "REMOVE",
            market_data_pb2.REPLACE: "REPLACE"
        }.get(incremental.update_type, "UNKNOWN")

        side = "BID" if incremental.is_bid else "ASK"
        print(
            f"UPDATE [{incremental.timestamp}] "
            f"Instrument {incremental.instrument_id}: "
            f"{update_type_str} {side} "
            f"{incremental.level.price:.2f} @ {incremental.level.quantity:.2f}"
        )

    def stop(self):
        self._running = False
        self.stop_stream()

    async def input_loop(self, instruments):
        """Reads commands from stdin"""
        help_text = (
            "\nCommands:\n"
            "  l / list                 - list instruments\n"
            "  s <index|id>             - start stream for instrument (by list index or id)\n"
            "  c / close                - close current stream\n"
            "  p / pause                - pause printing\n"
            "  r / resume               - resume printing\n"
            "  i                        - show current instrument\n"
            "  h / help                 - show help\n"
            "  q / quit                 - exit\n"
        )
        print(help_text)
        index_by_id = {inst.id: idx for idx, inst in enumerate(instruments)}

        def format_row(idx, inst):
            return f"[{idx}] id={inst.id} symbol={inst.symbol} depth={inst.depth}"

        def print_list():
            print("\nAvailable instruments:")
            for idx, inst in enumerate(instruments):
                print(" ", format_row(idx, inst))

        print_list()

        loop = asyncio.get_running_loop()
        while self._running:
            try:
                line = await loop.run_in_executor(None, input, "> ")
            except (EOFError, KeyboardInterrupt):
                self.stop()
                break

            cmd = (line or "").strip().split()
            if not cmd:
                continue

            op = cmd[0].lower()

            if op in ("h", "help"):
                print(help_text)

            elif op in ("l", "list"):
                print_list()

            elif op in ("i",):
                if self._current_instrument_id is None:
                    print("No active stream.")
                else:
                    idx = index_by_id.get(self._current_instrument_id, "?")
                    print(f"Active stream -> id={self._current_instrument_id} (list index {idx})")

            elif op in ("p", "pause"):
                self._printing_enabled = False
                print("Printing paused.")

            elif op in ("r", "resume"):
                self._printing_enabled = True
                print("Printing resumed.")

            elif op in ("c", "close"):
                if self._current_instrument_id is None:
                    print("No active stream to close.")
                else:
                    print(f"Closing stream for {self._current_instrument_id}...")
                    self.stop_stream()

            elif op in ("s", "start"):
                if len(cmd) < 2:
                    print("Usage: s <index|id>")
                    continue
                sel = cmd[1]
                instrument_id = None

                if sel.isdigit():
                    idx = int(sel)
                    if 0 <= idx < len(instruments):
                        instrument_id = instruments[idx].id
                    else:
                        print("Index out of range.")
                        continue
                else:
                    try:
                        instrument_id = int(sel)
                    except ValueError:
                        print("Provide a numeric instrument id.")
                        continue

                print(f"Starting stream for instrument id={instrument_id} ...")
                self.start_stream(instrument_id)

            elif op in ("q", "quit", "exit"):
                self.stop()
                break

            else:
                print("Unknown command. Type 'h' for help.")


async def main():
    client = MarketDataClient()

    # graceful shutdown from Ctrl+C
    def sig_handler(signum, frame):
        print("\nReceived shutdown signal...")
        client.stop()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        await client.connect()
        instruments = await client.get_instruments()
        if not instruments:
            print("No instruments available")
            return

        input_task = asyncio.create_task(client.input_loop(instruments))
        await input_task

    finally:
        client.stop()
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown complete")
