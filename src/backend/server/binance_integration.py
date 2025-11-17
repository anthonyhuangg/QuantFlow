import asyncio
import aiohttp
import json
import websockets
import time

class BinanceOrderbookManager:
    """Manages real-time orderbook data from Binance WebSocket streams."""

    def __init__(self):
        self.orderbooks = {}  # symbol -> orderbook data
        self.subscribers = {}  # symbol -> list of callback functions
        self.websocket_tasks = {}  # symbol -> websocket task
        self.base_url = "wss://stream.binance.com:9443/ws/"

    async def subscribe_to_symbol(self, symbol, callback):
        """Subscribe to orderbook updates for a symbol"""
        symbol = symbol.lower()

        if symbol not in self.subscribers:
            self.subscribers[symbol] = []

        self.subscribers[symbol].append(callback)

        if symbol not in self.websocket_tasks:
            self.websocket_tasks[symbol] = asyncio.create_task(
                self._start_websocket_stream(symbol)
            )

    async def unsubscribe_from_symbol(self, symbol, callback):
        """Unsubscribe from orderbook updates"""
        symbol = symbol.lower()

        if symbol in self.subscribers and callback in self.subscribers[symbol]:
            self.subscribers[symbol].remove(callback)

            # Stop WebSocket if no more subscribers
            if not self.subscribers[symbol] and symbol in self.websocket_tasks:
                self.websocket_tasks[symbol].cancel()
                del self.websocket_tasks[symbol]
                del self.subscribers[symbol]

    async def _start_websocket_stream(self, symbol):
        """Start WebSocket stream for a symbol."""
        stream_name = f"{symbol}@depth@100ms"
        url = f"{self.base_url}{stream_name}"

        while True:
            try:
                print(f"Connecting to Binance WebSocket for {symbol.upper()}...")
                async with websockets.connect(url) as websocket:
                    print(f"Connected to {symbol.upper()} stream")

                    async for message in websocket:
                        data = json.loads(message)
                        await self._process_orderbook_update(symbol, data)

            except websockets.exceptions.ConnectionClosed:
                print(f"WebSocket connection closed for {symbol}, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Error in WebSocket stream for {symbol}: {e}")
                await asyncio.sleep(5)

    async def _process_orderbook_update(self, symbol, data):
        """Process incoming orderbook data from Binance."""
        if 'b' not in data or 'a' not in data:
            return

        bids = [(float(price), float(qty)) for price, qty in data['b'] if float(qty) > 0]
        asks = [(float(price), float(qty)) for price, qty in data['a'] if float(qty) > 0]

        # Sort bids (highest first) and asks (lowest first)
        bids.sort(reverse=True)
        asks.sort()

        orderbook_data = {
            'symbol': symbol.upper(),
            'bids': bids,
            'asks': asks,
            'timestamp': int(time.time() * 1000),
            'last_update_id': data.get('lastUpdateId', 0)
        }

        self.orderbooks[symbol] = orderbook_data

        # Notify all subscribers
        if symbol in self.subscribers:
            for callback in self.subscribers[symbol]:
                try:
                    await callback(orderbook_data)
                except Exception as e:
                    print(f"Error calling subscriber callback: {e}")

    async def get_snapshot(self, symbol):
        """Get current orderbook snapshot"""
        return self.orderbooks.get(symbol.lower())

    async def get_available_symbols(self):
        """Get list of available trading symbols from Binance"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/exchangeInfo') as response:
                    data = await response.json()
                    symbols = []
                    for symbol_info in data['symbols']:
                        if symbol_info['status'] == 'TRADING':
                            symbols.append(symbol_info['symbol'])
                    return symbols[:50]
        except Exception as e:
            print(f"Error fetching symbols: {e}")
            return ['BTCUSDT', 'ETHUSDT', 'ADAUSDT']

class RealDataOrderbook:
    """Wrapper to integrate Binance data with internal orderbook structure."""

    def __init__(self, instrument, binance_manager):
        self.instrument = instrument
        self.binance_manager = binance_manager
        self.current_data = None
        self._subscribers = []
        self.binance_symbol = self._map_to_binance_symbol(instrument.symbol)

    def _map_to_binance_symbol(self, symbol):
        """Map internal symbol format to Binance symbol format"""
        symbol_map = {
            'BTC': 'BTCUSDT',
            'ETH': 'ETHUSDT',
            'ADA': 'ADAUSDT'
        }
        return symbol_map.get(symbol.upper(), 'BTCUSDT')

    async def start_real_data_feed(self):
        """Start receiving real data from Binance"""
        await self.binance_manager.subscribe_to_symbol(
            self.binance_symbol,
            self._handle_binance_update
        )

    async def stop_real_data_feed(self):
        """Stop receiving real data"""
        await self.binance_manager.unsubscribe_from_symbol(
            self.binance_symbol,
            self._handle_binance_update
        )

    async def _handle_binance_update(self, orderbook_data):
        """Handle updates from Binance"""
        self.current_data = orderbook_data

        for callback in self._subscribers:
            try:
                await callback(orderbook_data)
            except Exception as e:
                print(f"Error notifying subscriber: {e}")

    def add_subscriber(self, callback):
        """Add a callback for orderbook updates"""
        self._subscribers.append(callback)

    def remove_subscriber(self, callback):
        """Remove a callback"""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def get_current_snapshot(self):
        """Get current orderbook snapshot in your format"""
        if not self.current_data:
            return None

        return {
            'instrument_id': self.instrument.id,
            'symbol': self.instrument.symbol,
            'bids': self.current_data['bids'][:self.instrument.depth],
            'asks': self.current_data['asks'][:self.instrument.depth],
            'timestamp': self.current_data['timestamp']
        }
