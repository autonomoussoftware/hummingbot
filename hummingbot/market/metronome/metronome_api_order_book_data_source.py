#!/usr/bin/env python

import asyncio

import aiohttp
import logging
import pandas as pd
import uuid
from typing import List, Optional, Dict, Any
import time
from decimal import Decimal
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.market.metronome.metronome_active_order_tracker import MetronomeActiveOrderTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.metronome.metronome_order_book import MetronomeOrderBook
from hummingbot.market.metronome.metronome_order_book_tracker_entry import MetronomeOrderBookTrackerEntry
from hummingbot.wallet.ethereum.ethereum_chain import EthereumChain


class MetronomeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    METRONOME_MAINNET_REST_ENDPOINT = "https://api.metronome.io/acc"

    # Setup local api for testnet https://github.com/autonomoussoftware/metronome-api
    METRONOME_ROPSTEN_REST_ENDPOINT = "http://localhost:3002/acc"
    TICKER_URL = "/ticker"
    ORDER_BOOK_URL = "/orderbook"

    __daobds__logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.__daobds__logger is None:
            cls.__daobds__logger = logging.getLogger(__name__)
        return cls.__daobds__logger

    def __init__(self, trading_pairs: Optional[List[str]] = None, chain: EthereumChain = EthereumChain.MAIN_NET):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._active_markets = None
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()
        if chain is EthereumChain.MAIN_NET:
            self._api_endpoint = self.METRONOME_MAINNET_REST_ENDPOINT
        elif chain is EthereumChain.ROPSTEN:
            self._api_endpoint = self.METRONOME_ROPSTEN_REST_ENDPOINT

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls, api_endpoint: str = METRONOME_MAINNET_REST_ENDPOINT) -> pd.DataFrame:
        async with aiohttp.ClientSession() as client:
            markets_response: aiohttp.ClientResponse = await client.get(f"{api_endpoint}{cls.TICKER_URL}")
            if markets_response.status != 200:
                raise IOError(f"Error fetching active Metronome markets. HTTP status is {markets_response.status}.")
            markets_data = await markets_response.json()
            data = [{
                "market": "MET-ETH",
                "baseAsset": "MET",
                "quoteAsset": "ETH",
                "USDVolume": markets_data["volume"]
            }]

            all_markets: pd.DataFrame = pd.DataFrame.from_records(
                data=data, index="market", columns=list(data[0].keys())
            )
            return all_markets

    @property
    async def exchange_name(self) -> str:
        return "metronome"

    @property
    def order_book_class(self) -> MetronomeOrderBook:
        return MetronomeOrderBook

    async def fetch(url, session):
        async with session.get(url) as response:
            return await response.read()

    async def api_request(self,
                          client: aiohttp.ClientSession,
                          http_method: str,
                          url: str,
                          params: Optional[List[Any]] = None) -> Dict[str, Any]:
        async with client.request(http_method,
                                  url=url,
                                  params=params) as response:
            if response.status != 200:
                raise IOError(f"Error fetching Metronome market snapshot from {url}. HTTP status is {response.status}.")
            response_data = await response.json()

            return response_data

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:

        url = f"{self._api_endpoint}{self.ORDER_BOOK_URL}"
        order_book = await self.api_request(client, "get", url)

        for bid, ask in zip(order_book["bid"], order_book["ask"]):
            bid["orderId"] = str(uuid.uuid4())
            bid["amount_base"] = Decimal(bid["size"])
            bid["amount_quote"] = Decimal(bid["size"]) * Decimal(bid["price"])

            ask["orderId"] = str(uuid.uuid4())
            ask["amount_base"] = Decimal(ask["size"])
            ask["amount_quote"] = Decimal(ask["size"]) * Decimal(ask["price"])

        snapshot = {
            "data": {
                "orderBook": {
                    "marketId": trading_pair,
                    "bids": order_book["bid"],
                    "asks": order_book["ask"]
                }
            }
        }
        return snapshot

    async def get_tracking_pairs(self):
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()

            retval: Dict[str, MetronomeOrderBookTrackerEntry] = {}

            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg = MetronomeOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        {"marketId": trading_pair}
                    )

                    metronome_order_book: OrderBook = self.order_book_create_function()
                    metronome_active_order_tracker: MetronomeActiveOrderTracker = MetronomeActiveOrderTracker()

                    bids, asks = metronome_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)

                    metronome_order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
                    retval[trading_pair] = MetronomeOrderBookTrackerEntry(
                        trading_pair,
                        snapshot_timestamp,
                        metronome_order_book,
                        metronome_active_order_tracker
                    )
                    return retval
                except IOError:
                    self.logger().network(
                        f"Error getting snapshot for {trading_pair}.",
                        exc_info=True,
                        app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
                    )
                    await asyncio.sleep(5.0)
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
                    await asyncio.sleep(5.0)

    async def get_trading_pairs(self) -> List[str]:
        if self._active_markets is None:
            self._active_markets: pd.DataFrame = await self.get_active_exchange_markets(self._api_endpoint)

        if not self._trading_pairs:
            try:
                self._trading_pairs = self._active_markets.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass
