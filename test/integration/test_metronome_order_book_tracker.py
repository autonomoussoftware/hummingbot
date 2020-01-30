#!/usr/bin/env python
from os.path import (
    join,
    realpath
)
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    OrderBookEvent
)
import asyncio
import logging
import unittest
from typing import (
    Dict,
    Optional,
    List
)
from hummingbot.market.metronome.metronome_order_book_tracker import MetronomeOrderBookTracker
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTrackerDataSourceType
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.wallet.ethereum.ethereum_chain import EthereumChain


class MetronomeOrderBookTrackerUnitTest(unittest.TestCase):
    order_book_tracker: Optional[MetronomeOrderBookTracker] = None
    events: List[OrderBookEvent] = [
        OrderBookEvent.TradeEvent
    ]

    trading_pairs: List[str] = ["MET-ETH"]

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_tracker: MetronomeOrderBookTracker = MetronomeOrderBookTracker(
            data_source_type=OrderBookTrackerDataSourceType.EXCHANGE_API,
            trading_pairs=cls.trading_pairs,
            chain=EthereumChain.ROPSTEN)
        cls.order_book_tracker_task: asyncio.Task = safe_ensure_future(cls.order_book_tracker.start())
        cls.ev_loop.run_until_complete(cls.wait_til_tracker_ready())

    @classmethod
    async def wait_til_tracker_ready(cls):
        while True:
            if len(cls.order_book_tracker.order_books) > 0:
                print("Initialized real-time order books.")
                return
            await asyncio.sleep(1)

    async def run_parallel_async(self, *tasks, timeout=None):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        timer = 0
        while not future.done():
            if timeout and timer > timeout:
                raise Exception("Time out running parallel async task in tests.")
            timer += 1
            # now = time.time()
            # next_iteration = now // 1.0 + 1
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks, timeout=None):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks, timeout=timeout))

    def setUp(self):
        self.event_logger = EventLogger()
        for event_tag in self.events:
            for trading_pair, order_book in self.order_book_tracker.order_books.items():
                order_book.add_listener(event_tag, self.event_logger)

    def test_tracker_integrity(self):
        # Wait 5 seconds to process some diffs.
        self.ev_loop.run_until_complete(asyncio.sleep(5.0))
        order_books: Dict[str, OrderBook] = self.order_book_tracker.order_books
        met_eth_book: OrderBook = order_books["MET-ETH"]
        print(met_eth_book.get_price_for_volume(True, 10).result_price)
        self.assertGreaterEqual(met_eth_book.get_price_for_volume(True, 10).result_price,
                                met_eth_book.get_price(True))
        self.assertLessEqual(met_eth_book.get_price_for_volume(False, 10).result_price,
                             met_eth_book.get_price(False))


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
