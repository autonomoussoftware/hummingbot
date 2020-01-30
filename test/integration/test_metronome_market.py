#!/usr/bin/env python
from os.path import (
    join,
    realpath
)
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
import asyncio
import conf
import contextlib
from decimal import Decimal
import logging
import os
import time
from typing import (
    List,
    Optional
)
import unittest

from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.event.events import (
    MarketEvent,
    WalletEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger import NETWORK
from hummingbot.market.metronome.metronome_market import MetronomeMarket
from hummingbot.market.market_base import OrderType
from hummingbot.market.markets_recorder import MarketsRecorder
from hummingbot.model.market_state import MarketState
from hummingbot.model.order import Order
from hummingbot.model.sql_connection_manager import (
    SQLConnectionManager,
    SQLConnectionType
)
from hummingbot.model.trade_fill import TradeFill
from hummingbot.wallet.ethereum.ethereum_chain import EthereumChain
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet

TRADING_PAIR = "MET-ETH"
s_decimal_0 = Decimal(0)


class MetronomeMarketUnitTest(unittest.TestCase):
    market_events: List[MarketEvent] = [
        MarketEvent.ReceivedAsset,
        MarketEvent.BuyOrderCompleted,
        MarketEvent.SellOrderCompleted,
        MarketEvent.WithdrawAsset,
        MarketEvent.OrderFilled,
        MarketEvent.BuyOrderCreated,
        MarketEvent.SellOrderCreated,
        MarketEvent.OrderCancelled
    ]

    wallet_events: List[WalletEvent] = [
        WalletEvent.WrappedEth,
        WalletEvent.UnwrappedEth
    ]

    wallet: Web3Wallet
    market: MetronomeMarket
    market_logger: EventLogger
    wallet_logger: EventLogger
    stack: contextlib.ExitStack

    @classmethod
    def setUpClass(cls):
        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.wallet = Web3Wallet(private_key=conf.metronome_test_wallet_private_key,
                                backend_urls=conf.test_web3_provider_list,
                                erc20_token_addresses=[conf.metronome_test_erc20_token_address],
                                chain=EthereumChain.ROPSTEN)
        cls.market: MetronomeMarket = MetronomeMarket(
            wallet=cls.wallet,
            ethereum_rpc_url=conf.test_web3_provider_list[0],
            order_book_tracker_data_source_type=OrderBookTrackerDataSourceType.EXCHANGE_API,
            trading_pairs=[TRADING_PAIR]
        )
        print("Initializing metronome market... ")
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.clock.add_iterator(cls.wallet)
        cls.clock.add_iterator(cls.market)
        cls.stack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)
        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        print("Ready.")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.close()

    @classmethod
    async def wait_til_ready(cls):
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if cls.market.ready:
                break
            else:
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    def setUp(self):
        self.db_path: str = realpath(join(__file__, "../metronome_test.sqlite"))
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

        self.market_logger = EventLogger()
        self.wallet_logger = EventLogger()
        for event_tag in self.market_events:
            self.market.add_listener(event_tag, self.market_logger)
        for event_tag in self.wallet_events:
            self.wallet.add_listener(event_tag, self.wallet_logger)

    def tearDown(self):
        for event_tag in self.market_events:
            self.market.remove_listener(event_tag, self.market_logger)
        self.market_logger = None
        for event_tag in self.wallet_events:
            self.wallet.remove_listener(event_tag, self.wallet_logger)
        self.wallet_logger = None

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        await self.market.start_network()
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def test_get_wallet_balances(self):
        balances = self.market.get_all_balances()
        self.assertGreaterEqual((balances["ETH"]), s_decimal_0)

    def test_quantize_order_amount(self):
        amount = self.market.quantize_order_amount(TRADING_PAIR, Decimal(1))
        self.assertEqual(1, amount)

    def test_cancel_all_failed_cancel(self):
        trading_pair = TRADING_PAIR
        buy_amount: Decimal = Decimal(10)
        buy_order_id: str = self.market.buy(trading_pair, buy_amount, OrderType.MARKET)
        [buy_order_created_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))
        self.assertEqual(buy_order_id, buy_order_created_event.order_id)
        self.assertEqual(buy_amount, float(buy_order_created_event.amount))
        self.assertEqual(TRADING_PAIR, buy_order_created_event.trading_pair)
        self.assertEqual(OrderType.MARKET, buy_order_created_event.type)

        trading_pair = TRADING_PAIR
        sell_amount: Decimal = Decimal(10)
        sell_order_id: str = self.market.sell(trading_pair, sell_amount, OrderType.MARKET)
        [sell_order_created_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCreatedEvent))
        self.assertEqual(sell_order_id, sell_order_created_event.order_id)
        self.assertEqual(sell_amount, float(sell_order_created_event.amount))
        self.assertEqual(TRADING_PAIR, sell_order_created_event.trading_pair)
        self.assertEqual(OrderType.MARKET, sell_order_created_event.type)

        [cancellation_results] = self.run_parallel(self.market.cancel_all(90))
        self.assertGreater(len(cancellation_results), 0)
        for cr in cancellation_results:
            self.assertEqual(False, cr.success)

    def test_market_buy(self):
        trading_pair = TRADING_PAIR
        current_price: Decimal = Decimal(self.market.get_price(trading_pair, True))
        buy_amount: Decimal = Decimal('0.16') / current_price
        buy_order_id: str = self.market.buy(trading_pair, buy_amount, OrderType.MARKET)
        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCompletedEvent))
        order_completed_event: BuyOrderCompletedEvent = order_completed_event
        self.assertEqual(buy_order_id, order_completed_event.order_id)

    def test_market_sell(self):
        trading_pair = TRADING_PAIR
        current_price: Decimal = Decimal(self.market.get_price(trading_pair, False))
        sell_amount: Decimal = Decimal('0.16') / current_price
        sell_order_id: str = self.market.sell(trading_pair, sell_amount, OrderType.MARKET)
        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))
        order_completed_event: SellOrderCompletedEvent = order_completed_event
        self.assertEqual(sell_order_id, order_completed_event.order_id)

    def test_orders_saving_and_restoration(self):
        config_path: str = "test_config"
        strategy_name: str = "test_strategy"
        trading_pair: str = TRADING_PAIR
        sql: SQLConnectionManager = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id: Optional[str] = None
        recorder: MarketsRecorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
        recorder.start()

        try:
            self.assertEqual(0, len(self.market.tracking_states))

            current_price: Decimal = Decimal(self.market.get_price(trading_pair, True))
            amount: Decimal = Decimal(0.06) / current_price

            order_id = self.market.buy(trading_pair, amount, OrderType.MARKET)
            [order_created_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))
            order_created_event: BuyOrderCreatedEvent = order_created_event
            self.assertEqual(order_id, order_created_event.order_id)

            # Verify tracking states
            self.assertEqual(1, len(self.market.tracking_states))
            self.assertEqual(order_id, list(self.market.tracking_states.keys())[0])

            # Verify orders from recorder
            recorded_orders: List[Order] = recorder.get_orders_for_config_and_market(config_path, self.market)
            self.assertEqual(1, len(recorded_orders))
            self.assertEqual(order_id, recorded_orders[0].id)

            # Verify saved market states
            saved_market_states: MarketState = recorder.get_market_states(config_path, self.market)
            self.assertIsNotNone(saved_market_states)
            self.assertIsInstance(saved_market_states.saved_state, dict)
            self.assertIsInstance(saved_market_states.saved_state, dict)
            self.assertGreater(len(saved_market_states.saved_state), 0)

            # Close out the current market and start another market.
            self.clock.remove_iterator(self.market)
            for event_tag in self.market_events:
                self.market.remove_listener(event_tag, self.market_logger)
            self.market: MetronomeMarket = MetronomeMarket(
                wallet=self.wallet,
                ethereum_rpc_url=conf.test_web3_provider_list[0],
                order_book_tracker_data_source_type=OrderBookTrackerDataSourceType.EXCHANGE_API,
                trading_pairs=[TRADING_PAIR]
            )
            for event_tag in self.market_events:
                self.market.add_listener(event_tag, self.market_logger)
            recorder.stop()
            recorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
            recorder.start()
            saved_market_states = recorder.get_market_states(config_path, self.market)
            self.clock.add_iterator(self.market)
            self.assertEqual(0, len(self.market.tracking_states))
            self.market.restore_tracking_states(saved_market_states.saved_state)
            self.assertEqual(1, len(self.market.tracking_states))

        finally:
            if order_id is not None:
                self.market.cancel(trading_pair, order_id)
                # self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))

            recorder.stop()
            os.unlink(self.db_path)

    def test_order_fill_record(self):
        config_path: str = "test_config"
        strategy_name: str = "test_strategy"
        trading_pair: str = TRADING_PAIR
        sql: SQLConnectionManager = SQLConnectionManager(SQLConnectionType.TRADE_FILLS, db_path=self.db_path)
        order_id: Optional[str] = None
        recorder: MarketsRecorder = MarketsRecorder(sql, [self.market], config_path, strategy_name)
        recorder.start()

        try:
            # Try to buy 0.16 ETH worth of KNC from the exchange, and watch for completion event.
            current_price: Decimal = Decimal(self.market.get_price(trading_pair, True))
            amount: Decimal = Decimal(0.06) / current_price
            order_id = self.market.buy(trading_pair, amount)
            [buy_order_completed_event] = self.run_parallel(self.market_logger.wait_for(BuyOrderCompletedEvent))

            # Reset the logs
            self.market_logger.clear()

            # Try to sell back the same amount of KNC to the exchange, and watch for completion event.
            amount = Decimal(buy_order_completed_event.base_asset_amount)
            order_id = self.market.sell(trading_pair, amount)
            [sell_order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))

            # Query the persisted trade logs
            trade_fills: List[TradeFill] = recorder.get_trades_for_config(config_path)
            self.assertEqual(2, len(trade_fills))
            buy_fills: List[TradeFill] = [t for t in trade_fills if t.trade_type == "BUY"]
            sell_fills: List[TradeFill] = [t for t in trade_fills if t.trade_type == "SELL"]
            self.assertEqual(1, len(buy_fills))
            self.assertEqual(1, len(sell_fills))

            order_id = None

        finally:
            if order_id is not None:
                self.market.cancel(trading_pair, order_id)

            recorder.stop()
            os.unlink(self.db_path)


def main():
    logging.basicConfig(level=NETWORK)
    unittest.main()


if __name__ == "__main__":
    main()
