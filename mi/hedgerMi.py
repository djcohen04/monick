import time

import asyncio
from datetime import timedelta, datetime

import pytz
from typing import List

import sched

from src.utils.dateTimeInfo import DateTimeInfo

from src.hedger.hedger_new import Hedger
from src.order.orderStateHandler import OrderState
from src.rabbit.rabbitProducerMQ import RabbitMQProducer
from src.rithmic.marketFeedEnum import MarketFeed
from src.hedgerAlias.aliasHedgerMi import AliasHedgerMI

import logwood
import pandas as pd

# Class that represents the new hedger strategy
class HedgerManualIntervention(Hedger):

    """
    ManualIntervention is a class based on the Hedger abstract class.
    It can enter a trade before a trade has been placed and the hedger can be intervened manually.

    Parameters:

    logger, ==> logging object from logwood
    session, ==> unique id for the trading session
    producer, ==> rabbitmq producer to send orders
    fcm_id, ==> info to use to connect to our provider Rithmic api
    ib_id, ==> info to use to connect to our provider Rithmic api
    locale,  ==> way to reference a strategy ==> combination of locale, city, street, tag
    city, ==> way to reference a strategy ==> combination of locale, city, street, tag
    street_name, ==>way to reference a strategy ==> combination of locale, city, street, tag
    tag_name, ==> way to reference a strategy ==> combination of locale, city, street, tag
    account_id, ==> account
    mails, ==> list mail
    level, ==> PAPER or PROD
    initialisation_info, ==> basic information about strategy settings from mongo. Only at initialisation
    alias, ==> alias class to have all information about alias. Alias are for example ES1
    order, ==> order class to have all our orders and related function
    queue_name, queue of message in rabbitmq
    channel, channel in rabbitmq
    df_info_alias ==> basic start dataframe information the aliases. Only at initialisation
    """

    # Initializer
    def __init__(
        self, 
        logger: logwood.Logger, 
        session: str, 
        producer: RabbitMQProducer, 
        locale: str, 
        fcm_id: str, 
        ib_id: str,
        account_id, 
        city: str,
        street_name: str,
        tag_name: str, 
        mails: List[str], 
        level, 
        initialisation_info: pd.DataFrame, 
        alias, 
        order: OrderState, 
        queue_name: str,
        channel, # Connection with RabbitMQ
        df_info_alias: pd.DataFrame,
        ):

        super().__init__(logger, session, producer, locale, fcm_id, ib_id, account_id, city, street_name,
                         tag_name, mails, level, initialisation_info, alias, order, queue_name,
                         channel, df_info_alias)
        
        self.start_hedge_date = None
        self.tz = pytz.timezone("US/Eastern")

    # Method for setting product information for the strategy
    def set_hedger_alias(self, logger, initialisation_info, df_info_alias, locale, fcm_id, ib_id, account_id, city,
                         street_name, tag_name, mails, level):
        alias_mapper = dict()
        basic_info = dict(logger=logger, locale=locale, fcm_id=fcm_id, ib_id=ib_id, tag_name=tag_name,
                          account_id=account_id, city=city, street_name=street_name,
                          mails=mails, level=level, session=self.session, producer=self.producer)
        df_info_alias = df_info_alias.set_index(["symbol_alias"])
        for symbol_alias in df_info_alias.index:
            self.logger.info(f'symbol_alias: {symbol_alias}')
            basic_info["symbol_alias"] = symbol_alias
            try: 
                info = {**basic_info, **df_info_alias.loc[symbol_alias].to_dict(),
                        **initialisation_info.loc[symbol_alias].to_dict()}
            except:
                info = {**basic_info, **df_info_alias.loc[symbol_alias].to_dict()}
            self.logger.info(f'info: {info}')
            if symbol_alias in initialisation_info.index:
                for ticker in initialisation_info.loc[symbol_alias]["aliases_to_listen"]:
                    initialisation = initialisation_info.loc[symbol_alias].to_dict()
                    keys = ["hedge_seconds_before_next_order", "hedge_start_time",
                            "hedge_minimum_contract_size", "hedge_period", 'round_out_per_side', 'contract_size',
                            'max_not_complete_orders_per_side', 'max_orders_per_price_level',
                            'max_position', "action"]
                    initialisation_filter = {key: initialisation[key] for key in keys}
                    basic_info["symbol_alias"] = ticker
                    if ticker == symbol_alias:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[symbol_alias] = AliasHedgerMI(
                            **{**basic_info, **df_info_alias.loc[symbol_alias].to_dict(),
                               **initialisation_filter,
                               **more_info})
                    else:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[ticker] = AliasHedgerMI(**{**basic_info, **df_info_alias.loc[ticker].to_dict(),
                                                              **initialisation_filter,
                                                              **more_info})
        return alias_mapper

    def update_feeds(self):
        pass

    def start_specific_scheduler(self, days: int = 0):
        self.logger.info(f'.. start hedger scheduler')
        for symbol_alias, alias in self.hedger_alias.items():
            if alias.is_tradable:
                if alias.action == 'hedger':
                    for delay in range(alias.minimum_hedge_attempt):
                        task_time_hedging = alias.eastern.localize(
                            datetime.combine(DateTimeInfo.get_date() + timedelta(days=days), alias.hedge_start_time),
                            is_dst=None).timestamp() + delay * alias.hedge_seconds_before_next_order
                        if delay == 0:
                            alias.hedging_datetime = task_time_hedging
                            self.s.enterabs(time=task_time_hedging, priority=1, action=self.go_hedging,
                                            kwargs={"alias": alias})
                            self.logger.info(f'.. add new schedule hedging task FLAG for {task_time_hedging}')
                        self.s.enterabs(time=task_time_hedging, priority=1, action=self.hedger_logic,
                                        kwargs={"symbol_alias": symbol_alias})
                        self.logger.info(f'.. add new schedule hedging task for {task_time_hedging}')
                    task_time_hedging = (alias.eastern.localize(
                        datetime.combine(DateTimeInfo.get_date() + timedelta(days=days), alias.hedge_start_time),
                        is_dst=None) + alias.hedge_period).timestamp()
                    self.logger.info(f'.. add last schedule hedging task for {task_time_hedging} with Market Order')
                    self.s.enterabs(time=task_time_hedging, priority=1, action=self.hedger_logic,
                                    kwargs={"symbol_alias": symbol_alias})
                    task_time_hedging_final_check = (alias.eastern.localize(
                        datetime.combine(DateTimeInfo.get_date() + timedelta(days=days), alias.hedge_start_time),
                        is_dst=None) + alias.hedge_period).timestamp() + 10
                    self.logger.info(
                        f'.. add a schedule 10 sec after the market order was sent at {task_time_hedging_final_check}')
                    self.s.enterabs(time=task_time_hedging_final_check, priority=1, action=self.hedger_logic,
                                    kwargs={"symbol_alias": symbol_alias})
                    task_time_hedging_alert = (alias.eastern.localize(
                        datetime.combine(DateTimeInfo.get_date() + timedelta(days=days), alias.hedge_start_time),
                        is_dst=None) + alias.hedge_period).timestamp() + 60
                    self.logger.info(f'.. add last schedule email alert task for {task_time_hedging_alert}. HEDGING FAILED')
                    self.s.enterabs(time=task_time_hedging_alert, priority=1, action=self.send_failed_email_alert)

    async def start_main_scheduler(self):
        await super(HedgerManualIntervention, self).start_main_scheduler()

    def on_order_update(self, data):
        super(HedgerManualIntervention, self).on_order_update(data=data)

    def on_market_mode_update(self, data: dict, channel):
        super(HedgerManualIntervention, self).on_market_mode_update(data=data, channel=channel)

    def on_trade_route_update(self, data):
        super(HedgerManualIntervention, self).on_trade_route_update(data=data)

    def on_market_update(self, data: dict, routing: str):
        symbol_alias = data['symbol_alias']
        self.market_feed_func_mapper.get(MarketFeed(routing))(symbol_alias=symbol_alias, data=data)
        self.hedger_logic(symbol_alias=symbol_alias)
        if self.alias.get(symbol_alias).net_position != 0:
            self.alias.get(symbol_alias).update_open_pnl(symbol=data["symbol"])

    def hedger_specific_risk_check(self, data):
        super(HedgerManualIntervention, self).hedger_specific_risk_check(data=data)

    def hedger_logic(self, symbol_alias: str):
        super(HedgerManualIntervention, self).hedger_logic(symbol_alias=symbol_alias)

    def order_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def order_rcvd_from_client(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_rcvd_from_client(internal_id=internal_id, data=data)

    def order_pending(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_pending(internal_id=internal_id, data=data)

    def order_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def order_sent_to_exch(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_sent_to_exch(internal_id=internal_id, data=data)

    def order_status(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_status(internal_id=internal_id, data=data)

    def order_open(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_open(internal_id=internal_id, data=data)

    def order_fill(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_fill(internal_id=internal_id, data=data)

    def modify_fill(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_fill(internal_id=internal_id, data=data)

    def order_complete(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_complete(internal_id=internal_id, data=data)
        self.order.drop_order(internal_id=internal_id)

    def order_reject(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_reject(internal_id, data=data)

    def order_trigger_pending(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_trigger_pending(internal_id=internal_id, data=data)

    def order_trigger(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_trigger(internal_id=internal_id, data=data)

    def order_link_orders_failed(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_link_orders_failed(internal_id=internal_id, data=data)

    def order_generic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).order_generic(internal_id=internal_id, data=data)

    def modify_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def modify_rcvd_from_client(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_rcvd_from_client(internal_id=internal_id, data=data)

    def modify_pending(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_pending(internal_id=internal_id, data=data)

    def modify_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def modify_sent_to_exch(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_sent_to_exch(internal_id=internal_id, data=data)

    def modify_modify(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_modify(internal_id=internal_id, data=data)

    def modify_status(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_status(internal_id=internal_id, data=data)

    def modify_open(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_open(internal_id=internal_id, data=data)

    def modify_not_modified(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_not_modified(internal_id=internal_id, data=data)

    def modify_modified(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_modified(internal_id=internal_id, data=data)

    def modify_generic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_generic(internal_id=internal_id, data=data)

    def modify_modification_failed(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_modification_failed(internal_id=internal_id, data=data)

    def modify_reject(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_reject(internal_id=internal_id, data=data)

    def modify_trigger(self, internal_id: str, data: dict):
        super(HedgerManualIntervention).modify_trigger(internal_id=internal_id, data=data)

    def modify_complete(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).modify_complete(internal_id=internal_id, data=data)
        self.order.drop_order(internal_id=internal_id)

    def cancel_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def cancel_cancellation_failed(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_cancellation_failed(internal_id=internal_id, data=data)

    def cancel_not_cancelled(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_not_cancelled(internal_id=internal_id, data=data)

    def cancel_generic(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_generic(internal_id=internal_id, data=data)

    def cancel_reject(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_reject(internal_id=internal_id, data=data)

    def cancel_cancel(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_cancel(internal_id=internal_id, data=data)

    def cancel_sent_to_exch(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_sent_to_exch(internal_id=internal_id, data=data)

    def cancel_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def cancel_pending(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_pending(internal_id=internal_id, data=data)

    def cancel_rcvd_from_client(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_rcvd_from_client(internal_id=internal_id, data=data)

    def cancel_complete(self, internal_id: str, data: dict):
        super(HedgerManualIntervention, self).cancel_complete(internal_id=internal_id, data=data)

    def on_end_of_day_update(self, data: dict):
        super(HedgerManualIntervention, self).on_end_of_day_update(data=data)
















