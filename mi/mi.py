import asyncio
import sched
import time
from abc import ABC
from src.alias.aliasStateMi import AliasStateMI
from src.hedger.hedgerMi import HedgerManualIntervention
from src.strategy.strategy_new import Strategy
from src.utils.dateTimeInfo import DateTimeInfo
from src.utils.rounding import Rounding


class MI(Strategy, ABC):
    """
        Momentum class based on the new strategy to handle Momentum strategy
    """
    def __init__(
        self, 
        logger, 
        producer, 
        consumer_order, 
        df_info_alias, 
        locale, 
        fcm_id, 
        ib_id, 
        account_id, 
        city, 
        street_name,
        tag_name, 
        mails, 
        level, 
        initialisation_info,
        ):
        super(MI, self).__init__(logger=logger, producer=producer, consumer_order=consumer_order,
                                    df_info_alias=df_info_alias, locale=locale, fcm_id=fcm_id, ib_id=ib_id,
                                    account_id=account_id, city=city, street_name=street_name, tag_name=tag_name,
                                    mails=mails, level=level, initialisation_info=initialisation_info)
        self.hedge = HedgerManualIntervention(logger=logger, session=self.session, producer=producer, locale=locale,
                                 fcm_id=fcm_id, ib_id=ib_id, account_id=account_id, city=city,
                                 street_name=street_name, tag_name=tag_name, mails=mails, level=level,
                                 initialisation_info=initialisation_info, alias=self.alias, order=self.order,
                                 queue_name=self.producer.queue_name, channel=self.producer.channel,
                                 df_info_alias=df_info_alias)

    def start_specific_scheduler(self):
        self.logger.info(f'.. start scheduler')

        for symbol_alias, alias in self.alias.items():


            ###########
            print(f"SYMBOL ALIAS: {symbol_alias}")
            print(f"ALIAS: {alias}")
            print(f"ALIAS START TIME: {alias.entry_start_time}")
            print(f"ALIAS START TIME TYPE: {type(alias.entry_start_time)}")
            if alias.action == 'enter':
                self.s.enterabs(time=alias.entry_start_timestamp, priority=1, action=self.go_entry,
                           kwargs={"alias": alias})
                        
            elif alias.action == "hedge":
                self.s.enterabs(time =alias.entry_start_timestamp, priority = 1, action = self.go_hedging)

    async def start_main_scheduler(self):
        await super(MI, self).start_main_scheduler()

    def go_entry(self, alias: AliasStateMI):
        alias.set_entry_flag()
        self.logger.info(f"Set entry flag to true for {alias.symbol_alias}.")

    def update_feeds(self):
        super().HedgerManualIntervention.update_feeds()

    def show_task_scheduled(self, task):
        self.logger.info(f'{task}')

    def go_hedging(self):
        for symbol_alias, alias in self.alias.items():
            if alias.is_tradable:
                self.logger.info(f'.. go to hedging {symbol_alias}')
                alias.is_hedging = True

    def update_binding_keys_for_hedging(self, exchange_binding_keys_list: list, flag: bool):
        if flag:
            for exchange, binding_key in exchange_binding_keys_list:
                self.producer.channel.queue_bind(
                    exchange=exchange, queue=self.producer.queue_name, routing_key=binding_key
                )
        else:
            for exchange, binding_key in exchange_binding_keys_list:
                self.producer.channel.queue_unbind(
                    exchange=exchange, queue=self.producer.queue_name, routing_key=binding_key
                )

    def update_order_binding_keys_after_start(self, exchange_binding_keys_list: list, flag: bool):
        if flag:
            for exchange, binding_key in exchange_binding_keys_list:
                self.consumer_order.channel.queue_bind(
                    exchange=exchange, queue=self.consumer_order.queue_name, routing_key=binding_key
                )
        else:
            for exchange, binding_key in exchange_binding_keys_list:
                self.consumer_order.channel.queue_unbind(
                    exchange=exchange, queue=self.consumer_order.queue_name, routing_key=binding_key
                )

    def set_alias(self, logger, initialisation_info, df_info_alias, locale, fcm_id, ib_id, account_id, city,
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
                info = {**basic_info, **df_info_alias.loc[symbol_alias].to_dict(),**initialisation_info.loc[symbol_alias].to_dict()}
            except:
                info = {**basic_info, **df_info_alias.loc[symbol_alias].to_dict()}
            self.logger.info(f'info: {info}')
            if symbol_alias in initialisation_info.index:
                for ticker in initialisation_info.loc[symbol_alias]["aliases_to_listen"]:
                    initialisation = initialisation_info.loc[symbol_alias].to_dict()
                    basic_info["symbol_alias"] = ticker
                    keys_to_remove = ["aliases_to_listen",
                                      "hedge_seconds_before_next_order", "hedge_start_time",
                                      "hedge_minimum_contract_size", "hedge_period"]
                    for key in keys_to_remove:
                        initialisation.pop(key)
                    if ticker == symbol_alias:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[symbol_alias] = AliasStateMI(**{**basic_info, **df_info_alias.loc[symbol_alias].to_dict(),
                                                                            **initialisation,
                                                                            **more_info})
                    else:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[ticker] = AliasStateMI(**{**basic_info, **df_info_alias.loc[ticker].to_dict(),
                                                                         **initialisation,
                                                                         **more_info})
        return alias_mapper

    async def start_data_feed(self):
        await super(MI, self).start_data_feed()

    async def start_data_feed_order(self):
        await super(MI, self).start_data_feed_order()

    def update(self, ch, method, properties, body):
        super(MI, self).update(ch=ch, method=method, properties=properties, body=body)

    def update_order(self, ch, method, properties, body):
        super(MI, self).update_order(ch=ch, method=method, properties=properties, body=body)

    def on_order_update(self, data):
        super(MI, self).on_order_update(data=data)

    def update_pricer(self, symbol_alias):
        if self.alias.get(symbol_alias).entry_flag and not self.alias.get(symbol_alias).is_hedging:
            self.alias.get(symbol_alias).update_contract_size_to_enter_per_seconds()
            if self.alias.get(symbol_alias).contract_size_to_enter > 0:
                self.enter_position(symbol_alias=symbol_alias)
                self.modify_orders_to_enter(symbol_alias=symbol_alias)

    def on_market_mode_update(self, data: dict, channel):
        super(MI, self).on_market_mode_update(data=data, channel=channel)

    def on_trade_route_update(self, data):
        super(MI, self).on_trade_route_update(data=data)

    def on_market_update(self, data: dict, routing: str):
        super(MI, self).on_market_update(data=data, routing=routing)
        self.update_pricer(symbol_alias=data['symbol_alias'])

    def strategy_specific_risk_check(self, data):
        super(MI, self).strategy_specific_risk_check(data=data)

    def order_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(MI, self).order_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def order_rcvd_from_client(self, internal_id: str, data: dict):
        super(MI, self).order_rcvd_from_client(internal_id=internal_id, data=data)

    def order_pending(self, internal_id: str, data: dict):
        super(MI, self).order_pending(internal_id=internal_id, data=data)

    def order_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(MI, self).order_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def order_sent_to_exch(self, internal_id: str, data: dict):
        super(MI, self).order_sent_to_exch(internal_id=internal_id, data=data)

    def order_status(self, internal_id: str, data: dict):
        super(MI, self).order_status(internal_id=internal_id, data=data)

    def order_open(self, internal_id: str, data: dict):
        super(MI, self).order_open(internal_id=internal_id, data=data)

    def order_fill(self, internal_id: str, data: dict):
        super(MI, self).order_fill(internal_id=internal_id, data=data)
        self.alias.get(data["symbol_alias"]).position_left_to_enter()

    def modify_fill(self, internal_id: str, data: dict):
        super(MI, self).modify_fill(internal_id=internal_id, data=data)
        self.alias.get(data["symbol_alias"]).position_left_to_enter()

    def order_new_orders_failed(self, internal_id: str, data: dict):
        super(MI, self).order_new_orders_failed(internal_id=internal_id, data=data)

    def order_complete(self, internal_id: str, data: dict):
        super(MI, self).order_complete(internal_id=internal_id, data=data)
        self.order.drop_order(internal_id=internal_id)

    def order_reject(self, internal_id: str, data: dict):
        super(MI, self).order_reject(internal_id=internal_id, data=data)

    def order_trigger_pending(self, internal_id: str, data: dict):
        super(MI, self).order_trigger_pending(internal_id=internal_id, data=data)

    def order_trigger(self, internal_id: str, data: dict):
        super(MI, self).order_trigger(internal_id=internal_id, data=data)

    def order_link_orders_failed(self, internal_id: str, data: dict):
        super(MI, self).order_link_orders_failed(internal_id=internal_id, data=data)

    def order_generic(self, internal_id: str, data: dict):
        super(MI, self).order_generic(internal_id=internal_id, data=data)

    def modify_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(MI, self).modify_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def modify_rcvd_from_client(self, internal_id: str, data: dict):
        super(MI, self).modify_rcvd_from_client(internal_id=internal_id, data=data)

    def modify_pending(self, internal_id: str, data: dict):
        super(MI, self).modify_pending(internal_id=internal_id, data=data)

    def modify_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(MI, self).modify_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def modify_sent_to_exch(self, internal_id: str, data: dict):
        super(MI, self).modify_sent_to_exch(internal_id=internal_id, data=data)

    def modify_modify(self, internal_id: str, data: dict):
        super(MI, self).modify_modify(internal_id=internal_id, data=data)

    def modify_status(self, internal_id: str, data: dict):
        super(MI, self).modify_status(internal_id=internal_id, data=data)

    def modify_open(self, internal_id: str, data: dict):
        super(MI, self).modify_open(internal_id=internal_id, data=data)

    def modify_not_modified(self, internal_id: str, data: dict):
        super(MI, self).modify_not_modified(internal_id=internal_id, data=data)

    def modify_modified(self, internal_id: str, data: dict):
        super(MI, self).modify_modified(internal_id=internal_id, data=data)

    def modify_generic(self, internal_id: str, data: dict):
        super(MI, self).modify_generic(internal_id=internal_id, data=data)

    def modify_modification_failed(self, internal_id: str, data: dict):
        super(MI, self).modify_modification_failed(internal_id=internal_id, data=data)

    def modify_reject(self, internal_id: str, data: dict):
        super(MI, self).modify_reject(internal_id=internal_id, data=data)

    def modify_trigger(self, internal_id: str, data: dict):
        super(MI, self).modify_trigger(internal_id=internal_id, data=data)

    def modify_complete(self, internal_id: str, data: dict):
        super(MI, self).modify_complete(internal_id=internal_id, data=data)
        self.order.drop_order(internal_id=internal_id)

    def cancel_rcvd_by_rithmic(self, internal_id: str, data: dict):
        super(MI, self).cancel_rcvd_by_rithmic(internal_id=internal_id, data=data)

    def cancel_cancellation_failed(self, internal_id: str, data: dict):
        super(MI, self).cancel_cancellation_failed(internal_id=internal_id, data=data)

    def cancel_not_cancelled(self, internal_id: str, data: dict):
        super(MI, self).cancel_not_cancelled(internal_id=internal_id, data=data)

    def cancel_generic(self, internal_id: str, data: dict):
        super(MI, self).cancel_generic(internal_id=internal_id, data=data)

    def cancel_reject(self, internal_id: str, data: dict):
        super(MI, self).cancel_reject(internal_id=internal_id, data=data)

    def cancel_cancel(self, internal_id: str, data: dict):
        super(MI, self).cancel_cancel(internal_id=internal_id, data=data)

    def cancel_sent_to_exch(self, internal_id: str, data: dict):
        super(MI, self).cancel_sent_to_exch(internal_id=internal_id, data=data)

    def cancel_rcvd_by_exch_gtwy(self, internal_id: str, data: dict):
        super(MI, self).cancel_rcvd_by_exch_gtwy(internal_id=internal_id, data=data)

    def cancel_pending(self, internal_id: str, data: dict):
        super(MI, self).cancel_pending(internal_id=internal_id, data=data)

    def cancel_rcvd_from_client(self, internal_id: str, data: dict):
        super(MI, self).cancel_rcvd_from_client(internal_id=internal_id, data=data)

    def cancel_complete(self, internal_id: str, data: dict):
        super(MI, self).cancel_complete(internal_id=internal_id, data=data)

    def on_end_of_day_update(self, data: dict):
        super(MI, self).on_end_of_day_update(data=data)

