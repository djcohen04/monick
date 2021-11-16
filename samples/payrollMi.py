import time
import pytz
import sched
import logwood
import asyncio
import pandas as pd
from typing import List
from datetime import timedelta, datetime
from src.utils.dateTimeInfo import DateTimeInfo
from src.hedger.hedger_new import Hedger
from src.order.orderStateHandler import OrderState
from src.rabbit.rabbitProducerMQ import RabbitMQProducer
from src.rithmic.marketFeedEnum import MarketFeed
from src.hedgerAlias.aliasHedgerMi import AliasHedgerMI


class EmploymentPayrollTrader(Hedger):
    def set_hedger_alias(self, **kwargs):
        ''' Method for setting product information for the strategy
        '''
        alias_mapper = dict()

        #
        df_info_alias = kwargs.pop('df_info_alias').set_index(["symbol_alias"])
        initialisation_info = kwargs.pop('initialisation_info')

        #
        basic_info = copy(kwargs)
        basic_info['session'] = self.session
        basic_info['producer'] = self.producer

        #
        for symbol_alias in df_info_alias.index:
            self.logger.info(f'symbol_alias: {symbol_alias}')
            basic_info["symbol_alias"] = symbol_alias
            try:
                info = {
                    **basic_info,
                    **df_info_alias.loc[symbol_alias].to_dict(),
                    **initialisation_info.loc[symbol_alias].to_dict()
                }
            except:
                info = {
                    **basic_info,
                    **df_info_alias.loc[symbol_alias].to_dict()
                }

            self.logger.info(f'info: {info}')
            if symbol_alias in initialisation_info.index:
                for ticker in initialisation_info.loc[symbol_alias]["aliases_to_listen"]:
                    initialisation = initialisation_info.loc[symbol_alias].to_dict()
                    keys = [
                        "hedge_seconds_before_next_order", "hedge_start_time",
                        "hedge_minimum_contract_size", "hedge_period", 'round_out_per_side', 'contract_size',
                        'max_not_complete_orders_per_side', 'max_orders_per_price_level',
                        'max_position', "action"
                    ]
                    initialisation_filter = {key: initialisation[key] for key in keys}
                    basic_info["symbol_alias"] = ticker
                    if ticker == symbol_alias:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[symbol_alias] = AliasHedgerMI(**{
                            **basic_info,
                            **df_info_alias.loc[symbol_alias].to_dict(),
                            **initialisation_filter,
                            **more_info
                        })
                    else:
                        more_info = dict(to_listen=True, is_tradable=True)
                        alias_mapper[ticker] = AliasHedgerMI(**{
                            **basic_info,
                            **df_info_alias.loc[ticker].to_dict(),
                            **initialisation_filter,
                            **more_info
                        })

        return alias_mapper

    def update_feeds(self):
        pass


# --------- MARK -----------

    def start_specific_scheduler(self, days: int = 0):
        ''' Do some initial scheduling
        '''
        # Pull the datetime of the next payroll announcement:
        nextpayroll = self.next_payroll_announcement()

        # Let's schedule our trade to start an hour before the announcement --
        # this should be plenty of time to prepare for the trade to occur 15
        # minutes before the announcement:
        startime = nextpayroll - timedelta(hours=1)

        # Schedule our trade to kick off on the target start time:
        self.s.enterabs(time=startime, priority=1, action=self.tradepayroll, kwargs={'nextpayroll': nextpayroll})

    def tradepayroll(self, nextpayroll):
        ''' Do the payroll trade
        '''
        # Get employment forecast/actual data:
        employment = self.getemployment()

        # Determine the signal/strength based on the downloaded employment data:
        signal = self.getsignal(employment)

        # Get the symbols, weights that we want to trade:
        positions = self.gettrades()

        # Schedule a time that we will be MAKING the trades:
        opentime = nextpayroll - timedelta(minutes=15)
        self.s.enterabs(time=opentime, priority=1, action=self.opentrades, kwargs={'positions': positions})

        # Schedule a time that we will be CLOSING the trades:
        closetime = nextpayroll + timedelta(hours=2)
        self.s.enterabs(time=closetime, priority=1, action=self.closetrades, kwargs={'positions': positions})

        # Finally, later in the day, let's make sure to schedule the trade
        # to run again on next month's payroll announcement:
        reschedule = nextpayroll + timedelta(hours=12)
        self.s.enterabs(time=reschedule, priority=1, action=self.reschedule)

    def getemployment(self):
        ''' Get employment forecast/actual data
        '''
        raise NotImplementedError

    def getsignal(self, employment):
        ''' Determine the signal/strength based on the downloaded employment data
        '''
        raise NotImplementedError

    def gettrades(self):
        ''' Get the symbols, weights that we want to trade
        '''
        raise NotImplementedError

    def opentrades(self, positions):
        ''' Open the given positions
        '''
        raise NotImplementedError

    def closetrades(self, positions):
        ''' Close the given positions
        '''
        raise NotImplementedError

    def reschedule(self):
        ''' Reschedule the trade based on next month's payroll announcement
        '''
        raise NotImplementedError

# --------- END MARK -----------

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
