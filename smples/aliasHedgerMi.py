from dataclasses import dataclass

from datetime import datetime, timedelta

from src.hedgerAlias.aliasHedger_new import AliasHedger
from src.risk.enumPriceType import PriceType
from src.risk.enumSymbolType import SymbolType
from src.utils.dateTimeInfo import DateTimeInfo

@dataclass
class AliasHedgerMI(AliasHedger):
    """
    AliasHedgerMI class is based on the AliasStateData class + all his own function needed
    """
    internal_ids_to_cancel: list = None
    contract_size_to_hedge: int = None
    hedge_seconds_before_next_order: int = None
    hedge_start_time: datetime.time = None
    hedge_minimum_contract_size: int = None
    flag_start_cancel: bool = True
    start_date_hedge: datetime = None
    minimum_hedge_attempt: int = 1
    is_hedging: bool = False
    hedge_price_type: PriceType = PriceType.LIMIT
    hedging_datetime: datetime = None
    hedge_symbol_type: SymbolType = SymbolType.OUTRIGHT
    start_before_settlement_pay_one_tick: timedelta = timedelta(minutes=1)
    pay_tick_flag: bool = False
    max_order_size: int = 30
    action: str = ""

    def __post_init__(self):
        super().__post_init__()



