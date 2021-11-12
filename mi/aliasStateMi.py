from datetime import date, datetime, timedelta

from dataclasses import dataclass, fields
from src.alias.aliasState import AliasStateData
from src.utils.dateTimeInfo import DateTimeInfo
from src.utils.rounding import Rounding


@dataclass
class AliasStateMI(AliasStateData):
    """
    AliasStateMI class will update the state of the alias for Momentum strat
    """
    entry_flag: bool = False
    side_to_enter: str = ""
    entry_seconds_before_next_order: int = 2
    entry_minimum_contract_size: int = 1
    entry_period: str = "1m"
    entry_start_time: datetime.time = None
    last_order_sent_timestamp: DateTimeInfo.get_timestamp = DateTimeInfo.get_timestamp()
    start_date_entry: DateTimeInfo.get_date = None
    hedge_period = "1m"
    action: str = ""

    def __post_init__(self):
        super().__post_init__()

        for field in fields(self):
            if field is None:
                print('MISSING JSON ARGS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        ##########
        print(f"ENTRY PERIOD: {self.entry_period}")
        print(f"HEDGE PERIOD: {self.hedge_period}")
        ##########
        self.entry_start_time = datetime.strptime(self.entry_start_time, '%H:%M:%S').time()
        self.entry_start_timestamp = (
                    self.eastern.localize(datetime.combine(DateTimeInfo.get_date(), self.entry_start_time),
                                        is_dst=None)).timestamp()
  
