import time
import datetime
import traceback
import pandas as pd
from sqlalchemy import create_engine


class PricesDBClient(object):
    def __init__(self, host, username, password):
        '''
        '''
        self.dbpath = 'postgresql://%s:%s@%s/findb' % (username, password, host)
        self.engine = create_engine(self.dbpath)

    def getsymbols(self):
        ''' Get available equity symbols
        '''
        return pd.read_sql('SELECT name FROM tradable;', self.engine).name.tolist()

    def getprices(self, symbol, date):
        ''' Get all prices
        '''
        start = time.time()
        query = '''
            SELECT open, high, low, close, time, date(time), volume
            FROM price
            WHERE request_id IN (
                SELECT id FROM price_request WHERE tradable_id in (
                    select id from tradable where name='%s'
                )
            )
            AND date(time) = '%s';
        ''' % (symbol, date)
        prices = pd.read_sql(query, self.engine).sort_values('time')
        print('Downloaded %s Prices For %s on %s In %.2fs' % (prices.shape[0], symbol, date, time.time() - start))

        # Separate out date and time columns:
        prices.index = prices.time.copy()
        prices['date'] = prices.time.map(lambda x: x.date())
        prices['time'] = prices.time.map(lambda x: x.time())
        return prices

    def download(self, symbols, date):
        '''
        '''
        prices = {}
        for symbol in symbols:
            try:
                values = self.getprices(symbol, date).close
                if not values.empty:
                    prices[symbol] = values
            except:
                print 'Warning: (%s|%s) Could Not Be Fetched:'
                print traceback.format_exc()

        pd.DataFrame(prices).ffill().to_csv('clients/datasets/prices/%s.csv' % date)
