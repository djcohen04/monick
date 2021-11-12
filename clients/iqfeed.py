import os
import datetime
import pandas as pd

class IQFeedClient(object):
    def __init__(self):
        '''
        '''
        self.username = os.environ.get('IQFEED_USERNAME', '479984')
        self._password = os.environ.get('IQFEED_PASSWORD', '...')
        self.domain = '...'
        self.symbols = self._loadsymbols()

    def _loadsymbols(self):
        '''
        '''
        return pd.read_csv('clients/datasets/iqfeed_symbols.csv').SymbolIQFeed



if __name__ == '__main__':
    pass
