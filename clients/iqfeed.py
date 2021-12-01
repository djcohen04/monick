import os
import sys
import socket
import datetime
import pandas as pd
from StringIO import StringIO

class IQFeedClient(object):
    def __init__(self, host='localhost', port=9100, buffersize=4096):
        '''
        '''
        self.host = host
        self.port = port
        self.buffersize = buffersize

    @classmethod
    def symbols(cls):
        ''' IQFeed top 63 futures
        '''
        return [
            '@AD#', '@BP#', '@CD#', '@EU#', '@JY#', '@SF#', 'EB#', '@BTC#',
            'QBZ#', 'QC#', '@CC#', 'QCL#', '@CT#', '@ES#', '@ETH#', 'XD#',
            'EX#', 'BD#', 'BL#', 'EZ#', 'BX#', 'OAT#', 'FVS#', 'GAS#', 'QGC#',
            '@GF#', '@HE#', 'QHG#', 'QHO#', '@KC#', '@KW#', '@LE#', '@MFS#',
            '@MME#', 'QNG#', '@NIY#', '@NKD#', '@NQ#', 'QPL#', 'QRB#', 'LRC#',
            '@RS#', '@RTY#', '@SB#', 'QSI#', '@TN#', '@UB#', 'IHO#', 'IRB#',
            '@VX#', 'QW#', 'CRD#', '@YM#', '@3N#', '@US#', '@C#', '@FV#',
            '@BO#', '@SM#', '@TY#', '@S#', '@TU#', '@W#'
        ]

    @classmethod
    def payrolldates(cls):
        ''' Payroll Announcement Dates
        '''
        dates = ['2008-03-07', '2008-04-04', '2008-04-04', '2008-06-06', '2008-07-03', '2008-07-03', '2008-09-05', '2008-10-03', '2008-11-07', '2008-12-05', '2009-01-09', '2009-02-06', '2009-03-06', '2009-04-03', '2009-05-08', '2009-06-05', '2009-07-02', '2009-08-07', '2009-09-04', '2009-09-04', '2009-11-06', '2009-12-04', '2010-01-08', '2010-02-05', '2010-03-05', '2010-03-05', '2010-05-07', '2010-06-04', '2010-06-04', '2010-08-06', '2010-09-03', '2010-10-08', '2010-11-05', '2010-12-03', '2011-01-07', '2011-02-04', '2011-03-04', '2011-03-04', '2011-05-06', '2011-06-03', '2011-07-08', '2011-08-05', '2011-08-05', '2011-10-07', '2011-11-04', '2011-11-04', '2012-01-06', '2012-02-03', '2012-03-09', '2012-04-06', '2012-05-04', '2012-05-04', '2012-07-06', '2012-08-03', '2012-09-07', '2012-10-05', '2012-11-02', '2012-12-07', '2013-01-04', '2013-01-04', '2013-03-08', '2013-04-05', '2013-05-03', '2013-06-07', '2013-07-05', '2013-07-05', '2013-09-06', '2013-10-22', '2013-10-22', '2013-12-06', '2014-01-10', '2014-02-07', '2014-03-07', '2014-04-04', '2014-04-04', '2014-06-06', '2014-07-03', '2014-07-03', '2014-09-05', '2014-10-03', '2014-11-07', '2014-12-05', '2015-01-09', '2015-02-06', '2015-03-06', '2015-04-03', '2015-05-08', '2015-06-05', '2015-07-02', '2015-08-07', '2015-09-04', '2015-09-04', '2015-11-06', '2015-12-04', '2016-01-08', '2016-02-05', '2016-03-04', '2016-03-04', '2016-05-06', '2016-06-03', '2016-07-08', '2016-08-05', '2016-08-05', '2016-10-07', '2016-11-04', '2016-11-04', '2017-01-06', '2017-02-03', '2017-03-10', '2017-04-07', '2017-05-05', '2017-06-02', '2017-07-07', '2017-08-04', '2017-08-04', '2017-10-06', '2017-11-03', '2017-12-08', '2018-01-05', '2018-01-05', '2018-03-09', '2018-04-06', '2018-05-04', '2018-05-04', '2018-07-06', '2018-08-03', '2018-09-07', '2018-10-05', '2018-10-05', '2018-12-07', '2019-01-04', '2019-01-04', '2019-03-08', '2019-04-05', '2019-05-03', '2019-06-07', '2019-07-05', '2019-07-05', '2019-09-06', '2019-10-04', '2019-10-04', '2019-12-06', '2020-01-10', '2020-02-07', '2020-03-06', '2020-04-03', '2020-05-08', '2020-06-05', '2020-07-02', '2020-08-07', '2020-09-04', '2020-09-04', '2020-11-06', '2020-12-04', '2021-01-08', '2021-02-05', '2021-03-05', '2021-03-05', '2021-05-07', '2021-06-04', '2021-06-04', '2021-08-06', '2021-09-03', '2021-10-08', '2021-11-05']

        return [datetime.datetime.strptime(date, '%Y-%m-%d') for date in dates]


    def _doquery(self, query):
        ''' Do IQFeed Query
        '''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        try:
            # Send data request:
            #print 'Connected to %s:%s, Requesting Data...' % (host, port)
            #print 'IQFeed QUERY: %s' % query
            sock.sendall(query)


            # Read in repsonse data from TCP Socket:
            data = 'high,low,open,close,vol,periodvol,_\n'
            while True:
                data += sock.recv(self.buffersize).decode()
                if '!ENDMSG!' in data:
                    break

            # Drop 'ENDMSG' termination string:
            data = data[:-12]

        finally:
            # Close the TCP Socket:
            sock.close()


        if '!NO_DATA!' in data:
            # No data returned for our query:
            raise Exception('Data Unavailable for IQFeed Query: %s' % query)
        else:
            # Parse data into dataframe:
            iodata = StringIO(data)
            return pd.read_csv(iodata, parse_dates=True).drop('_', axis=1)


    def fetch(self, symbol, start, end, seconds=60):
        '''
        '''
        query = "HIT,%s,%s,%s 000000,%s 235959,,000000,235959,1\n" % (
            symbol,
            seconds,
            start.strftime('%Y%m%d'),
            end.strftime('%Y%m%d'),
        )
        return self._doquery(query)

    def getdate(self, symbols, date):
        ''' Get minute-by-minute data for multiple symbols for a single date
        '''
        print 'Fetching Data for %s: %s...' % (date, symbols)
        returns = {}
        for symbol in symbols:
            try:
                returns[symbol] = client.fetch(symbol, date, date).close
            except:
                print '  WARNING: Data unavailable for %s on %s, skipping...' % (symbol, date)

        return pd.DataFrame(returns).ffill().bfill()

    def makefolder(self, relpath):
        '''
        '''
        fullpath = os.getcwd() + relpath
        if not os.path.isdir(fullpath):
            try:
                os.mkdir(fullpath)
            except OSError as e:
                print 'Creation of the directory %s failed: %s' % (fullpath, e)
            else:
                print 'Created Directory: %s' % fullpath
        else:
            print 'Directory Already Exists: %s (Skipping)' % fullpath


if __name__ == "__main__":
    # Initialize Client:
    host = 'localhost'
    port = 9100
    client = IQFeedClient(host, port)

    # Define symbols/dates to iterate over:
    symbols = IQFeedClient.symbols()
    dates = IQFeedClient.payrolldates()

    # Aggregate data for all symbols for each date:
    for date in dates:
        client.getdate(symbols, date).to_csv('prices/%s.csv' % date.date())
