import json
import urllib
import requests
import datetime
import pandas as pd


class InvestingClient(object):
    def __init__(self):
        '''
        '''
        self._domain = 'https://sbcharts.investing.com'
        self._headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        }

    def getall(self):
        ''' Get Employment & Payroll Data From Investing.com
        '''
        # Download Employment Changes Dataset:
        employment = self._employment().rename(columns={
            'actual': 'emp-actual',
            'forecast': 'emp-forecast',
            'date': 'emp-date',
        })
        # Download Payroll Dataset:
        payrolls = self._payrolls().rename(columns={
            'actual': 'pay-actual',
            'forecast': 'pay-forecast',
            'date': 'pay-date',
        })

        # Join Datasets:
        dataset = pd.DataFrame.join(employment, payrolls)

        # Add in actual/forecast differences:
        dataset['emp-diff'] = dataset['emp-actual'] - dataset['emp-forecast']
        dataset['pay-diff'] = dataset['pay-actual'] - dataset['pay-forecast']
        return dataset.dropna()

    def _employment(self):
        ''' Download ADP Nonfarm Employment Changes
        '''
        return self._getjson(1)

    def _payrolls(self):
        ''' Download Nonfarm Payrolls
        '''
        return self._getjson(227)

    def _getjson(self, id):
        ''' GET Events Dataset From Investing.com
        '''
        # Send GET Request to Investing.com:
        url = '%s/events_charts/us/%s.json' % (self._domain, id)
        print('Sending GET Request to %s...' % url)
        response = requests.get(url, headers=self._headers).json().get('attr')

        # Format dates/timestamps, trim unused data:
        dataset = pd.DataFrame(response)
        dataset['date'] = dataset.timestamp.map(lambda ts: datetime.datetime.fromtimestamp(ts / 1000).date())
        dataset['month'] = dataset.date.map(lambda dt: dt.strftime('%Y-%m'))
        dataset.index = dataset['month']
        return dataset[['forecast', 'actual', 'date']].dropna()



if __name__ == '__main__':
    client = InvestingClient()
    data = client.getall()
    print('Payroll vs Employment:')
    print(data.tail(10))
    print('Employment Misses -> Payroll Misses Correlations:')
    print(data[['emp-diff', 'pay-diff']].corr())
