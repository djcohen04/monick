import json
import urllib
import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt

class InvestingClient(object):
    def __init__(self):
        '''
        '''
        self._domain = 'https://sbcharts.investing.com'
        self._headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        }

    def save(self):
        ''' Download & Save Combined Employment & Payroll Dataset to CSV
        '''
        filename = 'datasets/employment-vs-payroll.csv'
        self.getall().to_csv(filename)

    def getall(self):
        ''' Download Combined Employment & Payroll Dataset From Investing.com
        '''
        # Download Employment Changes Dataset:
        employment = self.employment().dropna().drop('time', axis=1).rename(columns={
            'actual': 'emp-actual',
            'forecast': 'emp-forecast',
            'date': 'emp-date',
        })
        # Download Payroll Dataset:
        payrolls = self.payrolls().dropna().drop('time', axis=1).rename(columns={
            'actual': 'pay-actual',
            'forecast': 'pay-forecast',
            'date': 'pay-date',
        })

        # Add in differences between forecasted value & actual value:
        payrolls['pay-diff'] = payrolls['pay-actual'] - payrolls['pay-forecast']
        employment['emp-diff'] = employment['emp-actual'] - employment['emp-forecast']

        # Join Datasets:
        dataset = pd.DataFrame.join(employment, payrolls)

        return dataset.dropna()

    def employment(self):
        ''' Download ADP Nonfarm Employment Changes
        '''
        return self._download(1)

    def payrolls(self):
        ''' Download Nonfarm Payrolls
        '''
        return self._download(227)

    def _download(self, id):
        ''' Download Events Dataset From Investing.com Based on Dataset ID
        '''
        # Construct API URL::
        url = '%s/events_charts/us/%s.json' % (self._domain, id)

        # Send API Request:
        print('Sending GET Request to %s...' % url)
        response = requests.get(url, headers=self._headers).json().get('attr')

        # Add Formated columns for date/month from response timestamps:
        dataset = pd.DataFrame(response)
        dataset['date'] = dataset.timestamp.map(lambda ts: datetime.datetime.fromtimestamp(ts / 1000).date())
        dataset['time'] = dataset.timestamp.map(lambda ts: datetime.datetime.fromtimestamp(ts / 1000).time())
        dataset['month'] = dataset.date.map(lambda dt: dt.strftime('%Y-%m'))
        dataset.index = dataset['month']

        # Convert thousands back to regular units:
        dataset['forecast'] *= 1000
        dataset['actual'] *= 1000

        # Finally, trim unused data and return:
        return dataset[['forecast', 'actual', 'date', 'time']]

    def getcurrent(self):
        ''' Get most recent employment
        '''
        # TODO --


if __name__ == '__main__':
    # Initialize data client, and download combined dataset for historic
    # employment and payroll:
    client = InvestingClient()
    data = client.getall()
