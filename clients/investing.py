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
        filename = 'files/employment-vs-payroll.csv'
        self.getall().to_csv(filename)

    def getall(self):
        ''' Download Combined Employment & Payroll Dataset From Investing.com
        '''
        # Download Employment Changes Dataset:
        employment = self._employment().dropna().rename(columns={
            'actual': 'emp-actual',
            'forecast': 'emp-forecast',
            'date': 'emp-date',
        })
        # Download Payroll Dataset:
        payrolls = self._payrolls().dropna().rename(columns={
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
        ''' Get Events Dataset From Investing.com Based on Dataset ID
        '''
        # Construct API URL::
        url = '%s/events_charts/us/%s.json' % (self._domain, id)

        # Send API Request:
        print('Sending GET Request to %s...' % url)
        response = requests.get(url, headers=self._headers).json().get('attr')

        # Add Formated columns for date/month from response timestamps:
        dataset = pd.DataFrame(response)
        dataset['date'] = dataset.timestamp.map(lambda ts: datetime.datetime.fromtimestamp(ts / 1000).date())
        dataset['month'] = dataset.date.map(lambda dt: dt.strftime('%Y-%m'))
        dataset.index = dataset['month']

        # Finally, trim unused data and return:
        return dataset[['forecast', 'actual', 'date']]

    def getcurrent(self):
        ''' Get most recent employment
        '''
        # TODO --


if __name__ == '__main__':
    # Initialize data client, and download combined dataset for historic
    # employment and payroll:
    client = InvestingClient()
    data = client.getall()

    # Isolate Employment and Payroll *Differences* Between Forecast & Actual
    # Values, and Drop Any "Outliers" (Here, we just set the cutoff arbitrarily
    # to 500 for th purpose of this exersize, but it might be interesting to
    # look more closely at these uncommon events, where the predictions were so
    # far off from the actual values--for now, I am dropping as they throw off
    # the dataset quite a bit, which makes it difficult to assess the core
    # strategy/hypothesis):
    pairs = data[['emp-diff', 'pay-diff']]
    pairs = pairs[pairs['emp-diff'].abs() < 500].dropna()

    # Print last 10 rows of combined dataframe:
    print('Payroll vs Employment:')
    print(data.tail(10))

    # Print Correlation between missed employment predictions and missed payroll
    # predictions:
    print('Employment Misses -> Payroll Misses Correlations:')
    print(pairs.corr())

    # Plot Scatter:
    plt.scatter(pairs.T.iloc[0], pairs.T.iloc[1])
    plt.show()
