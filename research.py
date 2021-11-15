import datetime
import traceback
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from clients.investing import InvestingClient
from clients.colors import ColorClient

class AnnouncementResearch(object):
    def __init__(self, cutoff=500000):
        ''' Research & Plot Ecomonic Forecast Correlations
        '''
        # Ingore any data points where the employment forecast was off by a net
        # cutoff value -- this is put in place to drop outliers (though this
        # could be an interesting thing to look futher into with a bit more time):
        self.cutoff = cutoff

        # Initialize data client, and download combined dataset for historic
        # employment and payroll:
        self.client = InvestingClient()
        self.data = self.client.getall()

        # Isolate Employment and Payroll *Differences* Between Forecast & Actual
        # Values, and Drop Any "Outliers" (Here, we just set the cutoff arbitrarily
        # to 500 for th purpose of this exersize, but it might be interesting to
        # look more closely at these uncommon events, where the predictions were so
        # far off from the actual values--for now, I am dropping as they throw off
        # the dataset quite a bit, which makes it difficult to assess the core
        # strategy/hypothesis):
        pairs = self.data[['emp-diff', 'pay-diff']]
        self.pairs = pairs[pairs['emp-diff'].abs() < self.cutoff].dropna()

        # Separate Employment and Payroll histories:
        self.employment = self.pairs['emp-diff']
        self.payroll = self.pairs['pay-diff']

    @property
    def correlation(self):
        ''' Get Correlation Between Employment Data  & Payroll Data
        '''
        return self.pairs.corr().iloc[0][1]

    def scatter(self):
        ''' Plot Employment v Payroll Scatter
        '''
        plt.figure(figsize=(14, 8))
        plt.scatter(self.employment, self.payroll, c=ColorClient.lightblue, s=100., linewidth=0.15)

        # Add some guidelines:
        plt.plot((-self.cutoff, self.cutoff), (-self.cutoff, self.cutoff), '--', c=ColorClient.lightgray)
        plt.hlines(0, -self.cutoff, self.cutoff, colors=ColorClient.lightgray)
        plt.vlines(0, -self.cutoff, self.cutoff, colors=ColorClient.lightgray)
        plt.xlim((-self.cutoff, self.cutoff))
        plt.ylim((-self.cutoff, self.cutoff))

        # Add Some Labeling:
        plt.xlabel('EMPLOYMENT (Forecast - Actual)')
        plt.ylabel('PAYROLL (Forecast - Actual)')
        plt.title('EMPLOYMENT vs PAYROLL (Forecast - Actual) (CORRELATION=%.3f)' % self.correlation)

        # Show Plot:
        plt.show()


class PayrollMoversResearch(object):
    def __init__(self, months=36):
        ''' Research & Plot Biggest Movers Based on Payroll Announcement Misses
        '''
        self.investing = InvestingClient()
        self.months = months

        # Download Payroll Announement Data:
        self.payrolls = self.investing.payrolls()

        # Load prices data which occurred during payroll announcements:
        self.prices = self.loadprices()

        # Combine payroll announcements with price movements:
        self.data = self.combinedata()

    def loadprices(self):
        '''
        '''
        allreturns = {}
        for _, announcement in self.payrolls[-self.months:].iterrows():
            try:
                # Download equity prices on the day of this payroll announcement
                # to determine biggest movers:
                filename = 'clients/datasets/prices/%s.csv' % announcement.date
                prices = pd.read_csv(filename).bfill()

                # Parse timestamps into datetime objects:
                prices.index = prices.time.map(lambda ts: datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S+00:00'))
                prices.drop('time', axis=1, inplace=True)

                # Get the time window on this announcement date that we want to
                # analye for equity price movement (for this research, the analysis
                # is simple -- compare the largest movers based on the price 15
                # minutes before the payroll announcement with the price 2 hours
                # after the accounement):
                dt = datetime.datetime.combine(announcement.date, announcement.time)
                start = dt - datetime.timedelta(minutes=15)
                end = dt + datetime.timedelta(minutes=120)

                # Compute the total return of the vehicle over the investment window:
                returns = 100 * (prices.loc[end] / prices.loc[start] - 1.)

                # Save results for this date:
                allreturns[announcement.date] = returns
            except:
                print('WARN: An Error Occurred Fetching Prices on %s, Skipping...' % announcement.date)

        # Combine price changes from all symbols for all available dates:
        return pd.DataFrame(allreturns).T

    def combinedata(self):
        ''' Combine price movement dataset with payroll announcement forecasts
        '''
        # Create some copies of our instance datasets (to preserve method purity):
        payrolls = self.payrolls.copy()
        prices = self.prices.copy()
        payrolls.index = payrolls.date

        # Limit payroll dataset based on available prices data:
        payrolls = payrolls.loc[prices.index]

        # Compute the differences between payroll forecasts and observered
        # values, and combine with the price movements dataset:
        diffs = payrolls.actual - payrolls.forecast
        prices['payroll'] = diffs

        # Return the combined dataset with both price movements and payroll
        # values:
        return prices

    @property
    def correlations(self):
        ''' Compute Price Movement Correlations with Payroll Announcments
        '''
        return self.data.corr()['payroll']

    def barchart(self, top=20):
        ''' Plot A Barchart Showing Correlations Between
        '''
        # Get sorted correlations, filter by top N items:
        corrs = self.topcorrs(top)

        # Setup Plot:
        index = range(corrs.shape[0])
        plt.figure(figsize=(14, 8))
        plt.bar(index, corrs.values, color=ColorClient.lightblue, linewidth=0.15)

        # Some labeling:
        plt.xticks(index, corrs.index)
        plt.title('Equity Correlations w Payroll (Announcement - Forecast)')
        plt.ylabel('CORRELATION')
        plt.xlabel('SYMBOL')

        # Show Plot:
        plt.show()

    def topcorrs(self, top=20):
        ''' Get top N correlated equities with payroll numbers
        '''
        return self.correlations.sort_values().dropna().drop('payroll')[-top:]

    def backtest(self, symbols):
        '''
        '''
        economic = self.investing.getall()

        # Get the months that we have pricing information on:
        months = self.prices.index.map(lambda dt: dt.strftime('%Y-%m'))

        # Get employment announcment values (this is our trading signal):
        employment = economic.loc[months].dropna()[['emp-diff', 'pay-date']]
        employment.index = employment['pay-date']

        # Compute the trading signal:
        signal = employment['emp-diff'].loc[self.prices.index].dropna()
        signal[signal > 0] = 1.
        signal[signal < 0] = -1.

        # Get prices subframe for these symbols:
        prices = self.prices.copy().loc[signal.index][symbols]

        # Compute trades on symbols based on signals:
        trades = prices.multiply(signal, axis=0)

        summary = {}
        for symbol in symbols:
            summary[symbol] = {
                'mean': trades[symbol].mean(),
                'count': trades[symbol].count(),
                'min': trades[symbol].min(),
                'max': trades[symbol].max(),
                '50%': trades[symbol].median(),
                'win%': (trades[trades[symbol] > 0].count()[symbol]) / float(trades[symbol].count()) * 100.,
                'sharpe': trades[symbol].mean() / trades[symbol].std()
            }

        stats = pd.DataFrame(summary)

        # Compute basket stats:
        basket = trades.T.mean()
        basketstats = {
            'mean': basket.mean(),
            'count': stats.T['count'].sum(),
            'min': basket.min(),
            'max': basket.max(),
            '50%': basket.median(),
            'win%': (basket[basket > 0].count()) / float(basket.count()) * 100.,
            'sharpe': basket.mean() / basket.std()
        }
        stats['basket'] = pd.Series(basketstats)

        return trades, stats


if __name__ == '__main__':
    # Initialize Research Client:
    announcements = AnnouncementResearch()

    # Print last 10 rows of combined dataframe:
    print('\nPayroll vs Employment (Last 12 months):')
    print(announcements.data.tail(12))

    # Build a scatter plot:
    announcements.scatter()

    # Initalize Payroll vs Equity Movers Research Class:
    payroll = PayrollMoversResearch()

    # Plot top 20 Correlated Movers:
    payroll.barchart()

    # Run backtest:
    symbols = payroll.topcorrs(10).index
    trades, stats = payroll.backtest(symbols)
    print(tabulate(stats, headers=stats.columns, tablefmt="github"))
