import datetime
import traceback
import numpy as np
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
    def __init__(self, months=36, before=15, after=120):
        ''' Research & Plot Biggest Movers Based on Payroll Announcement Misses
        '''
        self.investing = InvestingClient()
        self.months = months
        self._cache = {}

        # Minutes before/after payroll announcement positions are opened/closeed:
        self.before = before
        self.after = after

        # Download Payroll Announement Data:
        self.payrolls = self.investing.payrolls()

        # Load prices data which occurred during payroll announcements:
        self.loadreturns()

        # Combine payroll announcements with price movements:
        self.data = self.combinedata()

        # Get all announcements data:
        self.announcements = self.investing.getall()

    def _loadprices(self, filename):
        ''' Load Price Files
        '''
        if filename in self._cache:
            return self._cache[filename]
        else:
            # Parse timestamps into datetime objects:
            prices = pd.read_csv(filename).bfill()
            prices.index = prices['Unnamed: 0'].map(lambda ts: datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S'))
            prices.drop('Unnamed: 0', axis=1, inplace=True)

            # Save to cache & return prices:
            self._cache[filename] = prices
            return self._cache[filename]

    def loadreturns(self):
        ''' Load aggregated returns dataframe from all potential trade events
        '''
        allreturns = {}
        allchanges = {}
        allcosts = {}
        for _, announcement in self.payrolls[-self.months:].iterrows():
            try:
                # Download equity prices on the day of this payroll announcement
                # to determine biggest movers:
                filename = 'clients/datasets/prices/%s.csv' % announcement.date
                prices = self._loadprices(filename)

                # Get the time window on this announcement date that we want to
                # analye for equity price movement (for this research, the analysis
                # is simple -- compare the largest movers based on the price N
                # minutes before the payroll announcement with the price 2 hours
                # after the accounement):
                dt = datetime.datetime.combine(announcement.date, announcement.time)
                start = dt - datetime.timedelta(minutes=self.before)
                end = dt + datetime.timedelta(minutes=self.after)

                # Compute the total return of the vehicle over the investment
                # window, the change in product price, and the product cost:
                returns = 100 * (prices.loc[end] / prices.loc[start] - 1.)
                change = prices.loc[end] - prices.loc[start]
                cost = prices.loc[start]

                # Save results for this date:
                allreturns[announcement.date] = returns
                allchanges[announcement.date] = change
                allcosts[announcement.date]   = cost
            except Exception as e:
                pass
                # print('WARN: An Error Occurred Fetching Prices on %s: %s (Skipping)' % (announcement.date, e))

        # Aggregate price changes, returns, and costs for all dates:
        self.returns = pd.DataFrame(allreturns).T
        self.changes = pd.DataFrame(allchanges).T
        self.costs = pd.DataFrame(allcosts).T

    def combinedata(self):
        ''' Combine price movement dataset with payroll announcement forecasts
        '''
        # Create some copies of our instance datasets (to preserve method purity):
        payrolls = self.payrolls.copy()
        prices = self.returns.copy()
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

    def correlations(self, dates=None):
        ''' Compute Price Movement Correlations with Payroll Announcments
        '''
        if dates:
            # Use the subset of dates given to compute the signal/future
            # product correlations:
            return self.data.loc[dates].corr()['payroll']
        else:
            return self.data.corr()['payroll']

    @classmethod
    def smooth(cls, series, span=1):
        ''' Smooth the given time series
        '''
        smoothed = series.copy()
        for i in series.index:
            smoothed[i] = series.loc[(i - span - 1):(i + span)].mean()

        return smoothed

    @property
    def timeheld(self):
        ''' Compute Time Each Trade is Held in Days
        '''
        return (self.after + self.before) / (60. * 23.)

    @property
    def dates(self):
        ''' Get all available sample dates
        '''
        payrolls = set(self.payrolls.date)
        returns = set(self.returns.index)
        return sorted(payrolls.intersection(returns))

    def splitdates(self, split=0.6):
        ''' Get random split of available sample dates
        '''
        dates = self.dates
        count = len(dates)
        pivot = int(split * count)
        np.random.shuffle(dates)
        train = sorted(dates[:pivot])
        test = sorted(dates[pivot:])
        return train, test

    def barchart(self, top=20, dates=None):
        ''' Plot A Barchart Showing Correlations Between
        '''
        # Get sorted correlations, filter by top N items:
        corrs = self.topcorrs(top, dates=dates)

        # Setup Plot:
        index = range(corrs.shape[0])
        plt.figure(figsize=(14, 8))
        plt.bar(index, corrs.values, color=ColorClient.lightblue, linewidth=0.15)

        # Some labeling:
        plt.xticks(index, corrs.index)
        plt.title('Equity Correlations w Payroll (Announcement - Forecast)')
        plt.ylabel('CORRELATION')
        plt.xlabel('SYMBOL')
        plt.xticks(rotation=45)

        # Show Plot:
        plt.show()

    def topcorrs(self, top=20, dates=None):
        ''' Get top N correlated equities with payroll numbers
        '''
        return self.correlations(dates).sort_values().dropna().drop('payroll')[-top:]

    def backtest(self, symbols, dates):
        '''
        '''
        # Get the months that we have pricing information on:
        months = self.returns.loc[dates].index.map(lambda dt: dt.strftime('%Y-%m'))

        # Get employment announcment values (this is our trading signal):
        employment = self.announcements.copy().loc[months].dropna()[['emp-diff', 'pay-date']]
        employment.index = employment['pay-date']

        # Compute the trading signal:
        signal = employment['emp-diff'].loc[self.returns.loc[dates].index].dropna()
        signal[signal > 0] = 1.
        signal[signal < 0] = -1.

        # Compute trades on symbols based on signals:
        returns = self.returns.loc[signal.index][symbols].fillna(0.).multiply(signal, axis=0)
        trades = self.changes.loc[signal.index][symbols].fillna(0.).multiply(signal, axis=0)
        costs = self.costs.loc[signal.index][symbols].fillna(0.).multiply(signal, axis=0)

        summary = {}
        for symbol in symbols:
            summary[symbol] = {
                'mean': trades[symbol].mean(),
                'count': trades[symbol].count(),
                'min': trades[symbol].min(),
                'max': trades[symbol].max(),
                '50(%)': trades[symbol].median(),
                '25(%)': np.percentile(trades[symbol].dropna(), 25.),
                '75(%)': np.percentile(trades[symbol].dropna(), 75.),
                'total_dates': len(trades[symbol]),
                'total_pnl': trades[symbol].sum(),
                'total_volume': trades[symbol].count(),
                'std_unit_pnl': trades[symbol].std(),
                'std_daily_pnl': trades[symbol].std(),
                'mean_unit_pnl': trades[symbol].mean(),
                'average_daily_pnl': trades[symbol].mean(),
                'win(%)': (trades[trades[symbol] > 0].count()[symbol]) / float(trades[symbol].count()) * 100.,
                'sharpe': trades[symbol].mean() / trades[symbol].std() * ((252. / self.timeheld / 12.) ** 0.5)
            }

        stats = pd.DataFrame(summary)

        # Compute basket stats:
        basket = trades.T.mean()
        basketstats = {
            'mean': basket.mean(),
            'count': stats.T['count'].sum(),
            'min': basket.min(),
            'max': basket.max(),
            '50(%)': basket.median(),
            '25(%)': np.percentile(basket.dropna(), 25.),
            '75(%)': np.percentile(basket.dropna(), 75.),
            'total_pnl': basket.sum(),
            'std_unit_pnl': basket.std(),
            'std_daily_pnl': basket.std(),
            'mean_unit_pnl': basket.mean(),
            'average_daily_pnl': basket.mean(),
            'total_dates': len(basket),
            'total_volume': stats.T['count'].sum(),
            'win(%)': (basket[basket > 0].count()) / float(basket.count()) * 100.,
            'sharpe': basket.mean() / basket.std() * ((252. / self.timeheld / 12.) ** 0.5)
        }
        stats['basket'] = pd.Series(basketstats)

        # Reorder index to match required convention:
        stats = stats.T[[
            'count', 'mean', 'min', '25(%)', '50(%)', '75(%)', 'max', 'win(%)',
            'mean_unit_pnl', 'std_unit_pnl', 'average_daily_pnl', 'total_dates',
            'std_daily_pnl', 'sharpe', 'total_pnl', 'total_volume'
        ]].T

        # Assign an index name:
        stats.index.name = 'statistic'

        return trades, stats


    def scan(self, symbols, dates, lower=1, upper=120, smoothed=True, plot=True):
        ''' Scan over trade window/basket combinations
        '''
        original = self.after
        try:
            # Break out symbols into baskets, so we can anaylze each basket's
            # sharpe ratios individually:
            baskets = {
                tuple(symbols[-i:]): []
                for i in range(3, len(symbols) + 1)
            }

            # Iterate over window sizes:
            for after in range(lower, upper + 1):

                #
                self.after = after
                print 'Scanning (%s-%s) Window...' % (self.before, self.after)
                self.loadreturns()

                # For each product basket, get backtest stats
                for basket in baskets:
                    _, stats = self.backtest(list(basket), dates=dates)
                    baskets[basket].append(stats.basket.sharpe)

            # Maybe Smooth Data Series:
            if smoothed:
                for key in baskets:
                    series = pd.Series(baskets[key])
                    baskets[key] = self.smooth(series)

            # Maybe plot the resulting sharpe ladders:
            if plot:
                # Initialize Our Line Plot:
                plt.figure(figsize=(28, 16))

                # Plot the sharpe-ratio/minute surfaces for each basket:
                minutes = list(range(lower, upper + 1))
                for basket in sorted(baskets.keys(), key=lambda x: len(x), reverse=True):
                    sharpes = baskets[basket]
                    plt.plot(minutes, sharpes, label='-'.join(reversed(basket)), lw=2.)

                # Plot Target Sharpe Ratio
                target = 2.0
                plt.hlines(target, lower, upper, colors=ColorClient.lightgray, label='Target Sharpe (%s)' % target)

                # Add some axis labeling & save plot:
                plt.title('Sharpe Ratios Across Trade Windows & Baskets')
                plt.ylabel('SHARPE (annualized)')
                plt.xlabel('TRADE WINDOW (min. after payroll announcement)')
                plt.legend()
                plt.savefig('sharpes.png', bbox_inches='tight')
                plt.close()

        finally:
            # Revert after and prices value back to original amount:
            self.after = original
            self.loadreturns()

        return pd.DataFrame(baskets, index=minutes)


if __name__ == '__main__':
    # # Initialize Research Client:
    # announcements = AnnouncementResearch()
    #
    # # Print last 10 rows of combined dataframe:
    # print('\nPayroll vs Employment (Last 12 months):')
    # print(announcements.data.tail(12))
    #
    # # Build a scatter plot:
    # announcements.scatter()

    # # Initalize Payroll vs Equity Movers Research Class:
    # payroll = PayrollMoversResearch(months=156, after=60)
    #
    # # Get a train/test date split of available announcement dates:
    # train, test = payroll.splitdates(0.5)
    #
    # # # Plot top 20 Correlated Movers:
    # # payroll.barchart(dates=train)
    #
    # # Get the top 20 highest correlated stocks based on training sample:
    # print 'Optimizing trade basket/exit...'
    # symbols = payroll.topcorrs(6, dates=train).index
    #
    # # Scan across all trading windows and sub-baskets for the top-correlated
    # # symbols based on training data:
    # sharpes = payroll.scan(symbols, upper=80, dates=train, plot=True)
    #
    #
    # # Get the basket/window with the maximum sharpe ratio:
    # maxbasket = sorted([s for s in sharpes.max().idxmax() if isinstance(s, str)])
    # maxwindow = sharpes.idxmax()[sharpes.max().idxmax()]
    #
    # # Print out results:
    # print 'Found Optimal Solution: %s 15-%s (Sharpe=%.4f)' % (
    #     maxbasket,
    #     maxwindow,
    #     sharpes.max().max()
    # )
    #
    # # Do in-sample testing with best basket/window from above:
    # print 'In Sample Stats:'
    # _, stats = payroll.backtest(maxbasket, dates=train)
    # print(tabulate(stats.round(3), headers=stats.columns, tablefmt='github'))
    #
    # print '\n~~~~~~~~~~~~~~~~~~\n'
    #
    # # Do out-of-sample testing with best basket/window from above:
    # print 'Out-of-Sample Stats:'
    # _, stats = payroll.backtest(maxbasket, dates=test)
    # print(tabulate(stats.round(3), headers=stats.columns, tablefmt='github'))


    # Initalize Payroll vs Equity Movers Research Class:
    payroll = PayrollMoversResearch(months=156, after=5)
    # symbols = ['@RTY#', 'EB#', 'IHO#']
    symbols = payroll.returns.columns.tolist()
    trades, stats = payroll.backtest(symbols, payroll.dates)
    stats = stats.T.sort_values('total_pnl').T.round(2)
    basket = stats.pop('basket')
    stats['basket'] = basket
    stats.to_csv('summary.csv', sep=';')
