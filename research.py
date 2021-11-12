from clients.investing import InvestingClient
from clients.colors import ColorClient
import matplotlib.pyplot as plt

class TradeResearch(object):
    def __init__(self, cutoff=500000):
        '''
        '''
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

        # Separate Employment & Payroll Numbers:
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


if __name__ == '__main__':
    # Initialize Research Client:
    research = TradeResearch()

    # Print last 10 rows of combined dataframe:
    print('\nPayroll vs Employment (Last 12 months):')
    print(research.data.tail(12))

    # Build a scatter plot:
    research.scatter()
