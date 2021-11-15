from clients.investing import InvestingClient
from clients.prices import PricesDBClient
from clients.env import EnvClient


if __name__ == '__main__':
    # # Inialize Environment Variable Client for Loading up some Secrets:
    # env = EnvClient()
    #
    # # Initialize Database Connection for Downloading Prices:
    # database = PricesDBClient(host=env.DBHOST, username=env.DBUSERNAME, password=env.DBPASSWORD)
    #
    # # Download all equity symbols available in the prices database:
    # symbols = database.getsymbols()

    # Initialize a client to download investing.com data (payroll, nonfarm, etc):
    invclient = InvestingClient()

    # Download and save Full combined history of payroll & employment numbers:
    invclient.getall().to_csv('clients/datasets/employment-vs-payroll.csv')


    # # Get last 3 years of payroll announcement dates:
    # dates = invclient.payrolls().date[-36:]
    #
    # # Download data for payroll observation dates:
    # for date in reversed(dates):
    #     print '--- Downloading %s' % date
    #     database.download(symbols, date)
