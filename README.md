# Employment vs Payroll Trade

### Datasets:
- Employment numbers vs Payroll numbers dataset: [link](https://github.com/djcohen03/monick/blob/main/clients/datasets/employment-vs-payroll.csv)
- Equity Prices on Payroll Announcement Dates: [link](https://github.com/djcohen03/monick/tree/main/clients/datasets/prices)

### Premise:
There are two main legs to this trade:
1. Trying to predict the difference between forecasted U.S. Nonfarm Payroll versus the observed value, based on the difference between the  U.S. ADP Nonfarm Employment Change forecast and observed values (which are generally reported a few days before payroll numbers)
2. Trying to predict assets that will move as a result of the difference between forecasted U.S. Nonfarm Payroll versus the observed value

### 1) Correlation between employment announcement data and payroll data:
Based on the numbers from the past three years of annoucements (less a few outliers), it appears that there is a reasonably strong correlation between the employment misses (observed - forecasted) and the payroll misses:
![corrs](https://github.com/djcohen03/monick/blob/main/plots/EmpVsPay.png?raw=true)

### 2) Equities which are highly correlated with payroll miss data:
The following are the top 20 euqities (out of a sample of around 180) which have movement most-correlated with the result of the payroll announcements, relative to their forecasts:
![corrs](https://github.com/djcohen03/monick/blob/main/plots/EquityVsPayroll.png?raw=true)

### Trade Summary:
Assuming we buy/sell the top 10 most-correlated equities 15 minutes before the payroll announcement, based on the results of the employemnt data (simply put, we _buy_ them if employment numbers _exceeded_ expectations, and _sell_ otherwise), and close them 2 hours afterwards, in the past ~15 announcements, the trade would have resulted in the following performace: 
|        |        ADP |       AIG |      AMGN |        BAC |          C |         CB |        GE |       IBM |          PG |       XOM |      basket |
|--------|------------|-----------|-----------|------------|------------|------------|-----------|-----------|-------------|-----------|-------------|
| count  | 15         | 15        | 15        | 15         | 15         | 15         | 15        | 15        | 15          | 15        | 150         |
| min    | -1.13918   | -1.47415  | -1.01231  | -1.16009   | -1.51667   | -0.774019  | -1.24542  | -0.942071 | -0.902928   | -0.280041 |  -0.748835  |
| 50%    |  0         |  0        |  0.16363  |  0.0608088 |  0.0476417 | -0         |  0.291627 | -0.027482 | -0.116099   |  0.359477 |   0.0669183 |
| max    |  1.18121   |  2.49584  |  2.62564  |  2.20484   |  1.8563    |  1.07552   |  1.61765  |  1.34374  |  1.17427    |  0.908848 |   1.36417   |
| mean   |  0.0290071 |  0.136227 |  0.255764 |  0.125228  |  0.174411  |  0.0886187 |  0.325317 |  0.219803 | -0.0004657  |  0.362082 |   0.171599  |
| sharpe |  0.0450487 |  0.126554 |  0.270551 |  0.138264  |  0.191455  |  0.186956  |  0.397047 |  0.356139 | -0.00078178 |  1.0516   |   0.343508  |
| win%   | 33.3333    | 46.6667   | 60        | 53.3333    | 53.3333    | 20         | 66.6667   | 46.6667   | 33.3333     | 80        |  60         |

### Notes:
Many shortcuts were taken here for the purposes of time.  Some follow-up thoughts:
- The data that was used to find correlated assets was also used to see performance of trades, which is not really ideal.  Would have been better to try these equities with an untouched sample, to see if they are truly correlated with payroll, or if it was just noise in the data
- I was not able to look too closely at the time stamps, to make sure everything lined up with respect to the daylight savings time changes, when I was coordinating between payroll announcements and equity prices.  
- Many other economic indicators are available on the investing.com website that was used to pull payroll and employment data.  If the trade does indeed have legs, there may be other opportunities to pair other economic indicators
- Some basic minute-by-minute equities data was used to do the research, but futures data may be interesting to explore as well, particularly given the additional available leverage
- Lots of data-cleaning issues resulted in fairly small datasets (all said and done, I only ended up with 15 trades per equity).  Larger sample size would be nice, with a little more time
