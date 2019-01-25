import pandas as pd
import statsmodels.api as sm

from backtest import get_datasource


def _calc_rsrs(highs, lows, sample_periods):
    rsrs = [float('nan')] * sample_periods
    r2 = list(rsrs)
    lows = sm.add_constant(lows)
    for i in range(sample_periods, len(highs)):
        Y = highs[i - sample_periods:i]
        X = lows[i - sample_periods:i]
        cls = sm.OLS(Y, X).fit()
        beta = cls.params[1]
        rsrs.append(beta)
        r2.append(cls.rsquared)
    # assert (len(rsrs) == len(highs))
    return rsrs, r2


class RSRS_Indicator(object):
    def __init__(self, data=None):
        self.sample_periods = 18
        self.std_periods = 600
        if data is None:
            data = get_datasource()
        self.data = data

    def get_signals(self, code, index=False):
        key = 'rsrs_signals'
        cache = self.data.get_cache(key)
        code_key = code if not index else f'{code}_index'
        if not cache.has(code_key):
            if index:
                klines = self.data.get_index_daily(code)
            else:
                klines = self.data.get_daily_adj(code)

            rsrs, r2 = _calc_rsrs(klines['high'].values, klines['low'].values, self.sample_periods)
            rsrs = pd.Series(rsrs, index=klines.index, name='rsrs')
            r2 = pd.Series(r2, index=klines.index, name='r2')
            roll = rsrs.rolling(window=self.std_periods, min_periods=1)
            normal_rsrs = (rsrs - roll.mean()) / roll.std()
            fix_rsrs = normal_rsrs * r2
            cache.set(code_key, fix_rsrs)
        else:
            normal_rsrs = cache.get(code_key)
        return normal_rsrs


if __name__ == '__main__':
    import backtest

    data = backtest.get_datasource()
    daily = data.get_daily_adj('600000.SH')

    rsind = RSRS_Indicator()
    rsrs = rsind.get_signals(daily)
    print(rsrs)
