import numpy as np
import pandas as pd
import statsmodels.api as sm

from backtest import get_datasource


def _calc_ff_weights(rets, factors, cols, add_alpha=True):
    X = factors.values
    if add_alpha:
        X = sm.add_constant(X)
    items = []
    for code in rets.columns:
        # 最小二乘法进行拟合
        stock_rets = rets[code]
        cls = sm.OLS(stock_rets.values, X).fit()
        item = {'code': code}
        for i in range(len(cols)):
            item[cols[i]] = cls.params[i]
        items.append(item)
    result = pd.DataFrame(items).set_index('code')[cols]

    return result


def _calc_residuals(actual_rets, factors, weights):
    X = factors.copy()
    residuals = {}
    weights = weights[X.columns.tolist()].T
    for code in weights.columns:
        W = weights[code].values
        predict_rets = (X * W).sum(axis=1)
        residuals[code] = actual_rets[code] - predict_rets

    return pd.DataFrame(residuals)


class FF(object):
    def __init__(self, n_ret=1, data=None):
        if data is None:
            data = get_datasource()

        self.data = data
        self._codes = []
        self._n_ret = n_ret

    def get_factors(self, index_code, n_period, trade_date):
        codes = self.data.get_index_weight(index_code, trade_date=trade_date)['con_code'].values.tolist()
        if len(codes) > 0:
            self._codes = codes
        else:
            # 某些日期获取成份股会为空，用前面有效值代替
            self.codes = self._codes
        dates = self.data.get_trade_dates(end=trade_date)[-n_period:]
        rets = {}
        total_mv = {}
        pb = {}
        for code in codes:
            # print('debug',code)
            basic = self.data.get_daily_basic(code)
            clog = np.log(basic['close'])
            zf = clog - clog.shift(self._n_ret)
            rets[code] = zf.reindex(dates).fillna(0)
            total_mv[code] = basic['total_mv'].reindex(dates)
            pb[code] = basic['pb'].reindex(dates)
        rets = pd.DataFrame(rets)
        total_mv = pd.DataFrame(total_mv)
        pb = pd.DataFrame(pb)

        def smb(row):
            # 将指标排序分成3组，返回前1/3的平均回报 - 后1/3的平均回报
            row_rets = rets.loc[row.name]
            row = row.sort_values()
            n = int(len(row) / 3)
            small = row.index[:n]
            big = row.index[2 * n:]
            return row_rets.reindex(small).mean() - row_rets.reindex(big).mean()

        factor_items = {}
        # 市值加权计算市场收益作为市场因子
        factor_items['beta'] = (rets * total_mv).sum(axis=1) / total_mv.sum(axis=1)
        factor_items['total_mv'] = total_mv.apply(smb, axis=1)
        factor_items['pb'] = pb.apply(smb, axis=1)

        factors = pd.DataFrame(factor_items)
        cols = ['beta', 'total_mv', 'pb']
        return rets, factors[cols], cols

    def get_weights(self, index_code, n_period, trade_date):
        rets, factors, cols = self.get_factors(index_code, n_period, trade_date)
        cols = ['alpha'] + cols
        weights = _calc_ff_weights(rets, factors, cols, add_alpha=True)
        return weights[cols]

    def get_residuals(self, index_code, n_period, trade_date):
        actual_rets, factors, cols = self.get_factors(index_code, n_period, trade_date)
        # cols = ['alpha'] + cols
        weights = _calc_ff_weights(actual_rets, factors, cols, add_alpha=False)
        # factors['alpha'] = 1
        residuals = _calc_residuals(actual_rets, factors, weights)
        return residuals
