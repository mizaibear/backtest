import numpy as np
import pandas as pd
import statsmodels.api as sm

import data


class FF(object):
    def __init__(self):
        self.data = data.Data()
        self._codes = []

    def get_factors(self, index_code, n, trade_date):
        codes = self.data.get_index_weight(index_code, trade_date=trade_date)['con_code'].values.tolist()
        if len(codes) > 0:
            self._codes = codes
        else:
            # 某些日期获取成份股会为空，用前面有效值代替
            self.codes = self._codes
        dates = self.data.get_dates(end=trade_date)[-n:]
        rets = {}
        mvs = {}
        pbs = {}
        for code in codes:
            basic = self.data.get_basic(code)
            clog = np.log(basic['close'])
            zf = clog - clog.shift(1)
            rets[code] = zf.reindex(dates).fillna(0)
            mvs[code] = basic['total_mv'].reindex(dates).fillna(method='ffill')
            pbs[code] = basic['pb'].reindex(dates).fillna(method='ffill')
        rets = pd.DataFrame(rets)
        mvs = pd.DataFrame(mvs)
        pbs = pd.DataFrame(pbs)

        def smb(row):
            # 将指标排序分成3组，返回前1/3的平均回报 - 后1/3的平均回报
            row_rets = rets.loc[row.name]
            row = row.sort_values()
            n = int(len(row) / 3)
            small = row.index[:n]
            big = row.index[2 * n:]
            return row_rets.reindex(small).mean() - row_rets.reindex(big).mean()

        factors = pd.DataFrame(
            {'mean_rets': rets.mean(axis=1), 'mv_rets': mvs.apply(smb, axis=1), 'pb_rets': pbs.apply(smb, axis=1)})
        return rets, factors

    def get_alphas(self, index_code, n, trade_date):
        rets, factors = self.get_factors(index_code, n, trade_date)
        X = sm.add_constant(factors.values)
        items = []
        for col in rets.columns:
            # 最小二乘法进行拟合
            cls = sm.OLS(rets[col].values, X).fit()
            item = {'code': col, 'alpha': cls.params[0], 'beta': cls.params[1], 'w_mv': cls.params[2],
                    'w_pb': cls.params[3]}
            items.append(item)

        return pd.DataFrame(items).set_index('code')
