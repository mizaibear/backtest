import pandas as pd

from data.cache_data import CacheData


def _filter_end_date(df, report_type='1231'):
    return df[df.apply(lambda row: row['end_date'][4:] == report_type, axis=1)]


class FundamentalValueModel(object):
    def __init__(self):
        self.data = CacheData()
        self._rank_factor = 1

    def get_values(self, trade_date, codes=None):
        stocks = self.data.get_stock_basic()
        if codes is None:
            codes = stocks[stocks['list_date'] <= trade_date].index.tolist()

        items = []
        # total = len(codes)
        for ts_code in codes:
            # print(f'debug - {ts_code} - {codes.index(ts_code)} / {total}')
            income = self.data.get_fins(ts_code, 'income', trade_date)
            if income is None:
                continue
            cashflow = self.data.get_fins(ts_code, 'cashflow', trade_date)
            if cashflow is None:
                continue
            balancesheet = self.data.get_fins(ts_code, 'balancesheet', trade_date)
            if balancesheet is None:
                continue
            dividend = self.data.get_fins(ts_code, 'dividend', trade_date)
            if dividend is None:
                continue

            dividend = dividend[dividend['div_proc'] == '实施']

            income = _filter_end_date(income, report_type='1231')
            cashflow = _filter_end_date(cashflow, report_type='1231')
            # balancesheet = _filter_end_date(balancesheet, report_type='1231')

            if len(income) > 0:
                revenue = income['revenue'].rolling(window=5, min_periods=1).mean().values[-1]
            else:
                revenue = 0
            if len(cashflow) > 0:
                n_cashflow_act = cashflow['n_cashflow_act'].rolling(window=5, min_periods=1).mean().values[-1]
            else:
                n_cashflow_act = 0
            if len(dividend) > 0:
                cash_div = dividend['cash_div'].rolling(window=5, min_periods=1).mean().values[-1]
            else:
                cash_div = 0

            total_hldr_eqy_exc_min_int = balancesheet['total_hldr_eqy_exc_min_int'].values[-1]
            total_share = balancesheet['total_share'].values[-1]

            item = {'ts_code': ts_code, '营业收入': revenue, '现金流量': n_cashflow_act, '分红': cash_div * total_share,
                    '净资产': total_hldr_eqy_exc_min_int}

            # daily = self.data.get_daily(ts_code).loc[:trade_date]
            # if len(daily)>0:
            #     amount = daily['amount'].rolling(window=240,min_periods=1).mean().values[-1]
            # else:
            #     amount = 0
            #
            # item['日均成交额'] = amount

            items.append(item)

        df = pd.DataFrame(items).set_index('ts_code')
        # df = df.sort_values(by='日均成交额').iloc[int(0.2*len(df)):]
        factors = df[['营业收入', '现金流量', '分红', '净资产']].copy()
        factors = factors / factors.sum() * 100
        fv = factors.mean(axis=1)
        factors['fvalue'] = (fv - fv.mean()) / fv.std()
        factors['name'] = stocks['name']
        factors['industry'] = stocks['industry']
        # 做行业中性处理
        industries = factors['industry'].drop_duplicates().values.tolist()
        for industry in industries:
            index = factors[factors['industry'] == industry].index
            factors.loc[index, 'irank'] = factors.loc[index, 'fvalue'].rank(ascending=False)

        factors['fvalue_fix'] = factors['fvalue'] / (factors['irank'] * self._rank_factor)

        return factors.sort_values(by='fvalue_fix', ascending=False)


if __name__ == '__main__':
    df = FundamentalValueModel().get_values('20050101')
    print(df.shape)
    print(df.head(50))
    CacheData().save_cache()
