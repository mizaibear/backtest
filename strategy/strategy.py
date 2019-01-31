import numpy as np

from models import ff_model, rsrs_model, fv_model


class StrategyBase(object):
    def __init__(self, account=None):
        self.account = account


class FFStrategy(object):
    def __init__(self, index_code, sample_periods=120, return_periods=1, nums=10, freq=20, ascending=True,
                 account=None):
        self.index_code = index_code
        self.account = account
        self.sample_periods = sample_periods
        self.nums = nums
        self.freq = freq
        self.count = 0
        self.ascending = ascending
        self.ff = ff_model.FF(n_ret=return_periods)

    def run(self, date):
        # 每20天调仓
        if self.count % self.freq == 0:
            # 获取alpha最小的10个股票
            try:
                codes = self.signals()
            except Exception as e:
                print(f'debug - {date}: 信号计算异常')
                print(str(e))
                codes = None

            if codes is not None:
                # 如果持仓不在目标对象中就卖出
                holdings = [item for item in self.account._holdings.values()]
                for item in holdings:
                    if item['qty'] > 0 and item['code'] not in codes:
                        self.account.order(item['code'], item['price'], -item['qty'])

                # 用剩余现金平均买入
                codes = [code for code in codes if code not in self.account._holdings]
                n = len(codes)
                if n > 0:
                    amount = self.account._cash / n
                    for code in codes:
                        price, qty = self.account.get_max_qty(code, amount)
                        self.account.order(code, price, qty)
                    print(f'debug - {date}:buy {codes}')

        self.count += 1

    def signals(self):
        trade_date = self.account._date
        alphas = self.ff.get_weights(self.index_code, self.sample_periods, trade_date)
        return alphas['alpha'].sort_values(ascending=self.ascending).index[:self.nums].tolist()


class LowBeta(FFStrategy):
    def __init__(self, index_code, sample_periods=120, return_periods=1, nums=10, freq=20, ascending=True,
                 account=None):
        super().__init__(index_code, sample_periods, return_periods, nums, freq, ascending, account)

    def signals(self):
        trade_date = self.account._date
        weights = self.ff.get_weights(self.index_code, self.sample_periods, trade_date)
        return weights['beta'].sort_values(ascending=self.ascending).index[:self.nums].tolist()


class ResidualMomentum(FFStrategy):
    def __init__(self, index_code, sample_periods=720, return_periods=1, nums=10, freq=20, ascending=False,
                 sum_periods=240, account=None):
        super().__init__(index_code, sample_periods, return_periods, nums, freq, ascending, account)
        self.sum_periods = sum_periods

    def signals(self):
        trade_date = self.account._date
        residuals = self.ff.get_residuals(self.index_code, self.sample_periods, trade_date)
        res_mean = residuals.mean(axis=1)
        res_std = residuals.std(axis=1)
        # 横截面归一化
        residuals = (residuals - res_mean) / res_std
        # 累计残差动量
        roll_sum = residuals.rolling(window=self.sum_periods, min_periods=1).sum()
        return roll_sum.iloc[-1].sort_values(ascending=self.ascending).index[:self.nums].tolist()


def _to_buy_codes(codes_buy, account):
    codes = [code for code in codes_buy if code not in account._holdings]
    codes = [code for code in codes if len(account.get_bars(code)) > 0]

    n = len(codes)
    result = []
    if n > 0:
        commision = account.get_commision(account._cash)
        amount = (account._cash - commision) / n

        for code in codes:
            price = account.get_price(code)
            # print(f'debug - {self.account._date} {code} {price}')
            if price is None:
                continue
            qty = np.floor(amount / price)
            if qty > 0:
                account.order(code, price, qty)
                result.append(code)
    return result


def _to_sell_codes(codes_sell, account):
    holdings = [item for item in account._holdings.values()]
    result = []
    for item in holdings:
        if item['qty'] > 0 and item['code'] in codes_sell:
            account.order(item['code'], item['price'], -item['qty'])
            result.append(item['code'])
    return result


class BuyAndHold(object):
    def __init__(self, codes, account=None):
        self._codes = codes  # 要买入的代码
        self.account = account  # 账户对象

    def run(self, date):
        _to_buy_codes(self._codes, self.account)


class MOM(object):
    def __init__(self, codes, args=(20, 10), account=None):
        self._codes = codes
        self.account = account
        self.args = args

    def buy_signal(self, code):
        bars = self.account.get_bars(code)
        n = self.args[0]
        c = bars['close']
        c_roll = c.rolling(window=n)
        # 最近N天最高价
        n_max = c_roll.max()
        # 当前价格创N天新高，区间上移
        zone = n_max.values[-1] <= c.values[-1]
        return zone

    def sell_signal(self, code):
        bars = self.account.get_bars(code)
        n = self.args[1]
        c = bars['close']
        c_roll = c.rolling(window=n)
        # 最近N天最低价
        n_min = c_roll.min()
        # 当前价格创N天新低，区间下移
        zone = n_min.values[-1] >= c.values[-1]
        return zone

    def run(self, date):
        # 先检查持仓有没有卖出信号
        self.do_sell()

        # 再看看有没有买入信号
        self.do_buy()

    def do_buy(self):
        codes = [code for code in self._codes if code not in self.account._holdings]
        codes = [code for code in codes if len(self.account.get_bars(code)) > 0]

        n = len(codes)
        if n > 0:
            commision = self.account.get_commision(self.account._cash)
            amount = (self.account._cash - commision) / n
            for code in codes:
                if self.buy_signal(code):
                    price = self.account.get_price(code)
                    # print(f'debug - {self.account._date} {code} {price}')
                    if price is None:
                        continue
                    qty = np.floor(amount / price)
                    self.account.order(code, price, qty)

    def do_sell(self):
        holdings = [item for item in self.account._holdings.values()]
        for item in holdings:
            if item['qty'] > 0 and self.sell_signal(item['code']):
                self.account.order(item['code'], item['price'], -item['qty'])


class RSRS_Strategy(MOM):

    def __init__(self, codes, index=True, account=None):
        super().__init__(codes, account=account)
        self._rsind = rsrs_model.RSRS_Indicator()
        self.index = index

    def get_rsrs_value(self, code):
        date = self.account._date
        rsrs = self._rsind.get_signals(code, self.index).loc[:date].values[-1]
        return rsrs

    def get_grid(self, code):
        from indicators import calc_fma, calc_atr
        date = self.account._date
        klines = self.account.get_bars(code)
        fma = calc_fma(klines['close'], 20)
        atr = calc_atr(klines, 20)
        grid = (klines['close'] - fma) / atr
        return grid

    def buy_signal(self, code):
        rsrs = self.get_rsrs_value(code)

        return rsrs > 1 and self.buy_filter(code)

    def sell_signal(self, code):
        rsrs = self.get_rsrs_value(code)
        return rsrs < -1 and self.sell_filter(code)

    def buy_filter(self, code):
        grid = self.get_grid(code)
        return grid.values[-1] > 1

    def sell_filter(self, code):
        grid = self.get_grid(code)
        return grid.values[-1] < -1


class FVStrategy(StrategyBase):
    def __init__(self, account=None):
        super().__init__(account)
        self.nums = 100
        self.freq = 240
        self.top_rank = 2
        self.model = fv_model.FundamentalValueModel()
        self.count = 0
        self.codes = []
        self._rsind = rsrs_model.RSRS_Indicator()
        self.benchmark = '399300.SZ'
        self.rsrs_signal = False
        self.empty = False

    def run(self, date):
        if self.count % self.freq == 0:
            values = self.model.get_values(date)
            values = values.loc[values['irank'] <= self.top_rank]
            self.codes = values.index[:self.nums].tolist()

            # 如果不启用rsrs择时，在选股之后直接调仓
            if len(self.codes) > 0 and not self.rsrs_signal:
                self.rebalance(date)

        self.count += 1

        # rsrs择时启用，择时后才调仓
        if self.rsrs_signal:
            # 空仓时出现买入信号就买入，非空仓时出现卖出信号就转空仓
            if self.empty:
                self.empty = not self.buy_signal(self.benchmark)
            else:
                self.empty = self.sell_signal(self.benchmark)

            codes_holding = [item['code'] for item in self.account._holdings.values()]
            if self.empty and len(codes_holding) > 0:
                sell_result = _to_sell_codes(codes_holding, self.account)
                if len(sell_result) > 0:
                    print(f'debug - {date} sell {len(sell_result)}: {sell_result}')
            if not self.empty:
                self.rebalance(date)

    def rebalance(self, date):
        codes_holding = [item['code'] for item in self.account._holdings.values()]
        codes_sell = [code for code in codes_holding if code not in self.codes]
        sell_result = _to_sell_codes(codes_sell, self.account)
        buy_result = _to_buy_codes(self.codes, self.account)
        if len(sell_result) > 0:
            print(f'debug - {date} sell {len(sell_result)}: {sell_result}')
        if len(buy_result) > 0:
            print(f'debug - {date} buy {len(buy_result)}: {buy_result}')

    def get_rsrs_value(self, index_code):
        date = self.account._date
        rsrs = self._rsind.get_signals(index_code, True).loc[:date].values[-1]
        return rsrs

    def get_grid(self, index_code):
        from indicators import calc_fma, calc_atr
        date = self.account._date
        klines = self.account.data.get_index_daily(index_code).loc[:date]
        fma = calc_fma(klines['close'], 20)
        atr = calc_atr(klines, 20)
        grid = (klines['close'] - fma) / atr
        return grid

    def buy_signal(self, index_code):
        rsrs = self.get_rsrs_value(index_code)

        return rsrs > 1 and self.buy_filter(index_code)

    def sell_signal(self, index_code):
        rsrs = self.get_rsrs_value(index_code)
        return rsrs < -1 and self.sell_filter(index_code)

    def buy_filter(self, index_code):
        grid = self.get_grid(index_code)
        return grid.values[-1] > 1

    def sell_filter(self, index_code):
        grid = self.get_grid(index_code)
        return grid.values[-1] < -1



if __name__ == '__main__':
    import backtest
    import account

    acc = account.IndexAccount(data=backtest.get_datasource())
    stra = RSRS_Strategy(['399300.SZ'], account=acc)
    report = backtest.backtest(stra, start='20160101', end='20181231')
    report.show()
