import numpy as np

from models import ff_model, rsrs_model


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


class BuyAndHold(object):
    def __init__(self, codes, account=None):
        self._codes = codes  # 要买入的代码
        self.account = account  # 账户对象

    def run(self, date):
        # 买入取尚未持仓的股票
        codes = [code for code in self._codes if code not in self.account._holdings]
        n = len(codes)
        if n > 0:
            commision = self.account.get_commision(self.account._cash)
            amount = (self.account._cash - commision) / n  # 计算每只股票购买金额
            for code in self._codes:
                price = self.account.get_price(code)  # 获取当前价格
                qty = np.floor(amount / price)  # 向下取整
                self.account.order(code, price, qty)  # 直接调用账户执行交易


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

    def buy_signal(self, code):
        rsrs = self.get_rsrs_value(code)
        return rsrs > 1

    def sell_signal(self, code):
        rsrs = self.get_rsrs_value(code)
        return rsrs < -1


if __name__ == '__main__':
    import backtest
    import account

    report = backtest.backtest(RSRS_Strategy('399300.SZ', account=account.IndexAccount(data=backtest.get_datasource())),
                               start='20100101', end='20171231')
    report.show()
