import numpy as np

import model


class FFStrategy(object):
    def __init__(self, index_code, days=60, nums=10, freq=20, account=None):
        self.index_code = index_code
        self.account = account
        self.days = days
        self.nums = nums
        self.freq = freq
        self.count = 0
        self.ff = model.FF()

    def run(self, date):
        # 每20天调仓
        if self.count % self.freq == 0:
            # 获取alpha最小的10个股票
            codes = self.signals()
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
        try:
            alphas = self.ff.get_alphas(self.index_code, self.days, trade_date)
            return alphas['alpha'].sort_values().index[:10].tolist()
        except Exception as e:
            print(f'debug - {trade_date}: 信号计算异常')
            print(str(e))
            return None


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
        holdings = [item for item in self.account._holdings.values()]
        for item in holdings:
            if item['qty'] > 0 and self.sell_signal(item['code']):
                self.account.order(item['code'], item['price'], -item['qty'])

        # 再看看有没有买入信号
        codes = [code for code in self._codes if code not in self.account._holdings]
        n = len(codes)
        if n > 0:
            commision = self.account.get_commision(self.account._cash)
            amount = (self.account._cash - commision) / n
            for code in codes:
                if self.buy_signal(code):
                    price = self.account.get_price(code)
                    qty = np.floor(amount / price)
                    self.account.order(code, price, qty)
