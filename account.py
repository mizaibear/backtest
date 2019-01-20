import numpy as np
import pandas as pd

from report import Report


class Account(object):
    def __init__(self, init_cash=1000000, data=None):
        # 初始化账户的基础状态
        self._cash = init_cash  # 账户的初始现金默认100万
        self._holdings = {}  # 账户的持仓,用字典记录每个股票持仓
        self._date = None  # 更新持仓数据的日期
        self._records = []  # 账户的序列记录，用字典记录包括日期、净值
        self.data = data  # 数据源

    def holding_value(self):
        # 持仓市值
        value = 0
        if len(self._holdings) > 0:
            values = [item['price'] * item['qty'] for item in self._holdings.values()]
            value = sum(values)
        return value

    def portfolio_value(self):
        # 整个组合的净值 = 现金+持仓市值
        return self._cash + self.holding_value()

    def update_price(self, code, price):
        # 更新持仓价格
        if code in self._holdings:
            item = self._holdings[code]
            item['price'] = price

    def get_price(self, code):
        # 获取当前日期的价格
        return self.data.get_price(code, self._date)

    def get_bars(self, code):
        # 获取当前日期的序列
        return self.data.get_bars(code, self._date)

    def update(self, date):
        # 更新账户的日期和当日的持仓价格
        self._date = date
        for code in self._holdings.keys():
            price = self.get_price(code)  # 获取当前价格来更新
            self.update_price(code, price)

    def write_record(self):
        item = {'日期': self._date, '净值': self.portfolio_value()}
        self._records.append(item)

    def create_report(self, benchmark):
        df = pd.DataFrame(self._records).set_index('日期')
        # 获取指数行情作为对照基准
        bm_daily = self.data.get_daily(benchmark, index=True)
        df['benchmark'] = bm_daily['close']
        return Report(df)

    def get_commision(self, amount):
        # 计算费用
        commision_rate = 0.0015  # 设置费率平均千分之一点五
        return abs(amount) * commision_rate

    def get_max_qty(self, code, amount):
        if amount <= 0:
            raise Exception('金额必须大于0')

        commision = self.get_commision(amount)
        valid_amount = amount - commision
        price = self.get_price(code)
        qty = np.floor(valid_amount / price)
        return price, qty

    def order(self, code, price, qty):
        # 下单
        order_value = price * qty  # 交易额，qty为正就是买，qty为负就是卖
        commision = self.get_commision(order_value)
        cost = order_value + commision  # 支出等于交易额+费用
        # 检查现金和持仓数量是否超支
        if self._cash - cost < 0:
            raise Exception(f'{self._date}:现金不足支出：{cost}\n现金：{self._cash}')

        hold_qty = 0
        if code in self._holdings:
            item = self._holdings[code]
            hold_qty = item['qty']

        if hold_qty + qty < 0:
            raise Exception(f'{self._date}:持仓余额不足：{qty}')

        # 现金和持仓都足够，可以确认交易完成
        self._cash -= cost
        if code not in self._holdings:
            self._holdings[code] = {'code': code, 'qty': qty, 'price': price}
        else:
            item = self._holdings[code]
            item['qty'] += qty
            # 如果持仓为0就删掉
            if item['qty'] == 0:
                del self._holdings[code]
