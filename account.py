import numpy as np
import pandas as pd

from report import Report


class Account(object):
    def __init__(self, init_cash=1000000, data=None):
        # 初始化账户的基础状态
        self._cash = init_cash  # 账户的初始现金默认100万
        self._holdings = {}  # 账户的持仓,用字典记录每个股票持仓
        self._date = None  # 当前日期
        self._amount = 0  # 当日交易额
        self._commision = 0  # 当日手续费
        self._records = []  # 账户的序列记录，用字典记录包括日期、净值
        self.data = data
        self._holding_cost = {}  # 记录持仓成本总额
        self._sell_win = 0  # 盈利卖出数
        self._sell_lose = 0  # 亏损卖出数
        self._sell_win_amount = 0  # 卖出盈利额
        self._sell_lose_amount = 0  # 卖出亏损额

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

        # 初始化当日交易额和手续费
        self._amount = 0
        self._commision = 0

    def write_record(self):
        item = {
            '日期': self._date,
            '净值': self.portfolio_value(),
            '现金': self._cash,
            '成交额': self._amount,
            '手续费': self._commision,
            '持仓数': len(self._holdings),
            '盈利卖出笔数': self._sell_win,
            '亏损卖出笔数': self._sell_lose,
            '卖出盈利额': self._sell_win_amount,
            '卖出亏损额': self._sell_lose_amount
        }
        self._records.append(item)

    def create_report(self, benchmark):
        df = pd.DataFrame(self._records).set_index('日期')
        # 获取指数行情作为对照基准
        bm_daily = self.data.get_index_daily(benchmark)
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
        if order_value == 0:
            # 如果下单价值为0，不执行操作
            print(f'debug - {code} {price} {qty} order_value zero.')
            return

        commision = self.get_commision(order_value)
        cost = order_value + commision  # 支出等于交易额+费用
        # 检查现金和持仓数量是否超支
        if self._cash - cost < 0:
            raise Exception(f'{self._date}:现金不足支出：{order_value} + {commision}\n现金：{self._cash}')

        hold_qty = 0
        if code in self._holdings:
            item = self._holdings[code]
            hold_qty = item['qty']

        if hold_qty + qty < 0:
            raise Exception(f'{self._date}:持仓余额不足：{qty}')

        # 现金和持仓都足够，可以确认交易完成
        # 根据持仓成本计算盈亏
        if code not in self._holding_cost:
            self._holding_cost[code] = 0

        if qty > 0:  # 买入时记录成本
            self._holding_cost[code] += cost
        else:  # 卖出时判断盈亏，增加卖出笔数
            avg_cost = self._holding_cost[code] / self._holdings[code]['qty']
            balance = avg_cost * qty
            gains = balance - cost  # 获利额度，注意cost和balance都应是负数
            if gains > 0:
                self._sell_win += 1
                self._sell_win_amount += gains
            else:
                self._sell_lose += 1
                self._sell_lose_amount += gains
            # 卖出后成本加上保本卖出额
            self._holding_cost[code] += balance  # 注意，balance是负的，因为卖出时qty为负数

        # 处理现金和持仓变化
        self._cash -= cost
        if code not in self._holdings:
            self._holdings[code] = {'code': code, 'qty': qty, 'price': price}
        else:
            item = self._holdings[code]
            item['qty'] += qty
            # 如果持仓为0就删掉
            if item['qty'] == 0:
                del self._holdings[code]

        # 记录增加当日交易额和手续费
        self._amount += abs(order_value)
        self._commision += commision


class IndexAccount(Account):
    def __init__(self, init_cash=1000000, data=None):
        super().__init__(init_cash, data)

    def get_price(self, code):
        df = self.get_bars(code)
        if len(df) == 0:
            return None
        return df['close'].values[-1]

    def get_bars(self, code):
        df = self.data.get_index_daily(index_code=code)
        return df.loc[:self._date]
