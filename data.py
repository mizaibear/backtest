import os  # 系统操作

import pandas as pd
import tushare as ts  # 著名数据源

# 设置默认的通行证
_TOKEN = 'a4ebb94e55b03efa52e63aab7e6cda485842b7b5610972d1e0d3b410'
# 默认存储目录
_DIRNAME = 'tspro'


def read_daily(api, code, index=False):
    # 获取前复权的日线行情，缓存到本地以免重复下载
    dirname = _DIRNAME
    postfix = ''  # 后缀
    if index:
        postfix = '_index'
    filename = f'{code}{postfix}.csv'
    filepath = os.path.join(dirname, filename)
    if not os.path.exists(filepath):
        if not os.path.exists(dirname):
            # 如果当前目录没有_DIRNAME文件夹,就创建一个
            os.makedirs(dirname)
        if index:
            df = api.index_daily(ts_code=code)
        else:
            df = ts.pro_bar(pro_api=api, ts_code=code, adj='qfq')
        df.to_csv(filepath, index=False)
        print(f'{filepath} saved.')
    else:
        df = pd.read_csv(filepath, dtype={'ts_code': 'str', 'trade_date': 'str'}).set_index('trade_date',
                                                                                            drop=False)
    return df.sort_index()


def read_index_weight(api, index_code, trade_date):
    '''
    下载指数成份权重表
    :param api:ts.pro_api
    :param index_code: 指数代码
    :param trade_date: 交易日
    :return: 成份股权重
    '''
    dirname = _DIRNAME
    if not os.path.exists(dirname):
        # 如果当前目录没有_DIRNAME文件夹,就创建一个
        os.makedirs(dirname)

    filename = f'{index_code}_weight.csv'
    filepath = os.path.join(dirname, filename)

    if not os.path.exists(filepath):
        df = api.index_weight(index_code=index_code, end_date=trade_date)
        if df is not None and len(df) > 0:
            # 数据不为空才保存
            df.to_csv(filepath, index=False)
            print(f'{filepath} saved.')
    else:
        df = pd.read_csv(filepath, dtype={'trade_date': 'str'})
        # 追加保存新数据
        if df['trade_date'].max() < trade_date or df['trade_date'].min() > trade_date:
            df2 = api.index_weight(index_code=index_code, end_date=trade_date)
            if len(df2) > 0:
                df = df.append(df2).drop_duplicates().sort_values(by='trade_date')
                df.to_csv(filepath, index=False)
                print(f'{filepath} append saved.')
    con = df['trade_date'] <= trade_date
    date = df[con]['trade_date'].max()
    return df[df['trade_date'] == date]


def read_daily_basic(api, code):
    dirname = _DIRNAME
    if not os.path.exists(dirname):
        # 如果当前目录没有_DIRNAME文件夹,就创建一个
        os.makedirs(dirname)

    filename = f'{code}_basic.csv'
    filepath = os.path.join(dirname, filename)
    if not os.path.exists(filepath):
        df = api.daily_basic(ts_code=code)
        df.to_csv(filepath, index=False)
        print(f'{filepath} saved.')
    else:
        df = pd.read_csv(filepath, dtype={'ts_code': 'str', 'trade_date': 'str'}).set_index('trade_date',
                                                                                            drop=False)
    return df.sort_index()


def read_trade_dates(api):
    dirname = _DIRNAME
    filename = 'trade_cal.csv'
    filepath = os.path.join(dirname, filename)
    if not os.path.exists(filepath):
        result = api.query('trade_cal', exchange='SSE', is_open=1)
        result.to_csv(filepath, index=False)
        print(f'{filepath} saved.')
    else:
        result = pd.read_csv(filepath, dtype={'cal_date': 'str'})
    return result


class Data(object):
    def __init__(self, token=_TOKEN):
        self.pro = ts.pro_api(token=token)
        self._daily = {}
        self._basic = {}
        self._trade_dates = None

    def get_dates(self, start=None, end=None):
        # 获取交易日
        if self._trade_dates is None:
            # 做缓存
            self._trade_dates = read_trade_dates(self.pro)

        result = self._trade_dates

        if start is not None:
            result = result[result['cal_date'] >= start]
        if end is not None:
            result = result[result['cal_date'] <= end]
        return result['cal_date'].values.tolist()

    def get_price(self, code, date, index=False):
        # 获取某个交易日价格
        bars = self.get_bars(code, date, index)
        return bars['close'].values[-1]

    def get_bars(self, code, date, index=False):
        # 获取某个交易日的序列信息
        df = self.get_daily(code, index)
        return df.loc[:date]

    def get_daily(self, code, index=False):
        # 缓存在字段中，避免重复加载文件
        if code not in self._daily:
            df = read_daily(self.pro, code, index)
            self._daily[code] = df
        else:
            df = self._daily[code]
        return df

    def get_basic(self, code):
        # 缓存在字段中
        if code not in self._basic:
            df = read_daily_basic(self.pro, code)
            self._basic[code] = df
        else:
            df = self._basic[code]
        return df

    def get_index_weight(self, index_code, trade_date):
        return read_index_weight(self.pro, index_code, trade_date)
