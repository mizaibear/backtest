from data.pickle_cache import PickleCache, CacheManager
from data.ts_db import TsDatabase


class CacheData(object):
    _cache_manager = CacheManager(root_dir='d:/ts_data_caches')

    def __init__(self):
        self.database = TsDatabase()

    def save_cache(self):
        self._cache_manager.save_all()

    def get_cache(self, key):
        cache = self._cache_manager.get(key, init_load=True)  # type:PickleCache
        return cache

    def get_trade_dates(self, start=None, end=None):
        # 获取交易日
        key = 'trade_cal'
        cache = self.get_cache(key)

        # 做缓存
        if not cache.has(key):
            df = self.database.get_trade_cal()
            cache.set(key, df)

        result = cache.get(key)

        if start is not None:
            result = result[result['cal_date'] >= start]
        if end is not None:
            result = result[result['cal_date'] <= end]
        return result['cal_date'].values.tolist()

    def get_price(self, code, date):
        # 获取某个交易日价格
        bars = self.get_bars(code, date)
        if len(bars) == 0:
            return None
        return bars['close'].values[-1]

    def get_bars(self, code, date):
        # 获取某个交易日的前复权行情
        df = self.get_daily_adj(code)
        return df.loc[:date]

    def get_fins(self, code, api_name, trade_date, limit_time=60 / 80):
        key = 'fins'
        cache = self.get_cache(key)

        if not cache.has(code):
            fins = {}
            cache.set(code, fins)

        fins = cache.get(code)
        if api_name not in fins:
            df = self.database.query_by_api(api_name, query={'ts_code': code}, limit_time=limit_time)
            if df is not None:
                df = df.set_index('ann_date', drop=False).sort_index()
            fins[api_name] = df
            cache.set(code, fins)

        df = fins[api_name]

        if df is None:
            return None

        return df[df['ann_date'] <= trade_date]

    def get_daily(self, code):
        # 获取个股行情
        key = 'daily'
        cache = self.get_cache(key)

        if not cache.has(code):
            df = self.database.get_daily(code)
            cache.set(code, df)

        return cache.get(code)

    def get_adj_factor(self, code):
        key = 'adj_factor'
        cache = self.get_cache(key)

        if not cache.has(code):
            df = self.database.get_adj_factor(code)
            cache.set(code, df)

        return cache.get(code)

    def get_daily_adj(self, code):
        key = 'adj_daily'
        cache = self.get_cache(key)

        if not cache.has(code):
            df = self.get_daily(code).copy()
            adj_factor = self.get_adj_factor(code)
            df['adj_factor'] = adj_factor['adj_factor'] / adj_factor['adj_factor'].values[-1]
            df['adj_factor'] = df['adj_factor'].fillna(method='ffill')
            for col in ['open', 'close', 'high', 'low']:
                df[col] = df[col] * df['adj_factor']
            cache.set(code, df)

        return cache.get(code)

    def get_daily_basic(self, code):
        key = 'daily_basic'
        cache = self.get_cache(key)

        if not cache.has(code):
            df = self.database.get_daily_basic(code)
            cache.set(code, df)

        return cache.get(code)

    def get_index_daily(self, index_code):
        key = 'index_daily'
        cache = self.get_cache(key)

        if not cache.has(index_code):
            df = self.database.get_index_daily(index_code)
            cache.set(index_code, df)

        return cache.get(index_code)

    def get_index_dailybasic(self, index_code):
        key = 'index_dailybasic'
        cache = self.get_cache(key)

        if not cache.has(index_code):
            df = self.database.get_index_dailybasic(index_code)
            cache.set(index_code, df)

        return cache.get(index_code)

    def get_index_weight(self, index_code, trade_date):
        key = 'index_weight'
        cache = self.get_cache(key)

        if not cache.has(index_code):
            cache.set(index_code, {})

        index_weight = cache.get(index_code)

        if trade_date not in index_weight:
            index_weight[trade_date] = self.database.get_index_weight(index_code, trade_date)

        return index_weight[trade_date]

    def get_stock_basic(self):
        key = 'stock_basic'
        cache = self.get_cache(key)

        if not cache.has(key):
            df = self.database.get_stock_basic()
            cache.set(key, df)

        return cache.get(key)


if __name__ == '__main__':
    data = CacheData()
    df = data.get_fins('600000.SH', 'income', '20190129')
    print(df.tail())
