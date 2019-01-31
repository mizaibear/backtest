import time

import pandas as pd
import pymongo
import tushare as ts


class TsUpdate(object):
    def __init__(self, database=None):
        if database is None:
            database = TsDatabase()
        self.database = database

    @property
    def db(self):
        return self.database.db

    @property
    def pro(self):
        return self.database.pro

    # 更新股票基本信息
    def update_stock_basic(self):
        key = 'stock_basic'
        roll = self.db[key]
        df = self.pro.stock_basic()
        count = 0
        for item in df.to_dict(orient='records'):
            result = roll.update_one({'ts_code': item['ts_code']}, update=item, upsert=True)
            count += result.modified_count
        print(f'{count} modified.')

    # 向前更新ts_code+trade_date类的通用接口
    def update_previous_tradedates(self, api_name, ts_code, min_date):
        df = self.pro.query(api_name=api_name, ts_code=ts_code, end_date=min_date)
        df = df[df['trade_date'] < min_date]
        if len(df) > 0:
            ids = self.db[api_name].insert_many(df.to_dict(orient='records')).inserted_ids
            print(f'{len(ids)} inserted. {ts_code}')

    # 向后更新ts_code+trade_date类的通用接口
    def update_next_tradedates(self, api_name, ts_code, max_date):
        df = self.pro.query(api_name=api_name, ts_code=ts_code, start_date=max_date)
        df = df[df['trade_date'] > max_date]
        if len(df) > 0:
            ids = self.db[api_name].insert_many(df.to_dict(orient='records')).inserted_ids
            print(f'{len(ids)} inserted. {ts_code}')

    # 向后更新ts_code+end_date类的通用接口
    def update_next_reports(self, api_name, ts_code, max_date):
        df = self.pro.query(api_name=api_name, ts_code=ts_code, start_date=max_date)
        df = df[df['end_date'] > max_date]
        if len(df) > 0:
            ids = self.db[api_name].insert_many(df.to_dict(orient='records')).inserted_ids
            print(f'{len(ids)} inserted. {ts_code} {api_name}')

    # 聚合ts_code+trade_date类的日期信息
    def get_aggregate_tradedates(self, api_name):
        coll = self.db[api_name]
        pipeline = [
            {'$group': {
                '_id': '$ts_code',
                'min_date': {'$min': '$trade_date'},
                'max_date': {'$max': '$trade_date'},
                'count': {'$sum': 1}
            }},
        ]
        records = list(coll.aggregate(pipeline))
        agg_dates = pd.DataFrame(records).set_index('_id')
        return agg_dates

    # 聚合ts_code+end_date+ann_date类的日期信息
    def get_aggregate_financial(self, api_name):
        coll = self.db[api_name]
        pipeline = [
            {
                '$group': {
                    '_id': '$ts_code',
                    'min_date': {'$min': '$end_date'},
                    'max_date': {'$max': '$end_date'},
                    'last_ann_date': {'$max': '$ann_date'},
                    'count': {'$sum': 1}
                }
            }
        ]
        records = list(coll.aggregate(pipeline))
        agg_fins = pd.DataFrame(records).set_index('_id')
        return agg_fins

    # 批量更新可以按trade_date横截面查询的接口
    def update_routine_single(self, api_name, trade_date):
        df = self.pro.query(api_name=api_name, trade_date=trade_date)
        if len(df) > 0:
            records = df.to_dict(orient='records')
            roll = self.db[api_name]
            count = 0
            for item in records:
                filter = {'ts_code': item['ts_code'], 'trade_date': item['trade_date']}
                upserted_id = roll.update_one(filter=filter, update={'$set': item}, upsert=True).upserted_id
                if upserted_id:
                    count += 1
            print(f'{count} upserted. {api_name} {trade_date}')

    # 更新所有trade_date截面的日常行情数据
    def update_all_routine(self, trade_date, update_colls=None):
        if not update_colls:
            update_colls = ['daily', 'adj_factor', 'daily_basic']

        for name in update_colls:
            self.update_routine_single(name, trade_date)

    def fix_routine_single(self, api_name, limit_time):
        agg_dates = self.get_aggregate_tradedates(api_name)

        # 前补，接口限制一次只能获取4000行数据，可能前面有遗漏的补上
        filter_dates = agg_dates[agg_dates['count'] == 4000]
        for i, row in filter_dates.iterrows():
            time.sleep(limit_time)
            ts_code = row.name
            self.update_previous_tradedates(api_name, ts_code, row['min_date'])

        # 后补
        for i, row in agg_dates.iterrows():
            time.sleep(limit_time)
            ts_code = row.name
            self.update_next_tradedates(api_name, ts_code, row['max_date'])

    # 补全部日常行情数据
    def fix_all_routine(self, update_colls=None):
        if not update_colls:
            update_colls = ['daily', 'adj_factor', 'daily_basic', 'index_daily', 'index_dailybasic']
        limit_time = 60 / 200
        for name in update_colls:
            print(f'更新{name}')
            self.fix_routine_single(api_name=name, limit_time=limit_time)

    # 自动往后增量全部财务报表数据
    def fix_all_financial(self, max_report_date):
        financial_colls = ['fina_indicator', 'income', 'cashflow', 'balancesheet']

        t1 = time.time()
        limit_time = 60 / 80

        for name in financial_colls:
            print(f'更新 {name}')
            agg_fins = self.get_aggregate_financial(name)
            if max_report_date is not None:
                agg_fins = agg_fins[agg_fins['max_date'] < max_report_date]
            for i, row in agg_fins.iterrows():
                t2 = time.time()
                if t2 - t1 < limit_time:
                    time.sleep(limit_time)

                ts_code = row.name
                self.update_next_reports(api_name=name, ts_code=ts_code, max_date=row['max_date'])
                t1 = time.time()

    def run_daily_update(self):
        # 更新指数
        print('更新指数行情')
        self.fix_all_routine(update_colls=['index_daily', 'index_dailybasic'])
        index_daily = self.database.get_index_daily('000001.SH')
        date = index_daily['trade_date'].iloc[-1]

        # 更新个股
        print(f'更新股票行情{date}')
        self.update_all_routine(date)


class TsDatabase(object):
    client = None
    db = None

    def __init__(self):
        if self.db is None:
            self.client = pymongo.MongoClient(host='localhost', port=27017)
            self.db = self.client['tushare_pro']

        # 获取数据库中存储的token , 初始化pro_api
        token = self.get_token()
        self.pro = ts.pro_api(token)

    def set_token(self, token):
        configs = self.db['configs']
        configs.update_one({'key': 'token'}, {'$set': {'value': token}}, True)

    def get_token(self):
        configs = self.db['configs']
        doc = configs.find_one({'key': 'token'})
        return doc['value']

    def get_trade_cal(self):
        key = 'trade_cal'
        coll = self.db[key]
        records = list(coll.find())
        return pd.DataFrame(records).sort_values(by='cal_date')

    def get_daily(self, code):
        df = self.query_by_api(api_name='daily', query={'ts_code': code}, limit_time=60 / 200, try_download=True)
        # key = 'daily'
        # # 如果数据库不存在该数据，尝试下载储存
        # coll = self.db[key]
        # if coll.count_documents({'ts_code': code}) == 0:
        #     df = self.pro.daily(ts_code=code)
        #     records = df.to_dict(orient='records')
        #     if len(records)>0:
        #         ids = coll.insert_many(records).inserted_ids
        #         print(f'{len(ids)} inserted. {code}')
        # else:
        #     records = list(self.db[key].find({'ts_code': code}))
        #     df = pd.DataFrame(records)
        #     del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_adj_factor(self, code):
        df = self.query_by_api(api_name='adj_factor', query={'ts_code': code}, limit_time=60 / 200, try_download=True)
        # key = 'adj_factor'
        # coll = self.db[key]
        # if coll.count_documents({'ts_code': code}) == 0:
        #     df = self.pro.adj_factor(ts_code=code)
        #     records = df.to_dict(orient='records')
        #     ids = coll.insert_many(records).inserted_ids
        #     print(f'{len(ids)} inserted. {code}')
        # else:
        #     records = list(self.db[key].find({'ts_code': code}))
        #     df = pd.DataFrame(records)
        #     del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_daily_basic(self, code):
        df = self.query_by_api(api_name='daily_basic', query={'ts_code': code}, limit_time=60 / 200, try_download=True)
        # key = 'daily_basic'
        # # 如果数据库不存在该数据，尝试下载储存
        # coll = self.db[key]
        # if coll.count_documents({'ts_code': code}) == 0:
        #     df = self.pro.daily_basic(ts_code=code)
        #     records = df.to_dict(orient='records')
        #     ids = coll.insert_many(records).inserted_ids
        #     print(f'{len(ids)} inserted. {code} {key}')
        # else:
        #     records = list(self.db[key].find({'ts_code': code}))
        #     df = pd.DataFrame(records)
        #     del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_index_daily(self, index_code):
        df = self.query_by_api(api_name='index_daily', query={'ts_code': index_code}, limit_time=60 / 200,
                               try_download=True)
        # key = 'index_daily'
        # # 如果数据库不存在该数据，尝试下载储存
        # coll = self.db[key]
        # if coll.count_documents({'ts_code': index_code}) == 0:
        #     df = self.pro.index_daily(ts_code=index_code)
        #     records = df.to_dict(orient='records')
        #     ids = coll.insert_many(records).inserted_ids
        #     print(f'{len(ids)} inserted. {index_code} {key}')
        # else:
        #     records = list(self.db[key].find({'ts_code': index_code}))
        #     df = pd.DataFrame(records)
        #     del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_index_dailybasic(self, index_code):
        df = self.query_by_api(api_name='index_dailybasic', query={'ts_code': index_code}, limit_time=60 / 200,
                               try_download=True)
        # key = 'index_dailybasic'
        # # 如果数据库不存在该数据，尝试下载储存
        # coll = self.db[key]
        # if coll.count_documents({'ts_code': index_code}) == 0:
        #     df = self.pro.index_dailybasic(ts_code=index_code)
        #     b_flag = len(df) > 0
        #     while (b_flag):
        #         date_min = df['trade_date'].min()
        #         temp_df = self.pro.index_dailybasic(ts_code=index_code, end_date=date_min)
        #         temp_df = temp_df[temp_df['trade_date'] < date_min]
        #         b_flag = len(temp_df) > 0
        #         if b_flag:
        #             df = df.append(temp_df).reset_index(drop=True)
        #
        #     if len(df) == 0:
        #         return None
        #
        #     records = df.to_dict(orient='records')
        #     ids = coll.insert_many(records).inserted_ids
        #     print(f'{len(ids)} inserted. {index_code} {key}')
        # else:
        #     records = list(self.db[key].find({'ts_code': index_code}))
        #     df = pd.DataFrame(records)
        #     del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_index_weight(self, index_code, trade_date):
        key = 'index_weight'
        coll = self.db[key]
        query = {'index_code': index_code, 'trade_date': trade_date}
        # 如果数据库不存在该数据，尝试下载当天成份
        if coll.count_documents(query) == 0:
            df = self.pro.index_weight(index_code=index_code, end_date=trade_date)
            # 查询的交易日当天没有成份变化数据，取最大值，如果数据库不存在该日期数据，插入保存
            if len(df) > 0:
                end_date = df['trade_date'].max()
                df = df[df['trade_date'] == end_date]
                df['trade_date'] = trade_date

                records = df.to_dict(orient='records')
                ids = coll.insert_many(records).inserted_ids
                print(f'{len(ids)} inserted. {index_code} {end_date} {key}')
            else:
                df = None
        else:
            records = list(coll.find(query))
            df = pd.DataFrame(records)
            del df['_id']
        return df

    def query_by_api(self, api_name, query=None, limit_time=1.0, try_download=False):
        coll = self.db[api_name]

        if query is None:
            query = {}

        if coll.count_documents(query) == 0:
            if try_download:
                df = self.pro.query(api_name=api_name, **query)
                time.sleep(limit_time)
                if len(df) > 0:
                    records = df.to_dict(orient='records')
                    ids = coll.insert_many(records).inserted_ids
                    print(f'{len(ids)} inserted. {api_name} {query}')
            else:
                df = None
        else:
            records = list(coll.find(query))
            df = pd.DataFrame(records)
            if '_id' in df.columns:
                del df['_id']
        return df

    def get_stock_basic(self):
        df = self.query_by_api(api_name='stock_basic')
        if df is not None:
            df = df.set_index('ts_code', drop=False)
        return df


if __name__ == '__main__':
    # TsUpdate().fix_all_financial(max_report_date='20180930')
    TsUpdate().run_daily_update()
