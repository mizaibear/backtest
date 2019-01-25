import pandas as pd
import pymongo
import tushare as ts


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
        key = 'daily'
        # 如果数据库不存在该数据，尝试下载储存
        coll = self.db[key]
        if coll.count_documents({'ts_code': code}) == 0:
            df = self.pro.daily(ts_code=code)
            records = df.to_dict(orient='records')
            ids = coll.insert_many(records).inserted_ids
            print(f'{len(ids)} inserted. {code}')
        else:
            records = list(self.db[key].find({'ts_code': code}))
            df = pd.DataFrame(records)
            del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_adj_factor(self, code):
        key = 'adj_factor'
        coll = self.db[key]
        if coll.count_documents({'ts_code': code}) == 0:
            df = self.pro.adj_factor(ts_code=code)
            records = df.to_dict(orient='records')
            ids = coll.insert_many(records).inserted_ids
            print(f'{len(ids)} inserted. {code}')
        else:
            records = list(self.db[key].find({'ts_code': code}))
            df = pd.DataFrame(records)
            del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_daily_basic(self, code):
        key = 'daily_basic'
        # 如果数据库不存在该数据，尝试下载储存
        coll = self.db[key]
        if coll.count_documents({'ts_code': code}) == 0:
            df = self.pro.daily_basic(ts_code=code)
            records = df.to_dict(orient='records')
            ids = coll.insert_many(records).inserted_ids
            print(f'{len(ids)} inserted. {code} {key}')
        else:
            records = list(self.db[key].find({'ts_code': code}))
            df = pd.DataFrame(records)
            del df['_id']

        return df.set_index('trade_date', drop=False).sort_index()

    def get_index_daily(self, index_code):
        key = 'index_daily'
        # 如果数据库不存在该数据，尝试下载储存
        coll = self.db[key]
        if coll.count_documents({'ts_code': index_code}) == 0:
            df = self.pro.index_daily(ts_code=index_code)
            records = df.to_dict(orient='records')
            ids = coll.insert_many(records).inserted_ids
            print(f'{len(ids)} inserted. {index_code} {key}')
        else:
            records = list(self.db[key].find({'ts_code': index_code}))
            df = pd.DataFrame(records)
            del df['_id']

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
