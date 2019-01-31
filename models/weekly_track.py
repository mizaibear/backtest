import pandas as pd

from backtest import get_datasource


def get_value_tracks(index_code, value_col='pb'):
    data = get_datasource()
    df = data.get_index_dailybasic(index_code)
    item = {}
    value = df[value_col]
    item[value_col] = value
    tr = df['turnover_rate']
    for n in [55, 144, 233]:
        item[f'加权平均_{n}'] = (value * tr).rolling(window=n).sum() / tr.rolling(window=n).sum()
    item['加权平均_累计'] = (value * tr).expanding().sum() / tr.expanding().sum()
    item['平均估值'] = value.expanding().mean()
    exp_std = value.expanding().std()
    item['高估区间'] = item['平均估值'] + exp_std
    item['低估区间'] = item['平均估值'] - exp_std
    return pd.DataFrame(item)


def get_summary(index_codes, value_col='pb'):
    item = {}
    for code in index_codes:
        value_df = get_value_tracks(code, value_col)
        item[code] = value_df.iloc[-1]

    return pd.DataFrame(item).T


if __name__ == '__main__':
    df = get_summary(['399300.SZ', '000001.SH', '000016.SH', '000905.SH', '399006.SZ'])
    get_datasource().save_cache()
    print(df)
