import numpy as np
import pandas as pd


def ma(series, n):
    return series.rolling(window=n, min_periods=1).mean()


def ema(series, n):
    return series.ewm(span=n).mean()


def sma(series, n, m):
    alpha = m / n
    return series.ewm(alpha=alpha).mean()


def at_last_condition(series, con_series, shift=0):
    return series[con_series].shift(shift).reindex(series.index).fillna(method='ffill').fillna(series.values[0])


def cross(series1, series2):
    return (ref(series1, 1) < ref(series2, 1)) & (series1 > series2)


def bars_last(con_series):
    df = pd.DataFrame(con_series)
    df['__number__'] = df.reset_index().index
    df['__true_num__'] = df[con_series].reindex(df.index)['__number__'].fillna(method='ffill')
    df['__bars_last__'] = (df['__number__'] - df['__true_num__'])  # .fillna(-1)

    return df['__bars_last__']


def hhv(series, n):
    return series.rolling(window=n, min_periods=1).max()


def llv(series, n):
    return series.rolling(window=n, min_periods=1).min()


def ref(series, n):
    return series.shift(n)


def series_max(s1, s2):
    val = s1.where(s1 >= s2).fillna(0) + s2.where(s2 > s1).fillna(0)
    return val.replace(0, np.NaN)


def series_min(s1, s2):
    val = s2.where(s1 >= s2).fillna(0) + s1.where(s2 > s1).fillna(0)
    return val.replace(0, np.NaN)


def series_count(con, n):
    return con.where(con == True).rolling(window=n, min_periods=1).count()


def series_max_draw(s):
    return (s - s.expanding().max()).expanding().min()


def series_if(s, con, else_result):
    return s.where(con).fillna(else_result)


def calc_kelly(win_pct, wl_rate):
    kelly = (win_pct * (wl_rate + 1) - 1) / wl_rate
    return kelly


def calc_atr(kline, n):
    ref_c = kline['close'].shift(1)
    df1 = kline['high'] - kline['low']
    df2 = np.abs(ref_c - kline['high'])
    df3 = np.abs(ref_c - kline['low'])
    mtr = pd.concat([df1, df2, df3], axis=1).max(axis=1)
    return ma(mtr, n)


def calc_corr(data1, data2, date_list):
    data1 = data1.reindex(index=date_list).fillna(method='ffill').fillna(method='bfill')
    data2 = data2.reindex(index=date_list).fillna(method='ffill').fillna(method='bfill')
    zf1 = data1['close'].pct_change()
    zf2 = data2['close'].pct_change()
    return zf1.corr(zf2)


def calc_beta(df, bm_df, date_list):
    df = df.reindex(index=date_list).fillna(method='ffill').fillna(method='bfill')
    bm_df = bm_df.reindex(index=date_list).fillna(method='ffill').fillna(method='bfill')
    data_zf = df['close'].pct_change()
    bm_zf = bm_df['close'].pct_change()
    return data_zf.cov(bm_zf) / np.power(bm_zf.var(), 2)


def calc_rsi(series, n):
    cd = series - series.shift(1)
    return sma(cd.where(cd > 0).fillna(0), n, 1) / sma(np.abs(cd), n, 1) * 100


def calc_dhbl(series, mid_value=0):
    hd = series == ref(hhv(series, 5), -2)
    dd = series == ref(llv(series, 5), -2)

    lhd1 = at_last_condition(series, hd, 0)
    lhd2 = at_last_condition(series, hd, 1)
    lhd3 = at_last_condition(series, hd, 2)

    ldd1 = at_last_condition(series, dd, 0)
    ldd2 = at_last_condition(series, dd, 1)
    ldd3 = at_last_condition(series, dd, 2)

    hbl = hd & (lhd2 > mid_value) & (((lhd1 < lhd3) & (lhd3 > mid_value)) | ((lhd1 < lhd2) & (lhd3 < lhd2)))
    dbl = dd & (ldd2 < mid_value) & (((ldd1 > ldd3) & (ldd3 < mid_value)) | ((ldd1 > ldd2) & (ldd3 > ldd2)))
    return dbl, hbl


def calc_delta_filter(series, n):
    delta = 2.58 * series.rolling(window=n, min_periods=1).std()
    ss = llv(series + delta, n)
    sl = hhv(series - delta, n)
    return ss, sl


def calc_semi_pct_delta(series, n):
    pct = series.pct_change().fillna(0)
    mean_pct = pct.rolling(window=n, min_periods=1).mean()
    pos_pct = series_if(pct, pct > mean_pct, mean_pct)
    neg_pct = series_if(pct, pct < mean_pct, mean_pct)
    pos_pct_delta = pos_pct.rolling(window=n, min_periods=1).std() * 2.58
    neg_pct_delta = neg_pct.rolling(window=n, min_periods=1).std() * 2.58
    return pos_pct_delta, neg_pct_delta


def calc_semi_delta(series, n):
    pos_pct_delta, neg_pct_delta = calc_semi_pct_delta(series, n)
    pos_delta = pos_pct_delta * series
    neg_delta = neg_pct_delta * series
    return pos_delta, neg_delta


def rolling_zscore(series, n):
    std = series.rolling(window=n, min_periods=1).std()
    mean = series.rolling(window=n, min_periods=1).mean()
    return (series - mean) / std


def rolling_percentage(series, n):
    roll_min = series.rolling(window=n, min_periods=1).min()
    roll_max = series.rolling(window=n, min_periods=1).max()
    return (series - roll_min) / (roll_max - roll_min)


def expanding_zscore(series):
    std = series.expanding().std()
    mean = series.expanding().mean()
    return (series - mean) / std


def expanding_percentage(series):
    exp_min = series.expanding().min()
    exp_max = series.expanding().max()
    return (series - exp_min) / (exp_max - exp_min)


def fx(series, vfunc, top_distance=2):
    return (series == ref(vfunc(series, top_distance * 2 + 1), -top_distance)) & (series != series.shift(1))


def calc_fma(series, window=20, top_distance=2):
    if top_distance < 1 or type(top_distance) is not int:
        raise TypeError('top_distance must be int and larger than zero.')

    sfc = fx(series, hhv, top_distance)
    xfc = fx(series, llv, top_distance)
    hc = at_last_condition(series, sfc)
    lc = at_last_condition(series, xfc)
    value = (ma(hc, window) + ma(lc, window)) / 2
    return value


def big_period_into_small_index(small, big):
    union = small.append(big).sort_index()
    union = union.groupby(union.index).first()
    return big.reindex(index=union.index).fillna(method='ffill').reindex(small.index)


def calc_tunnel_signal(series, buy_con, sell_con):
    buy_signal = series.where(buy_con).fillna(0)
    sell_signal = series.where(sell_con).fillna(0)
    union_signal = buy_signal + sell_signal
    return union_signal.replace({0: np.NaN}).fillna(method='ffill').fillna(series)


def calc_in_tunnel_trend(true_con, false_con):
    true_signal = true_con.where(true_con)
    false_signal = true_con.where(false_con)
    combined_signal = true_signal.fillna(false_signal).fillna(method='ffill')
    combined_signal = combined_signal == 1
    return combined_signal


def weight_ma(weight_series, data_series, n=0):
    '''
    :param pd.Seires weight_series:
    :param pd.Seires data_series:
    :param int n:
    :return:pd.Series
    '''
    wdata = weight_series * data_series
    if n == 0:  # expanding
        return wdata.expanding().sum() / weight_series.expanding().sum()
    else:  # rolling
        return wdata.rolling(window=n, min_periods=1).sum() / weight_series.rolling(window=n, min_periods=1).sum()


def weight_ma_score(weight_series, data_series, low_series, n_list):
    malist = [weight_ma(weight_series, data_series, n) for n in n_list]
    conlist = []
    zero = pd.Series(0, index=data_series.index)
    for i, n in enumerate(n_list):
        if i > 0:
            conlist.append((low_series + malist[i - 1]) > (2 * malist[i]))
        else:
            conlist.append(low_series > malist[i])

    score = None
    for i, n in enumerate(n_list):
        value = pd.Series(n, index=data_series.index)
        large_con = None
        j = i + 1
        while j < len(n_list):
            if large_con is None:
                large_con = conlist[j]
            else:
                large_con = large_con | conlist[j]
            j += 1
        if large_con is None:
            large_con = pd.Series(False, index=data_series.index)
        s = series_if(value, conlist[i], series_if(-value, large_con, zero))
        if score is None:
            score = s
        else:
            score += s
    return np.round(score / sum(n_list), 2)


def calc_pcore(kline, fin, value_core_col):
    core_ps = (fin[value_core_col] / fin['总股本']).reindex(kline.index).fillna(method='ffill').fillna(method='bfill')
    pcore = kline['o_close'] / core_ps.where(core_ps > 0)
    return pcore


def calc_valuation(pcore, min_periods=240, date_start=None):
    if date_start is not None:
        pcore = pcore.loc[date_start:]

    agg = pcore.expanding(min_periods=min_periods)
    mid = np.round(agg.mean(), 2).fillna(method='bfill')
    std = np.round(agg.std(), 2).fillna(method='bfill')
    top = (mid + std).fillna(method='bfill')
    bot = mid - std
    pcore_min = pcore.expanding().min()
    bot = bot.where(bot > pcore_min).fillna(pcore_min)
    bot = bot.fillna(method='bfill').fillna(method='ffill')
    return mid, top, bot
