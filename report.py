import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 设置中文字体，不然画图时中文会乱码
mpl.rcParams['font.family'] = 'sans-serif'
mpl.rcParams['font.sans-serif'] = 'SimHei'


def show(df):
    fig, ax = plt.subplots(figsize=(16, 9))  # 指定画布大小
    # df.index = pd.to_datetime(df.index)
    df.plot(ax=ax)  # 调用DataFrame的plot方法画出折线图
    plt.show()


# 总收益
def total_return(values):
    return values.values[-1] / values.values[0] - 1


# 年化收益
def annual_return(values):
    total = values.values[-1] / values.values[0]
    return np.power(total, 252 / len(values)) - 1


# 最大回撤
def max_drawdown(values):
    exp_max = values.expanding().max()
    drawdown = (values / exp_max) - 1
    return drawdown.min()  # 最大回撤是负数，所以取最小值


# 夏普比率
def sharpe_ratio(values):
    daily_return = values.pct_change(1)
    return daily_return.mean() / daily_return.std() * np.sqrt(252)


# 年化换手
def annual_change(records):
    daily_change_rate = records['成交额'] / records['净值']
    return daily_change_rate.mean() * 252


# 总费用占净值比
def total_commision_rate(records):
    return records['手续费'].sum() / records['净值'].iloc[-1]


# 胜率
def winning_rate(records):
    rate = records['盈利卖出笔数'] / (records['亏损卖出笔数'] + records['盈利卖出笔数'])
    return rate.iloc[-1]


# 总盈亏比
def total_win_to_lose(records):
    rate = records['卖出盈利额'] / np.abs(records['卖出亏损额'])
    return rate.iloc[-1]


# 平均盈亏比
def avg_win_to_lose(records):
    avg_win = records['卖出盈利额'] / records['盈利卖出笔数']
    avg_lose = np.abs(records['卖出亏损额']) / records['亏损卖出笔数']
    rate = avg_win / avg_lose
    return rate.iloc[-1]


class Report(object):
    def __init__(self, records):
        # 初始化报告对象
        self._records = records
        self._summary = self.create_summary()  # 生成汇总

    def __str__(self):
        items = [f'{k}:{v}' for k, v in self.outputs().items()]
        return '\n'.join(items)

    def outputs(self):
        # 生成格式化字符串
        return {
            '交易天数': f'{len(self._records)}',
            '总收益%': f'{self._summary["总收益率"]*100:.2f}',
            '相对基准收益%': f'{(self._summary["总收益率"]-self._summary["基准收益率"])*100:.2f}',
            '年化收益%': f'{self._summary["年化收益率"]*100:.2f}',
            '最大回撤%': f'{self._summary["最大回撤"]*100:.2f}',
            '胜率%': f'{self._summary["胜率"]*100:.2f}',
            '总盈亏比': f'{self._summary["总盈亏比"]:.2f}',
            '每笔盈亏比': f'{self._summary["每笔盈亏比"]:.2f}',
            '夏普比值': f'{self._summary["夏普比值"]:.2f}',
            '基准收益%': f'{self._summary["基准收益率"]*100:.2f}',
            '年化换手%': f'{self._summary["年化换手"]*100:.2f}',
            '手续费占净值比%': f'{self._summary["手续费占净值比"]*100:.2f}'
        }

    def show(self):
        print(self)  # 打印绩效信息
        records = self._records[['净值', 'benchmark']]
        records /= records.iloc[0]
        show(records)  # 显示资金曲线图

    def create_summary(self):
        values = self._records['净值']
        benchmark = self._records['benchmark']
        summary = {}
        summary['总收益率'] = total_return(values)
        summary['年化收益率'] = annual_return(values)
        summary['最大回撤'] = max_drawdown(values)
        summary['夏普比值'] = sharpe_ratio(values)
        summary['基准收益率'] = total_return(benchmark)
        summary['基准年化收益率'] = annual_return(benchmark)
        summary['基准最大回撤'] = max_drawdown(benchmark)
        summary['年化换手'] = annual_change(self._records)
        summary['手续费占净值比'] = total_commision_rate(self._records)
        summary['胜率'] = winning_rate(self._records)
        summary['总盈亏比'] = total_win_to_lose(self._records)
        summary['每笔盈亏比'] = avg_win_to_lose(self._records)
        return summary

    def show_hedge(self):
        records = self._records / self._records.iloc[0]
        records['对冲净值'] = records['净值'] / records['benchmark']
        show(records[['对冲净值']])


def compare_reports(reports):
    # reports接受一个名字映射的报告字典
    items = []
    charts = {}
    for key, report in reports.items():
        item = {'title': key}
        # 直接把报告的格式化字符串更新进去
        item.update(report.outputs())
        items.append(item)
        charts[key] = report._records['净值']
    summary = pd.DataFrame(items).set_index('title')
    chart_df = pd.DataFrame(charts)
    # 指数化，不同量级的数据也可以同图对比走势
    chart_df /= chart_df.iloc[0]

    return summary, chart_df
