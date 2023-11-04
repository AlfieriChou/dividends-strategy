# 克隆自聚宽文章：https://www.joinquant.com/post/33960
# 标题：EBIT/EV-高股息-Fscore
# 作者：乌龟赛跑7

# 导入函数库
from jqdata import *
from jqdata import finance
import datetime
import pandas as pd
from jqfactor import get_factor_values
from jqlib.technical_analysis import *
from wechat_msg import *
from email_msg import *
from realtime_wechat_msg import *
from monitor_wechat_msg import *
import datetime

#显示所有列
pd.set_option('display.max_columns', None)

# 初始化函数，设定基准等等
def initialize(context):
  # 设定沪深300作为基准
  set_benchmark('000300.XSHG')
  # 开启动态复权模式(真实价格)
  set_option('use_real_price', True)
  # 输出内容到日志 log.info()
  log.info('初始函数开始运行且全局只运行一次')
  # 过滤掉order系列API产生的比error级别低的log
  # log.set_level('order', 'error')

  ### 股票相关设定 ###
  # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
  set_order_cost(
    OrderCost(
      close_tax = 0.001,
      open_commission = 0.0003,
      close_commission = 0.0003,
      min_commission = 5
    ),
    type = 'stock'
  )
  # 开盘前运行
  run_weekly(before_market_open, 1, time = 'before_open', reference_security = '000300.XSHG')
  #开盘时运行
  run_weekly(market_open, 1, time = 'open', reference_security = '000300.XSHG')
  # 收盘后运行
  run_weekly(after_market_close, 1, time = 'after_close', reference_security = '000300.XSHG')
  run_daily(print_position_info, '15:10:00')
  run_daily(filter_stock_list, '7:45:00')
  run_daily(filter_stock_list, '13:30:00')
  #定义交易月份
  g.Transfer_date = list(range(1, 13, 1))
  g.stock_num = 5
  run_time(5)

def run_time(x):
  #获取日内交易时间并剔除15:00
  list = get_price('000300.XSHG', count = 240, frequency = '1m').index.tolist()[:-1]  
  times = [str(t)[-8:] for t in list]  #提取交易时间
  times.insert(0, '09:30:01')
  for time in times[::x]:
    run_daily(run_monitor_schedule, time)

def run_monitor_schedule(context):
  configFile = read_file('fscore_config.json')
  config = json.loads(configFile)
  current_data = get_current_data()
  if config['is_send_wechat_msg']:
    send_realtime_wechat_msg(context, config, current_data)
    send_monitor_wechat_msg(context, config, current_data)
    send_realtime_wechat_msg(context, config['outside_config'], current_data)

# 打印每日持仓信息
def print_position_info(context):
  #打印当天成交记录
  trades = get_trades()
  for _trade in trades.values():
    print('成交记录：'+str(_trade))
  #打印账户信息
  for position in list(context.portfolio.positions.values()):
    securities = position.security
    cost = position.avg_cost
    price = position.price
    ret = 100 * (price / cost - 1)
    value = position.value
    amount = position.total_amount    
    print('代码:{}'.format(securities))
    print('成本价:{}'.format(format(cost, '.2f')))
    print('现价:{}'.format(price))
    print('收益率:{}%'.format(format(ret, '.2f')))
    print('持仓(股):{}'.format(amount))
    print('市值:{}'.format(format(value, '.2f')))
    print('———————————————————————————————————')
  print('———————————————————————————————————————分割线————————————————————————————————————————')

def get_PE_market_cap(stock_list):
  df = get_fundamentals(
    query(
      valuation.code
    ).filter(
      valuation.code.in_(stock_list),
      valuation.market_cap > 100,
      valuation.pe_ratio < 20
    )
  )
  return list(df.code)

# 过滤行业
def get_stock_industry(securities, watch_date, level = 'sw_l1', method = 'industry_name'): 
  industry_dict = get_industry(securities, watch_date)
  industry_ser = pd.Series({k: v.get(level, {method: np.nan})[method] for k, v in industry_dict.items()})
  industry_df = industry_ser.to_frame('industry')
  return industry_df

def filter_industry(industry_df, select_industry, level = 'sw_l1', method = 'industry_name'):
  filter_df = industry_df.query('industry != @select_industry')
  filter_list = filter_df.index.tolist()
  return filter_list

def get_stock(context):
  #获取前一个交易日的日期
  previous_day = context.previous_date
  # 获取要操作行业的股票代码列表
  g.security = get_all_securities(
    types = ['stock'],
    date = previous_day
  ).index.tolist()
  #筛选上市日期为5年前的股票；
  if previous_day.year >= 2009:
    days = 1800
  else:
    days = 365
  stock_list = start_date_filter(
    watch_date = previous_day,
    security = g.security,
    days = days
  )
  #剔除ST、停牌、退市的股票
  stock_list = get_ST_stock_out(security = stock_list, watch_date = previous_day).index.tolist()
  #过滤市值小于100亿并且PE大于20的个股
  stock_list = get_PE_market_cap(stock_list)
#   #过滤行业
#   industry_df = get_stock_industry(stock_list, context.previous_date)
#   choice = filter_industry(industry_df, config['exclude_industry_filter_list'])
  #获取EBIT/EV最高的股票
  data = get_EBIT_EV(security = stock_list, watch_date = previous_day)
  stock_list = data.index.tolist()[0:200]
  #获取股息5年最高的股票
  data = get_DP(security = stock_list, watch_date = previous_day, days = 1800)
  stock_list = data.index.tolist()[0:100]
  #获取最近一股息率最高的股票
  data = get_DP(security = stock_list, watch_date = previous_day, days = 365)
  stock_list = data.index.tolist()[0:50]
  #获取F-score评分最高的股票
  data = get_F_socre_and_rank(security = stock_list, watch_date = previous_day)
  stock_list = data.index.tolist()[0:20]
  
  return stock_list[:g.stock_num]

def filter_stock_list(context):
  configFile = read_file('fscore_config.json')
  config = json.loads(configFile)
  target_list = get_stock(context)
  current_data = get_current_data()
  log.info('预选列表：' + str(target_list))
  if config['isSendEmail']:
    send_email_msg(context, config, target_list, current_data)
    send_daily_target_wechat_msg(context, config, target_list, current_data)
    send_daily_hold_wechat_msg(context, config)

## 开盘前运行函数
def before_market_open(context):
  g.buy_list = get_stock(context)

## 开盘时运行函数
def market_open(context):
  log.info('函数运行时间(market_open):' + str(context.current_dt.time()))

  #获取当前交易日期的月份
  current_month = context.current_dt.month
  if current_month in g.Transfer_date:
    #买入股票列表
    buy_list = g.buy_list
    #简记当前组合
    p = context.portfolio
    # 获取当前时间数据
    cur_data = get_current_data()
    #获取当前交易日期
    current_day = context.current_dt
    # 卖出股票
    for code in list(p.positions.keys()):
      if code not in buy_list:
        if cur_data[code].paused:
          continue
        # 卖出股票
        order_target_value(code, 0)
      else:
        open_price = cur_data[code].day_open
        num_to_target = (p.total_value / len(buy_list)) / open_price // 100 * 100
        order_target(code, num_to_target)

    #买入股票
    for code in buy_list:
      if code not in p.positions:
        if cur_data[code].paused:
          continue
        open_price = cur_data[code].day_open
        num_to_buy = (p.total_value / len(buy_list)) / open_price // 100 * 100
        # 买入股票
        order_target(code, num_to_buy)

## 收盘后运行函数
def after_market_close(context):
  #获取当前交易的日期
  current_month = context.current_dt.month
  p = context.portfolio
  pos_level = p.positions_value / p.total_value
  record(pos_level = pos_level)

# 定义获取上市公司首发上市的日期
def start_date_filter(watch_date, security, days):
  #获取上市日期、证券简称；
  q0 = query(
    finance.STK_LIST.code,
    finance.STK_LIST.name,
    finance.STK_LIST.start_date
  ).filter(
    finance.STK_LIST.code.in_(security)
  )
  df_start_date = finance.run_query(q0)
  #筛选条件：days以前上市的；
  days_ago = watch_date - datetime.timedelta(days = days)
  df_start_date1 = df_start_date[df_start_date['start_date'] < days_ago]
  df_start_date2 = df_start_date1.set_index('code')
  df_start_list = df_start_date2.index.tolist()
  return df_start_list

#定义一个函数，这个函数可以剔除ST类股票
def get_ST_stock_out(security, watch_date):
  data1 = get_extras('is_st', security_list = security, start_date = watch_date, end_date = watch_date, df = True)
  data2 = data1.T
  data2.columns = ['ST']
  data3 = data2[data2['ST'] == False]
  return data3

def get_EBIT_EV(security,watch_date):
  #获取相关数据并形成EBIT相关数据
  if len(security) >= 2000:
    EBIT_data1 = get_history_fundamentals(
      security = security[int(len(security) * 0):int(len(security) * 0.33)],
      fields = [
        indicator.adjusted_profit,
        income.income_tax_expense,
        income.financial_expense
      ], 
      watch_date = watch_date,
      count = 4,
      interval = '1q',
      stat_by_year = False
    )
    EBIT_data2 = get_history_fundamentals(
      security = security[int(len(security) * 0.33):int(len(security) * 0.66)],
      fields=[
        indicator.adjusted_profit,
        income.income_tax_expense,
        income.financial_expense
      ], 
      watch_date = watch_date,
      count = 4,
      interval = '1q',
      stat_by_year = False
    )
    EBIT_data3 = get_history_fundamentals(
      security = security[int(len(security) * 0.66):int(len(security) * 1)],
      fields = [
        indicator.adjusted_profit,
        income.income_tax_expense,
        income.financial_expense
      ], 
      watch_date = watch_date,
      count = 4,
      interval = '1q',
      stat_by_year = False
    )
    EBIT_data = pd.concat(
      [
        EBIT_data1,
        EBIT_data2,
        EBIT_data3
      ],
      axis = 0,
      sort = True
    )
  else:
    EBIT_data = get_history_fundamentals(
      security = security,
      fields = [
        indicator.adjusted_profit,
        income.income_tax_expense,
        income.financial_expense
      ], 
      watch_date = watch_date,
      count = 4,
      interval = '1q',
      stat_by_year = False
    )

  # EBIT_data.fillna(0,inplace=True)
  EBIT_data = EBIT_data.groupby('code').sum()
  EBIT_data['EBIT'] = EBIT_data['adjusted_profit'] + EBIT_data['income_tax_expense'] + EBIT_data['financial_expense']
  
  #获取市值以及负债相关数据
  factor_data = get_factor_values(
    securities = security,
    factors = ['market_cap', 'financial_liability', 'financial_assets'],
    end_date = watch_date,
    count = 1
  )

  df_factor_data = pd.concat(
    [
      factor_data['market_cap'].T,
      factor_data['financial_liability'].T,
      factor_data['financial_assets'].T
    ],
    axis = 1
  )
  col1 = ['market_cap', 'financial_liability', 'financial_assets']
  df_factor_data.columns = col1
  #计算EBIT/EV
  df_factor_data['EV'] = df_factor_data['market_cap'] + df_factor_data['financial_liability'] + df_factor_data['financial_assets']
  EBIT_EV = EBIT_data['EBIT'] / df_factor_data['EV']
  
    #以EBIT/EV进行排名
  EBIT_EV = EBIT_EV.sort_values(ascending = False)

  return EBIT_EV

def get_DP(security,watch_date,days):
  # 获取股息数据
  one_year_ago = watch_date - datetime.timedelta(days = days)
  q1 = query(
    finance.STK_XR_XD.a_registration_date,
    finance.STK_XR_XD.bonus_amount_rmb,
    finance.STK_XR_XD.code
  ).filter(
    finance.STK_XR_XD.a_registration_date>= one_year_ago,
    finance.STK_XR_XD.a_registration_date <= watch_date,
    finance.STK_XR_XD.code.in_(security[0:int(len(security) * 0.3)])
  )
  q2 = query(
    finance.STK_XR_XD.a_registration_date,
    finance.STK_XR_XD.bonus_amount_rmb,
    finance.STK_XR_XD.code
  ).filter(
    finance.STK_XR_XD.a_registration_date >= one_year_ago,
    finance.STK_XR_XD.a_registration_date <= watch_date,
    finance.STK_XR_XD.code.in_(security[int(len(security) * 0.3):int(len(security) * 0.6)])
  )
  q3 = query(
    finance.STK_XR_XD.a_registration_date,
    finance.STK_XR_XD.bonus_amount_rmb,
    finance.STK_XR_XD.code
  ).filter(
    finance.STK_XR_XD.a_registration_date >= one_year_ago,
    finance.STK_XR_XD.a_registration_date<=watch_date,
    finance.STK_XR_XD.code.in_(security[int(len(security) * 0.6):])
  )
  
  df_data1 = finance.run_query(q1)
  df_data2 = finance.run_query(q2)
  df_data3 = finance.run_query(q3)
  df_data = pd.concat([df_data1 ,df_data2, df_data3],axis = 0,sort = False)
  df_data.fillna(0,inplace = True)
  
  df_data = df_data.set_index('code')
  df_data = df_data.groupby('code').sum()
  
  #获取市值相关数据
  q01 = query(
    valuation.code,
    valuation.market_cap
  ).filter(
    valuation.code.in_(security)
  )
  BP_data = get_fundamentals(q01, date = watch_date)
  BP_data = BP_data.set_index('code')
  
  #合并数据
  data = pd.concat([df_data, BP_data],axis = 1,sort = False)
  data.fillna(0,inplace = True)
  data['股息率'] = (data['bonus_amount_rmb'] / 10000) / data['market_cap']
  data1 = data.sort_values(by = ['股息率'],ascending = False)
  
  return data1

def get_F_socre_and_rank(security, watch_date):
  one_year_ago = watch_date - datetime.timedelta(days=365)
  h = get_history_fundamentals(
    security,
    [
      indicator.adjusted_profit,
      balance.total_current_assets,
      balance.total_assets,
      balance.total_current_liability,
      balance.total_non_current_liability,
      cash_flow.net_operate_cash_flow,
      income.operating_revenue,
      income.operating_cost,
    ],
    watch_date = watch_date,
    count = 5
  )  # 连续的5个季度
  
  #去除历史数据不足5的情况
  not_enough_data = h.groupby('code').size() <  5
  not_enough_data = not_enough_data[not_enough_data]
  h = h[~h['code'].isin(not_enough_data.index)]
  
  def ttm_sum(x):
      return x.iloc[1:].sum()
  def ttm_avg(x):
    return x.iloc[1:].mean()
  def pre_ttm_sum(x):
    return x.iloc[:-1].sum()
  def pre_ttm_avg(x):
    return x.iloc[:-1].mean()
  def val_1(x):
    return x.iloc[-1]
  def val_2(x):
    return x.iloc[-2]
  
  # 扣非利润
  adjusted_profit_ttm = h.groupby('code')['adjusted_profit'].apply(ttm_sum)
  adjusted_profit_ttm_pre = h.groupby('code')['adjusted_profit'].apply(pre_ttm_sum)

  # 总资产平均
  total_assets_avg = h.groupby('code')['total_assets'].apply(ttm_avg)
  total_assets_avg_pre = h.groupby('code')['total_assets'].apply(pre_ttm_avg)

  # 经营活动产生的现金流量净额
  net_operate_cash_flow_ttm = h.groupby('code')['net_operate_cash_flow'].apply(ttm_sum)

  # 长期负债率: 长期负债/总资产
  long_term_debt_ratio = h.groupby('code')['total_non_current_liability'].apply(val_1) / h.groupby('code')['total_assets'].apply(val_1)
  long_term_debt_ratio_pre = h.groupby('code')['total_non_current_liability'].apply(val_2) / h.groupby('code')['total_assets'].apply(val_2)

  # 流动比率：流动资产/流动负债
  current_ratio = h.groupby('code')['total_current_assets'].apply(val_1) / h.groupby('code')['total_current_liability'].apply(val_1)
  current_ratio_pre = h.groupby('code')['total_current_assets'].apply(val_2) / h.groupby('code')['total_current_liability'].apply(val_2)

  # 营业收入
  operating_revenue_ttm = h.groupby('code')['operating_revenue'].apply(ttm_sum)
  operating_revenue_ttm_pre = h.groupby('code')['operating_revenue'].apply(pre_ttm_sum)

  # 营业成本
  operating_cost_ttm = h.groupby('code')['operating_cost'].apply(ttm_sum)
  operating_cost_ttm_pre = h.groupby('code')['operating_cost'].apply(pre_ttm_sum)
  
  # 1. ROA 资产收益率
  roa = adjusted_profit_ttm / total_assets_avg
  roa_pre = adjusted_profit_ttm_pre / total_assets_avg_pre

  # 2. OCFOA 经营活动产生的现金流量净额/总资产
  ocfoa = net_operate_cash_flow_ttm / total_assets_avg

  # 3. ROA_CHG 资产收益率变化
  roa_chg = roa - roa_pre

  # 4. OCFOA_ROA 应计收益率: 经营活动产生的现金流量净额/总资产 -资产收益率
  ocfoa_roa = ocfoa - roa

  # 5. LTDR_CHG 长期负债率变化 (长期负债率=长期负债/总资产)
  ltdr_chg = long_term_debt_ratio - long_term_debt_ratio_pre

  # 6. CR_CHG 流动比率变化 (流动比率=流动资产/流动负债)
  cr_chg = current_ratio - current_ratio_pre

  # 8. GPM_CHG 毛利率变化 (毛利率=1-营业成本/营业收入)
  gpm_chg = operating_cost_ttm_pre/operating_revenue_ttm_pre - operating_cost_ttm/operating_revenue_ttm

  # 9. TAT_CHG 资产周转率变化(资产周转率=营业收入/总资产)
  tat_chg = operating_revenue_ttm/total_assets_avg - operating_revenue_ttm_pre/total_assets_avg_pre
  
  # 7. 股票是否增发
  spo_list = list(set(finance.run_query(
    query(
      finance.STK_CAPITAL_CHANGE.code
    ).filter(
      finance.STK_CAPITAL_CHANGE.code.in_(security),
      finance.STK_CAPITAL_CHANGE.pub_date.between(one_year_ago, watch_date),
      finance.STK_CAPITAL_CHANGE.change_reason_id == 306004
    )
  )['code']))

  spo_score = pd.Series(True, index = security)
  
  if spo_list:
    spo_score[spo_list] = False
  
  # 计算得分total
  df_scores = pd.DataFrame(index=security)
  # 1
  df_scores['roa'] = roa > 0 
  # 2
  df_scores['ocfoa'] = ocfoa > 0
  # 3
  df_scores['roa_chg'] = roa_chg > 0
  # 4
  df_scores['ocfoa_roa'] = ocfoa_roa > 0
  # 5
  df_scores['ltdr_chg'] = ltdr_chg <= 0
  # 6
  df_scores['cr_chg'] = cr_chg > 0
  # 7
  df_scores['spo'] = spo_score
  # 8
  df_scores['gpm_chg'] = gpm_chg > 0
  # 9
  df_scores['tat_chg'] = tat_chg > 0

  # 合计
  df_scores = df_scores.dropna()
  
  for u in df_scores.columns:
    if df_scores[u].dtype == bool:
      df_scores[u] = df_scores[u].astype('int')

  df_scores['total'] = df_scores['roa'] + df_scores['ocfoa'] + df_scores['roa_chg'] + df_scores['ocfoa_roa'] + df_scores['ltdr_chg'] + df_scores['cr_chg'] + df_scores['spo'] + df_scores['gpm_chg'] + df_scores['tat_chg']
  df_scores = df_scores.sort_values(by = 'total',ascending = False)   

  return df_scores

# end
