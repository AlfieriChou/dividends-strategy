# 克隆自聚宽文章：https://www.joinquant.com/post/46375
# 标题：深套后的自救，网格配对策略
# 作者：Glj01243939

# 本策略是对中、农、工、商四大银行的股票进行分析，
#对比各品种日内分钟线上收盘价相对于昨日收盘价的涨跌情况，进行纯多头配对轮询策略，
#只买进低涨幅或高跌幅的股票，而无做空的行为，即只执行统计套利中的做多部分。

import math
import pandas as pd
import numpy as np
import statsmodels.api as sm
import scipy.stats as scs
import scipy.optimize as sco
import talib as tl
from datetime import timedelta


bank_stocks = ['601398.XSHG', '601288.XSHG','601939.XSHG','601988.XSHG']  # 设置银行股票 工行，建行
   
# 初始化参数
def initialize(context):
  # 初始化此策略
  # 设置要操作的股票池为空，每天需要不停变化股票池
  set_universe([])
  g.riskbench = '000300.XSHG'
  #设置手续费
  set_order_cost(OrderCost(open_commission = 0.0002, close_commission = 0.00122, min_commission = 5),type = 'stock') 
  #滑点设置为0
  set_slippage(FixedSlippage(0.00)) 
  set_option('use_real_price', True)
  # 设置基准对比为沪深300指数
  g.is_run=False
  g.inter = 0.005
    
# 每天交易前调用
def before_trading_start(context):
  #获取四大行前一日的收盘价，以字典形式存储
  g.df_last = history(1, unit = '1d', field = 'close', security_list = bank_stocks, df = False, skip_paused = True, fq = 'pre')
  log.error('df_last:',g.df_last)
  g.is_run = False

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
  if g.is_run:
    return
  raito = []
  #求当下四大行相对于昨日收盘价的价格变动率
  for code in bank_stocks:
    r = (data[code].close) / g.df_last[code][0] 
    raito.append(r)
    log.info(code, data[code].close, '/', g.df_last[code][0] , '=', r)
  log.error('rate:', raito)
  #在未持仓的情况下    
  if not context.portfolio.positions.keys():
    #最大价格变动率与最小变动率进行比较
    if max(raito) - min(raito) > g.inter:
      #获取最小变化率的索引，以便获取对应的股票
      min_index = raito.index(min(raito))
      log.error('全仓买入', bank_stocks[min_index])
      g.is_run=True
      order_value(bank_stocks[min_index], context.portfolio.total_value)

  #若持有仓位        
  else:
    #获取现持仓的股票代码
    code = list(context.portfolio.positions.keys())[0]
    hold = context.portfolio.positions[code]
    if hold.closeable_amount <= 0:
      return
    #获取持仓股票在bank_stocks列表中的索引位置
    index = bank_stocks.index(code)
    #持仓股票的价格变动率与最小变动率进行比较
    if raito[index] - min(raito) > g.inter:
      log.error('全仓卖出',code)
      order_target(code, 0)
      g.is_run = True
      #min_index = raito.index(min(raito))
      #order_value(bank_stocks[min_index], context.portfolio.total_value)
