import talib

available_cash = 50000
risk_ratio = 0.1
ATR_timeperiod = 14

buylist = ['600900.XSHG', '600004.XSHG', '002555.XSHE', '600398.XSHG', '002032.XSHE']

def fun_getATR(stock):
  try:
    hStock = attribute_history(stock, ATR_timeperiod + 10, '1d', ('close','high','low') , df = False)
  except:
    log.info('%s 获取历史数据失败' %stock)
    return 0
  # 去极值，然后送入ATR函数，细致处理
  close_ATR = hStock['close']
  high_ATR = hStock['high']
  low_ATR = hStock['low']
  try:
    ATR = talib.ATR(high_ATR, low_ATR, close_ATR, timeperiod = ATR_timeperiod)
  except:
    return 0
  # 返回前一个ATR值
  return ATR[-1]

def ATR_Position(buylist):
  # 每次调仓，用 positionAdjustFactor(总资产*损失比率) 来控制承受的风险
  positionAdjustValue = available_cash * risk_ratio # 最大损失的资金量
  Adjustvalue_per_stock = float(positionAdjustValue) / len(buylist) # 个股能承受的最大损失资金量（等分）
  
  # 取到buylist个股名单上一个1分钟收盘价，df=False不返回df数据类型
  hStocks = history(1, '1m', 'close', buylist, df = False)
  # 建立一个dataframe：risk_value
  # 第一列是buylist股票代码，第二列是risk_value
  risk_value = {}
  # 计算个股动态头寸risk_value
  for stock in buylist:
    # curATR是2倍日线ATR值，输出转化成浮点数
    curATR = 2 * float(fun_getATR(stock))
    if curATR != 0 :
      # 拆解分析：当前价 * 个股能承受的最大损失资金量是【个股持仓价值】
      # 如果不除以curATR，说明不进行个股头寸波动性变化
      # ATR越大，个股risk_value越小；ATR越小，个股risk_value越大
      # 说明波动性和个股持仓价值应该负相关（进行个股持仓量动态分配），这符合资金管理或者资产配置原则
      risk_value[stock] = hStocks[stock] * Adjustvalue_per_stock / curATR
    else:
      risk_value[stock] = 0
  # 到此为止计算出个股应该持有的风险价值
  return risk_value

# 均分计算金额
stock_values = ATR_Position(buylist)
total_value = 0
for stock in stock_values:
  total_value += stock_values[stock][0]
print(int((stock_values['600900.XSHG'][0] / total_value) * available_cash))
print(int((stock_values['600004.XSHG'][0] / total_value) * available_cash))