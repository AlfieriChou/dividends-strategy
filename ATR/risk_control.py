import talib

class RiskControlStatus():
  RISK_WARNING = 1
  RISK_NORMAL = 2


class RiskControl(object):
  def __init__(self, symbol):
    self.symbol = symbol
    self.status = RiskControlStatus.RISK_NORMAL

  def check_for_ma_rate(self, period, ma_rate_min, ma_rate_max, show_ma_rate):
    ma_rate = self.compute_ma_rate(period, show_ma_rate)
    return (ma_rate_min < ma_rate < ma_rate_max)

  def compute_ma_rate(self, period, show_ma_rate):
    hst = get_bars(self.symbol, period, '1d', ['close'])
    close_list = hst['close']
    if (len(close_list) == 0):
      return -1.0

    if (math.isnan(close_list[0]) or math.isnan(close_list[-1])):
      return -1.0

    period = min(period, len(close_list))
    if (period < 2):
      return -1.0

    #ma = close_list.sum() / len(close_list)
    ma = talib.MA(close_list, timeperiod = period)[-1]
    ma_rate = hst['close'][-1] / ma
    if (show_ma_rate):
      print(ma, ma_rate)

    return ma_rate

  def check_for_rsi(self, period, rsi_min, rsi_max, show_rsi):
    hst = attribute_history(self.symbol, period + 1, '1d', ['close'])
    close = [float(x) for x in hst['close']]
    if (math.isnan(close[0]) or math.isnan(close[-1])):
      return False

    rsi = talib.RSI(np.array(close), timeperiod = period)[-1]
    if (show_rsi):
      print(RSI = max(0, (rsi - 50)))

    return (rsi_min < rsi < rsi_max)

  def check_for_benchmark_v1(self):
    could_trade_ma_rate = self.check_for_ma_rate(10000, 0.75, 1.50, True)

    could_trade = False
    if (could_trade_ma_rate):
      could_trade = self.check_for_rsi(90, 35, 99, False)
    else:
      could_trade = self.check_for_rsi(15, 50, 70, False)

    return could_trade

  def check_for_benchmark(self):
    ma_rate = self.compute_ma_rate(1000, False)
    if (ma_rate <= 0.0):
      return False

    if (self.status == RiskControlStatus.RISK_NORMAL):
      if ((ma_rate > 2.5) or (ma_rate < 0.30)):
        self.status = RiskControlStatus.RISK_WARNING
    elif (self.status == RiskControlStatus.RISK_WARNING):
      if (0.35 <= ma_rate <= 0.7):
        self.status = RiskControlStatus.RISK_NORMAL

    could_trade = False

    if (self.status == RiskControlStatus.RISK_WARNING):
      #if (self.status == RiskControlStatus.RISK_WARNING) or not(self.check_for_usa_intrest_rate(context)):
      could_trade = self.check_for_rsi(15, 55, 90, False) and self.check_for_rsi(90, 50, 90, False)
      # could_trade = self.check_for_rsi(60, 47, 99, False)
      #record(status=2.5)
    elif (self.status == RiskControlStatus.RISK_NORMAL):
      could_trade = self.check_for_rsi(60, 50, 99, False)
      # could_trade = True
      #record(status=0.7)

    return could_trade
