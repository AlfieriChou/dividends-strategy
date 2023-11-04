# -*- coding: utf-8 -*-

from jqdata import *
from jqlib.technical_analysis import *
import requests
import json

def get_bolling(security, unit = '1d', n = 20, m = 2):
  close_data = get_bars(security, count = n, unit = unit, fields = ['close'])
  price = np.array(close_data['close'])
  middle = price.sum() / n
  std = np.std(price)
  up = middle + m * std
  down = middle - m * std
  return [up, down, middle]

def get_operator_msg(config, current_data):
  sell_str = ''
  buy_str = ''
  for stock_info in config['realtime_stock_list']:
    stock = stock_info['code']
    [up, down, middle] = get_bolling(stock, unit = stock_info['unit'], n = config['n'], m = config['m'])
    current_price = current_data[stock].last_price
    code_name = current_data[stock].name
    if current_price >= up:
      sell_str += '- 代码: ' + '{}'.format(stock) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 卖出参考价: ' + '<font color = \"warning\">' + '{}'.format(format(up, '.2f')) + '</font>' + ' \n'
    if current_price <= down:
      buy_str += '- 代码: ' + '{}'.format(stock) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 买入参考价: ' + '<font color = \"info\">' + '{}'.format(format(down, '.2f')) + '</font>' + ' \n'
  return [sell_str, buy_str]

# send realtime wechat msg
def send_realtime_wechat_msg(context, config, current_data):
  url = config['realtime_wechat_msg_url']
  [sell_str, buy_str] = get_operator_msg(config, current_data)
  if sell_str == '' and buy_str == '':
    return
  headers = { 'Content-Type': 'application/json; charset=utf-8' }
  post_data = {
    'msgtype': 'markdown',
    'markdown': {
      'content':  '## ' + config['realtime_name'] + ' \n 当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n \n ### 买入参考: \n' + buy_str + ' \n \n ### 卖出参考: \n' + sell_str + '\n @Alfred'
    }
  }
  r = requests.post(url, headers = headers, data = json.dumps(post_data))
  log.info('返回结果：', str(r.text))
