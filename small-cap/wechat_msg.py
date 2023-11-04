
# -*- coding: utf-8 -*-

from jqdata import *
from jqdata import finance
import pandas as pd
from jqfactor import get_factor_values
from jqlib.technical_analysis import *
import requests
import json

def get_bolling(context, security, unit = '1d', n = 20, m = 2):
  close_data = get_bars(security, count = n, unit = unit, fields = ['close'])
  price = np.array(close_data['close'])
  middle = price.sum() / n
  std = np.std(price)
  up = middle + m * std
  down = middle - m * std
  return [up, down, middle]

def get_operator_msg(context, config, stock_list, current_data):
  sell_str = ''
  buy_str = ''
  now = context.current_dt
  for stock in stock_list:
    [up, down, middle] = get_bolling(context, stock, unit = '60m', n = config['n'], m = config['m'])
    current_price = current_data[stock].last_price
    code_name = current_data[stock].name
    if current_price >= up:
      sell_str += '- 代码: ' + '{}'.format(stock) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 卖出参考价: ' + '<font color = \"warning\">' + '{}'.format(format(up, '.2f')) + '</font>' + ' \n'
    if current_price <= down:
      buy_str += '- 代码: ' + '{}'.format(stock) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 买入参考价: ' + '<font color = \"info\">' + '{}'.format(format(down, '.2f')) + '</font>' + ' \n'
  return [sell_str, buy_str]

# send dingtalk msg
def send_wechat_msg(context, config, stock_list, current_data):
  url = config['wechat_msg_url']
  [sell_str, buy_str] = get_operator_msg(context, config, stock_list, current_data)
  if sell_str == '' and buy_str == '':
    return
  headers = { 'Content-Type': 'application/json; charset=utf-8' }
  post_data = {
    'msgtype': 'markdown',
    'markdown': {
      'content':  '## ' + config['name'] + ' \n 当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n \n ### 买入参考: \n' + buy_str + ' \n \n ### 卖出参考: \n' + sell_str + '\n @Alfred'
    }
  }
  r = requests.post(url, headers = headers, data = json.dumps(post_data))
  log.info('返回结果：', str(r.text))

def build_daily_target_wechat_msg(context, stock_list, config, current_data):
  yesterday = context.previous_date
  markdown = ''
  for stock in stock_list:
    [up, down, middle] = get_bolling(context, stock, unit = '30m', n = config['n'], m = config['m'])
    code_name = current_data[stock].name
    current_price = current_data[stock].last_price
    is_buy = '是' if current_price > middle else '否'
    turnover_df = get_valuation(stock, end_date = yesterday, fields = ['turnover_ratio'], count = 1)
    turnover_ratio = turnover_df.loc[0, 'turnover_ratio']
    markdown += '* 股票：' + code_name + stock
    markdown += ' 换手率：' + str(turnover_ratio)
    markdown += ' 支持买入：' + is_buy 
    markdown += ' 当前价格：￥' + str(current_price)
    markdown += ' 上轨：<font color = \"warning\">' + '{}'.format(format(up, '.2f')) + '</font>' 
    markdown += ' 下轨：<font color = \"info\">' + '{}'.format(format(down, '.2f')) + '</font>' 
    markdown += ' 中轨：<font color = \"warning\">' + '{}'.format(format(middle, '.2f')) + '</font>' 
    markdown += '\n'
  return markdown

def build_daily_hold_wechat_msg(hold_list):
  markdown = ''
  header = ['股票', '成本价', '现价', '收益率', '持仓（股）', '市值']
  for position in hold_list:
    security = position.security
    security_name = get_security_info(security).display_name
    cost = position.avg_cost
    price = position.price
    ret = 100 * (price / cost - 1)
    value = position.value
    amount = position.total_amount
    markdown += '* 股票：' + security_name + security
    markdown += ' 成本价：' + '￥' + '{}'.format(format(cost, '.2f'))
    markdown += ' 现价：' + '￥' + '{}'.format(format(price, '.2f')) 
    markdown += ' 收益率：' + '{}%'.format(format(ret, '.2f'))
    markdown += ' 持仓（股）：' + '{}'.format(format(amount, '.2f'))
    markdown += ' 市值：' + '￥' + '{}'.format(format(value, '.2f'))
    markdown += '\n'
  return markdown

def send_daily_hold_wechat_msg(context, config):
  url = config['daily_wechat_msg_url']
  markdown =  '## ' + config['name'] + '\n'
  markdown += '当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n'
  hold_list = list(context.portfolio.positions.values())
  hold_msg = build_daily_hold_wechat_msg(hold_list)
  markdown += '### 持仓: \n' + hold_msg + '\n'
  markdown += '@Alfred \n'
  headers = { 'Content-Type': 'application/json; charset=utf-8' }
  post_data = {
    'msgtype': 'markdown',
    'markdown': {
      'content':  markdown
    }
  }
  r = requests.post(url, headers = headers, data = json.dumps(post_data))
  log.info('返回结果：', str(r.text))

def send_daily_target_wechat_msg(context, config, target_list, current_data):
  url = config['daily_wechat_msg_url']
  markdown =  '## ' + config['name'] + '\n'
  markdown += '当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n'
  target_msg = build_daily_target_wechat_msg(context, target_list, config, current_data)
  markdown += '### 每日精选: \n' + target_msg + ' \n'
  markdown += '@Alfred \n'
  headers = { 'Content-Type': 'application/json; charset=utf-8' }
  post_data = {
    'msgtype': 'markdown',
    'markdown': {
      'content':  markdown
    }
  }
  r = requests.post(url, headers = headers, data = json.dumps(post_data))
  log.info('返回结果：', str(r.text))
