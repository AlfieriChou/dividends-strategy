# -*- coding: utf-8 -*-

from jqdata import *
from jqlib.technical_analysis import *
import requests
import json

def get_monitor_md_operator_msg(config, current_data):
  sell_str = ''
  buy_str = ''
  for stock_info in config['monitor_stock_list']:
    code = stock_info['code']
    max = stock_info['max']
    min = stock_info['min']
    current_price = current_data[code].last_price
    code_name = current_data[code].name
    if current_price >= max:
      sell_str += '- 代码: ' + '{}'.format(code) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 顶部参考价: ' + '<font color = \"warning\">' + '{}'.format(format(max, '.2f')) + '</font>' + ' \n'
    if current_price <= min:
      buy_str += '- 代码: ' + '{}'.format(code) + '/' + '{}'.format(code_name) + ' 当前价: ' + '{}'.format(format(current_price, '.2f')) + ' 底部参考价: ' + '<font color = \"info\">' + '{}'.format(format(min, '.2f')) + '</font>' + ' \n'
  return [sell_str, buy_str]

def send_monitor_wechat_msg(context, config, current_data):
  url = config['monitor_wechat_msg_url']
  [sell_str, buy_str] = get_monitor_md_operator_msg(config, current_data)
  if sell_str == '' and buy_str == '':
    return
  headers = { 'Content-Type': 'application/json; charset=utf-8' }
  post_data = {
    'msgtype': 'markdown',
    'markdown': {
      'content':  '## ' + config['monitor_name'] + ' \n 当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n \n ### 底部预警: \n' + buy_str + ' \n \n ### 顶部预警: \n' + sell_str + '\n @Alfred'
    }
  }
  r = requests.post(url, headers = headers, data = json.dumps(post_data))
  log.info('返回结果：', str(r.text))
