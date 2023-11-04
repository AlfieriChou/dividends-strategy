
# -*- coding: utf-8 -*-

from jqdata import *
from jqdata import finance
import pandas as pd
from jqfactor import get_factor_values
from jqlib.technical_analysis import *
import smtplib
from email.mime.text import MIMEText
from email.header import Header

def get_bolling(context, security, unit = '1d', n = 20, m = 2):
  close_data = get_bars(security, count = n, unit = unit, fields = ['close'])
  price = np.array(close_data['close'])
  middle = price.sum() / n
  std = np.std(price)
  up = middle + m * std
  down = middle - m * std
  return [up, down, middle]

def build_daily_half_hour_msg(context, stock_list, config, index_name, current_data):
  yesterday = context.previous_date
  # 打包hmlt表格
  title = '[' + index_name + ']每日操作点'
  header = ['股票', '换手率', '支持买入', '当前价格', '中轨', '市值', 'PE', '下轨', '上轨']
  # 数据二维数组
  rows = []
  for stock in stock_list:
    [up, down, middle] = get_bolling(context, stock, unit = '30m', n = config['n'], m = config['m'])
    code_name = current_data[stock].name
    current_price = current_data[stock].last_price
    is_buy = '是' if current_price > middle else '否'
    turnover_df = get_valuation(stock, end_date = yesterday, fields = ['turnover_ratio', 'pe_ratio'], count = 1)
    turnover_ratio = turnover_df.loc[0, 'turnover_ratio']
    pe_ratio = turnover_df.loc[0, 'pe_ratio']
    q = query(valuation).filter(valuation.code == stock)
    df = get_fundamentals(q)
    market_cap = df['market_cap'][0]
    rows.append([
      code_name + stock,
      turnover_ratio,
      is_buy,
      '￥' + str(current_price),
      format(middle, '.2f'),
      str(format(market_cap, '.2f')) + '亿',
      str(format(pe_ratio, '.2f')),
      format(down, '.2f'),
      format(up, '.2f')
    ])
  return render_to_html_table(title, {}, header, rows)

def build_hold_msg(hold_list):
  title = '持仓'
  header = ['股票', '成本价', '现价', '收益率', '持仓（股）', '市值']
  rows = []
  for position in hold_list:
    security = position.security
    security_name = get_security_info(security).display_name
    cost = position.avg_cost
    price = position.price
    ret = 100 * (price / cost - 1)
    value = position.value
    amount = position.total_amount
    rows.append([
      security_name + security,
      '￥' + '{}'.format(format(cost, '.2f')),
      '￥' + '{}'.format(format(price, '.2f')),
      '{}%'.format(format(ret, '.2f')),
      '{}'.format(format(amount, '.2f')),
      '￥' + '{}'.format(format(value, '.2f'))
    ])
  return render_to_html_table(title, {}, header, rows)

##163邮箱邮件发送简单邮件
def send_email(
  config,
  subject = '测试',
  message = '测试'
):
  ## 发送邮件
  receive_emails = config['testSendEmails'] if config['isTest'] else config['sendEmails']
  sender = '量化交易策略提醒<' + config['emailUsername'] + '>' #发送的邮箱
  msg = MIMEText(str(message),'plain','utf-8') #中文需参数‘utf-8'，单字节字符不需要
  msg['Subject'] = Header(subject, 'utf-8') #邮件主题
  msg['to'] = receive_emails #收件人
  msg['from'] = sender    #自己的邮件地址 

  server = smtplib.SMTP_SSL('smtp.163.com')
  try :
    server.login(config['emailUsername'], config['emailPassword']) # 登陆
    server.sendmail(sender, receive_emails.split(','), msg.as_string()) #发送
    print('邮件发送成功')
  except:
    print('邮件发送失败')
    print("Unexpected error:", sys.exc_info())
  server.quit() # 结束
  print('结束')

# 发送邮件
def send_email_msg(context, config, target_list, current_data):
  mail_msg = ''
  mail_msg += '<p>策略名称：' + config['name'] + '</p>'
  mail_msg += '<p>交易时间：' + context.current_dt.strftime('%Y-%m-%d') + '</p>'
  mail_msg += build_daily_half_hour_msg(context, target_list, config, '自选', current_data)
  hold_list = list(context.portfolio.positions.values())
  mail_msg += build_hold_msg(hold_list)
  send_email(config, config['name'], mail_msg)

# 渲染数据
def render_to_html_table(title = '', config = {}, header = [], rows = [[]]):
  color_list = ['White','WhiteSmoke', 'Crimson', 'Green']
  msg = ''
  title_color = 'LightBLue'
  msg += '<p>'+ title +'：</p>'
  # TODO config 需要重新考虑
  msg += '<table cellpadding="10">'
  # 构建表头
  msg += '<tr bgcolor="' + title_color + '" >'
  for field in header:
    msg += '<th>' + field + '</th>'
  msg += '</tr>'
  # 填充数据
  for index, values in enumerate(rows):
    msg += '<tr bgcolor="' + str(color_list[index % 2]) + '">'
    for value in values:
      msg += '<td>' + str(value) + '</td>'
    msg += '</tr>'
  return msg + '</table>'
# end
