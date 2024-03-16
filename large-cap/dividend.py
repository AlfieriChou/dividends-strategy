# -*- coding: utf-8 -*-

from jqdata import *
from jqlib.technical_analysis import *
import datetime
import pandas as pd
import requests
import json

class DividendYield(object):
  '''
  转换交易日期
  遇到节假日取最近的一个交易日
  '''
  @staticmethod
  def convert_trade_date(query_date: datetime.date) -> datetime.date:
    if query_date is None :
      return None
    
    if type(query_date) is str :
      query_date = datetime.datetime.strptime(query_date, "%Y-%m-%d").date()

    trade_days = get_trade_days(start_date = None, end_date = query_date, count = 1)
    return trade_days[0] if len(trade_days) > 0 else None

  '''
  单个股票在查询日期对应的去年分红（每10股分红）
  查询日期在年报公告日期前，使用前年分红数据；在年报后，使用去年分红数据
  '''
  @staticmethod
  def get_bonus_ratio_rmb(item:pd.DataFrame, query_date:datetime.date) -> dict :
    #取query_date之前最后的财报日期
    max_report_date = item[item['board_plan_pub_date'] < query_date]['report_date'].max()
    #过滤未分红等数据
    if not type(max_report_date) is datetime.date:
      return 0
    
    #查询日期前1年的财报的report_date
    last_year = datetime.datetime.strptime(str(query_date.year - 1) + '-12-31', "%Y-%m-%d").date()
    
    #如果查询日期在前1年年报披露后，使用前1年分红额；否则使用前2年的分红数据
    if max_report_date >= last_year:
      bonus_year = query_date.year - 1
    else:
      bonus_year = query_date.year - 2

    #汇总整年股息
    bonus_sum = item.groupby('bonus_year').sum().sort_values(by = 'bonus_year', ascending = False)

    bonus_ratio_rmb = 0
    if bonus_year in bonus_sum['bonus_ratio_rmb'] :
      bonus_ratio_rmb = bonus_sum['bonus_ratio_rmb'][bonus_year]

    return bonus_ratio_rmb
  
  '''
  批量查询一组股票的每10股分红数据
  return dataframe
  '''
  @staticmethod
  def get_bonus_ratio(code_list:list, query_date:datetime.date) -> pd.DataFrame:
      
    if len(code_list) == 0 or query_date is None:
      return None
    
    #查询股息分配信息
    tb = finance.STK_XR_XD
    q = query(
      tb.code,
      tb.report_date,
      tb.bonus_ratio_rmb,
      tb.board_plan_pub_date
    ).filter(
      tb.code.in_(code_list)
    ).order_by(
      tb.report_date.desc()
    )
    df = finance.run_query(q)
    df = df.dropna()

    #新增分配股息年份字段，便于合并年内多次分红
    df['bonus_year'] = df['report_date'].map(lambda x : x.year)

    tmp = []
    for code in code_list:
        
      tmp.append({
        'code':code,
        'query_date': query_date,
        'bonus_ratio_rmb': DividendYield.get_bonus_ratio_rmb(
          df[df['code'] == code],
          query_date
        )
      })

    return pd.DataFrame(tmp)
  
  '''
  股息率(%) = 10 * 每10股分红（元） / 股价（元）
  不用 分红总数 / 市值， 是因为多地上市公司计算起来比较麻烦，不仅要用A股分红还得加上其他地方的，比如港股分红及汇率问题
  '''
  @staticmethod
  def calc_dividend_yield(code_list:list, query_date:datetime.date, price_df:pd.DataFrame) -> pd.DataFrame:
    bonus_df = DividendYield.get_bonus_ratio(code_list, query_date) 
    if bonus_df is None :
      return None
    price_df = price_df.drop(['time'], axis = 1)
    df = pd.merge(price_df, bonus_df, on = 'code')
    #每10股分红转化为百分比股息率
    df['dividend_yield'] = 10 * df.bonus_ratio_rmb / df.close
    df = df.drop(['close', 'bonus_ratio_rmb'], axis = 1)
    return df
  
  '''
  发送监控报告到企业微信
  '''
  @staticmethod
  def send_monitor_wechat_msg(context, url: str, title: str, content: str) -> None:
    headers = { 'Content-Type': 'application/json; charset=utf-8' }
    post_data = {
      'msgtype': 'markdown',
      'markdown': {
        'content':  '## ' + title + ' \n 当前时间：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S") + ' \n \n ### 超过分红预警: \n' + content + '\n @Alfred'
      }
    }
    r = requests.post(url, headers = headers, data = json.dumps(post_data))
    log.info('返回结果：', str(r.text))
