from sqlalchemy import desc
import numpy as np
import pandas as pd
from dateutil import relativedelta
def initialize(context):
    run_monthly(month_select, 1, time='before_open')
    context.last_update_dt = None
def check_date(context):
    if context.last_update_dt is None: return True
    delta = relativedelta.relativedelta(months=1)
    dt = context.last_update_dt + delta
    if context.current_dt.year == dt.year and context.current_dt.month == dt.month: return True
    else: return False
def get_avg(context, weeks, security):
    delta = relativedelta.relativedelta(weeks=-weeks)
    start_date = context.current_dt + delta
    end_date = context.current_dt
    df = get_price(security, start_date, end_date,fields=['close'])
    return df.mean()['close']
def check_avg(context):
    ref_security = '000300.XSHG'
    if get_avg(context, 6, ref_security) < get_avg(context, 12, ref_security):
        context.selected_stocks = []
        #段均线小于长均线，清空股票
        print('均线小于长均线，清空股票')
        return False
    else: return True

def unpaused(stockspool):
    current_data=get_current_data()
    return [s for s in stockspool if not current_data[s].paused]
def get_stocks(context):
    stocks = get_all_securities(['stock'])
    #stocks = stocks[stocks['end_date'] > context.current_dt.date()].index
    stocks  = stocks.index
    date=context.current_dt.strftime("%Y-%m-%d")
    st=get_extras('is_st', stocks, start_date=date, end_date=date, df=True)
    st=st.loc[date]
    stocks=list(st[st==False].index)
    
    return unpaused(stocks)
def assign_order(s):
    s.dropna(inplace=True)
    arg_sort = s.argsort()
    s[arg_sort] = np.arange(len(arg_sort))
def month_select(context):
    print('month_select', context.current_dt)
    if not check_date(context): return
    context.last_update_dt = context.current_dt
    context.need_update = True
    stocks = get_stocks(context)
    
    q = query(valuation).filter(valuation.code.in_(stocks))
    df = get_fundamentals(q)
    s_cmc = pd.Series(df['market_cap'].values, index=df['code'