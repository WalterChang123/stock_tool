import requests
from io import StringIO
import pandas as pd
import datetime

from SQL_Function import *
import time
import random

# Test WEB
#https://mops.twse.com.tw/nas/t21/sii/t21sc03_106_1_0.html

#define the crawling period
start_month = datetime.date(2012, 1, 1)
end_month = datetime.date.today()
#datetime.date.today()

month_range = pd.date_range(start=start_month, end=end_month, freq='MS') #freq = Moonthly Start

#讀取以爬過的月份
try:
    occupied_month = read_df_from_sql('stock_data', 'month_report').index.levels[1].tolist()  # INDX=['stock_id', 'date']
except:
    print('Not found price data..')
    occupied_month = []

#print(m.strftime('%Y%m'))
for m in month_range:

    if str(m) in occupied_month:
        #print(d.strftime('%Y%m%d') + ' is existed.', end ='\r')
        print(m.strftime('%Y%m') + ' is existed.')
        continue



    # 指定爬取月報的網址
    url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_' + str((int(m.strftime('%Y')) - 1911))  + '_' + str(int(m.strftime('%m'))) + '_0.html'

    # 抓取網頁
    r = requests.get(url)
    
    #讓pandas可以看懂中文
    r.encoding = 'big5'
    
    # 把剛剛下載下來的網頁的 html 文字檔，利用 StringIO() 包裝成一個檔案給 pandas 讀取
    try:
        dfs = pd.read_html(StringIO(r.text))
    except:
        print("Somthing went worong when getting the report of " + m.strftime("%Y%m"))
        continue
    
    
    # 將dfs中，row長度介於5~11的table合併（這些才是我們需要的table，其他table不需要）
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])

    # 設定column名稱
    df.columns = df.columns.get_level_values(1)

    # 將 df 中的當月營收用 .to_numeric 變成數字，再把其中不能變成數字的部分以 NaN 取代（errors='coerce'）
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')

    # 再把當月營收中，出現 NaN 的 row 用 .dropna 整行刪除
    df = df[~df['當月營收'].isnull()]

    # 刪除「公司代號」中出現「合計」的行數，其中「～」是否定的意思
    df = df[df['公司代號'] != '合計']

    #Change the title
    df = df.rename(columns={'公司代號':'stock_id'})

    df.drop('備註', inplace=True, axis=1, errors = 'ignore')

    #加入當月日看
    df['date'] = m

    # 將「公司代號」與「公司名稱」共同列為 df 的 indexes
    df = df.set_index(['stock_id', 'date'])

    #Add to sql
    add_df_to_sql('stock_data', 'month_report', df, 'append')
    print(m.strftime('%Y%m') + " Updated!!!!!!!!")

    # TWSE 有 request limit, 每 5 秒鐘 3 個 request，超過的話會被 ban 掉
    time.sleep(random.uniform(3, 10))


sort_data('stock_data', 'month_report')
