# -*- coding: UTF8 -*-
import pandas as pd
import requests
from io import StringIO
from tkinter import *


def crawl_price(date):  # In put the form of "datetime.date"
    # 先定義可以爬蟲的方程式，輸入日期後可以直接產生該日期的股價table
    datestr = date.strftime('%Y%m%d')

    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + \
        datestr + '&type=ALLBUT0999'

    try:
        r = requests.get(url)
    except:
        return None

    # r.encoding = 'big5'
    content = r.text

    # 把出現在代碼前面莫名的'='拿掉
    content = content.replace('=', '')

    if content == '':
        return ""

    # pandas無法接受欄數不一致的csv，只把有16欄的有價值的欄給保留
    lines = content.split('\r\n')
    newlines = list(filter(lambda l: len(l.split('","')) == 16, lines))
    content = '\r\n'.join(newlines)

    # 在這之後全部用pandas操作了
    df = pd.read_csv(StringIO(content), engine='python')

    # 設置stock_index, date
    df['date'] = pd.to_datetime(date)
    df = df.rename(columns={'證券代號': 'stock_id'})
    df = df.set_index(['stock_id', 'date'])

    # 把不要的','拿掉，用成數字來操作
    df = df.astype(str)
    df = df.apply(lambda s: s.str.replace(',', ''))
    # 將所有的表格元素都轉換成數字，error='coerce'的意思是說，假如無法轉成數字，則用 NaN 取代
    df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))
    # 把整欄都是非數字的部分都拿掉
    df = df[df.columns[df.isnull().all() == False]]

    return df
