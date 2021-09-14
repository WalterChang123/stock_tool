from Crawl_Daily_Price_Function import *
from SQL_Function import *
import time
import random
# from tqdm import tqdm_gui
from tkinter.ttk import *
from tkinter import *
import time
import datetime

# test if the URL is blocked: 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=20200928&type=ALLBUT0999'


def crawl_price_daily():
    # 產生需要爬蟲的範圍
    start_date = datetime.date(2012, 1, 1)
    end_date = datetime.date.today() - datetime.timedelta(1)
    # end_date = datetime.date(2014, 1, 1)
    date_range = pd.date_range(start=start_date, end=end_date)
    try:
        occupied_date = read_df_from_sql(
            'stock_data', 'price').index.levels[1].tolist()  # INDX=['stock_id', 'date']
    except:
        print('Not found price data..')
        occupied_date = []

    try:
        close_date = read_df_from_sql(
            'stock_data', 'close_date', 'close_date').index.tolist()
    except:
        print('Not found close_date data..')
        close_date = []

    # 判斷已經存在或是沒有開市的日子，少走冤望路
    for d in date_range:
        if str(d) in close_date:
            #print(d.strftime('%Y%m%d') + ' is close.', end ='\r')
            print(d.strftime('%Y%m%d') + ' is close.')
            continue
        elif str(d) in occupied_date:
            #print(d.strftime('%Y%m%d') + ' is existed.', end ='\r')
            print(d.strftime('%Y%m%d') + ' is existed.')
            continue

        # 開始爬蟲
        content = crawl_price(d)

        if content is None:
            print("Error occurs when getting the " +
                d.strftime('%Y%m%d') + " price data.")
        elif len(content) == 0:
            print(d.strftime('%Y%m%d') + ' is close, save ' +
                d.strftime('%Y%m%d') + ' to close_date.')

            # 加入到沒開市的日子裡面
            add_close_date('stock_data', d)

        else:
            add_df_to_sql('stock_data', 'price', content, 'append')
            print(d.strftime('%Y%m%d') + " Updated!!!!!!!!")

        # TWSE 有 request limit, 每 5 秒鐘 3 個 request，超過的話會被 ban 掉
        time.sleep(random.uniform(3, 10))

    print('Sorting daily price data...')
    sort_data('stock_data', 'close_date', 'close_date')
    sort_data('stock_data', 'price')
    print('Update Finished!!!!')


crawl_price_daily()
