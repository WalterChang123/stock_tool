import pandas as pd
import sqlite3
from io import StringIO


def add_df_to_sql(data_file, data_name, df, action):
    conn = sqlite3.connect(data_file + '.sqlite3')
    # action can be 'append','replace','fail'
    df.to_sql(data_name, conn, if_exists=action)
    conn.close()


def read_df_from_sql(data_file, data_name, indx=['stock_id', 'date']):
    conn = sqlite3.connect(data_file + '.sqlite3')
    dfl = []
    for chunk in pd.read_sql('select * from ' + data_name, conn, index_col=indx, chunksize=500000):
        dfl.append(chunk)
    df = pd.concat(dfl, ignore_index=False)

    conn.close()
    return df


def get_close_date(data_file):
    conn = sqlite3.connect(data_file + '.sqlite3')
    df = pd.read_sql('select * from ' + 'close_date',
                    conn, index_col=['close_date'])
    list_a = pd.to_datetime(df.index)
    return list_a


def add_close_date(data_file, date):
    #date_content = 'close_date\r\n' + date.strftime('%Y-%m-%d')
    #df = pd.read_csv(StringIO(date_content))
    df = pd.DataFrame({'close_date': [date]})
    add_df_to_sql(data_file, 'close_date', df, 'append')


def sort_data(data_file, data_name, indx=['stock_id', 'date']):
    df = read_df_from_sql(data_file, data_name, indx)
    #df = df.reset_index().drop_duplicates(subset=indx).set_index(indx)
    df = df[~df.index.duplicated(keep='first')]
    df = df.sort_index()
    add_df_to_sql('stock_data', data_name, df, 'replace')

def ADD_TEST():
    print(fucking test)