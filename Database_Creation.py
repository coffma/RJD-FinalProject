import requests
import os
import sqlite3
import csv
import unittest
import plotly.graph_objects as go
import matplotlib.pyplot as plt

def make_empty_file():
    try:
        file = open("final206.db", 'r')
        file.close()
    except:
        file = open("final206.db", 'w')
        file.close()
    return "final206.db"

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
       
def make_integer_key_table(cur, conn):
    cur.execute("DROP TABLE IF EXISTS Quarter")
    cur.execute("CREATE TABLE IF NOT EXISTS Quarter (id INTEGER, month1 TEXT, month2 TEXT, month3 TEXT)")
    conn.commit()

    cur.execute("INSERT INTO Quarter (id, month1, month2, month3) VALUES (?,?,?,?)",(1, '01', '02', '03'))
    cur.execute("INSERT INTO Quarter (id, month1, month2, month3) VALUES (?,?,?,?)",(2, '04', '05', '06'))
    cur.execute("INSERT INTO Quarter (id, month1, month2, month3) VALUES (?,?,?,?)",(3, '07', '08', '09'))
    cur.execute("INSERT INTO Quarter (id, month1, month2, month3) VALUES (?,?,?,?)",(4, '10', '11', '12'))
    conn.commit() 

# Microsoft and Oracle API Data 

def get_data(symbol):
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={}&apikey=BL1X42CD6CU2F397".format(symbol)
    r = requests.get(url)
    data = r.json()
    return data

def create_table_MSFT(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS MSFT_data (idM INTEGER, dateM TEXT, openM FLOAT, closeM FLOAT, highM FLOAT, lowM FLOAT)")
    conn.commit()

    cur.execute("SELECT * FROM Quarter")
    quarter_data = cur.fetchall()
    quarter_dict = {}
    for pairs in quarter_data:
        quarter_dict[pairs[0]] = pairs[1],pairs[2],pairs[3]
        
    count = 0
    for x in data['Weekly Adjusted Time Series']:
        cur.execute("SELECT * FROM MSFT_data WHERE dateM = ?", (x,))
        first = cur.fetchall()

        if len(first) != 0:
            continue
        else:
            count += 1 
            open = data['Weekly Adjusted Time Series'][x]['1. open']
            high = data['Weekly Adjusted Time Series'][x]['2. high']
            low = data['Weekly Adjusted Time Series'][x]['3. low']
            close = data['Weekly Adjusted Time Series'][x]['4. close']
            month = x.split('-')[1]
            for id in quarter_dict:
                if month in quarter_dict[id]:
                    real_id = id
            cur.execute("INSERT INTO MSFT_data (idM, dateM, openM, closeM, highM, lowM) VALUES (?,?,?,?,?,?)",(real_id, x, open, close, high, low))
            
        if count == 25:
            break
    conn.commit()

def create_table_ORCL(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS ORCL_data (idO INTEGER, dateO TEXT, openO FLOAT, closeO FLOAT, high_value FLOAT, lowO FLOAT)")
    conn.commit()

    cur.execute("SELECT * FROM Quarter")
    quarter_data = cur.fetchall()
    quarter_dict = {}
    for pairs in quarter_data:
        quarter_dict[pairs[0]] = pairs[1],pairs[2],pairs[3]
    
    count = 0
    for x in data['Weekly Adjusted Time Series']:
        cur.execute("SELECT * FROM ORCL_data WHERE dateO = ?", (x,))
        first = cur.fetchall()

        if len(first) != 0:
            continue
        else:
            count += 1 
            open = data['Weekly Adjusted Time Series'][x]['1. open']
            high = data['Weekly Adjusted Time Series'][x]['2. high']
            low = data['Weekly Adjusted Time Series'][x]['3. low']
            close = data['Weekly Adjusted Time Series'][x]['4. close']
            month = x.split('-')[1]
            for id in quarter_dict:
                if month in quarter_dict[id]:
                    real_id = id
            cur.execute("INSERT INTO ORCL_data (idO, dateO, openO, closeO, high_value, lowO) VALUES (?,?,?,?,?,?)",(real_id, x, open, close, high, low))
        
        if count == 25:
            break
    conn.commit()

# S&P 500 API Data

def get_sp500_data(url):
    r = requests.get(url)
    data = r.json()
    # print(data)
    return data
url = "http://api.marketstack.com/v1/eod?access_key=2ce5cc310401da10b32ed56816d0c129&symbols=DJIA"


def make_integer_SP500_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS SP500_data (date STRING PRIMARY KEY, open FLOAT, close FLOAT, high FLOAT, low FLOAT)")
    
    count = 0 
    for x in data['data']:
        cleaned = x['date'].split("T")
        cur.execute("SELECT * FROM SP500_data WHERE date = ?", (cleaned[0],))
        first = cur.fetchall()
        
        if len(first) != 0:
            continue 
        else:
            count += 1
            cur.execute("INSERT INTO SP500_data(date, open, close, high, low) VALUES (?, ?, ?, ?, ?)", (cleaned[0], x['open'], x['close'], x['high'], x['low'])) 
        
        if count == 25:
            break

    conn.commit()  

# Company Rating Scores API Data

def get_rating_data(company):
    url = 'https://financialmodelingprep.com/api/v3/historical-rating/{}?limit=500&apikey=d8ef8ecd4ffecd0c60dc07ee87e0219c'.format(company)
    r = requests.get(url)
    rating_data = r.json()
    result = [{'date': d['date'], 'ratingScore': d['ratingScore']} for d in rating_data]

    return result

def make_oracle_rating_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS ORCL_rating (date TEXT, score INTEGER)")
    count = 0 
    for x in data:
        cur.execute("SELECT * FROM ORCL_rating WHERE date = ?", (x['date'], ))
        first = cur.fetchall()
        if len(first) != 0:
            continue 
        else:
            count += 1
            cur.execute("INSERT INTO ORCL_rating (date, score) VALUES (?, ?)", (x['date'], x['ratingScore'])) 
        
        if count == 25:
            break
    conn.commit()  

def make_msft_rating_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS MSFT_rating (date TEXT, score INTEGER)")
    count = 0 
    for x in data:
        cur.execute("SELECT * FROM MSFT_rating WHERE date = ?", (x['date'], ))
        first = cur.fetchall()
        if len(first) != 0:
            continue 
        else:
            count += 1
            cur.execute("INSERT INTO MSFT_rating (date, score) VALUES (?, ?)", (x['date'], x['ratingScore'])) 
        
        if count == 25:
            break
    conn.commit()


# Creating Main Function
def main():
    db_name_real = make_empty_file()
    cur, conn = open_database(db_name_real)
    make_integer_key_table(cur, conn)
    microsoft = get_data("MSFT")
    oracle = get_data("ORCL")
    # join_tables(cur, conn)
    create_table_ORCL(oracle, cur, conn)
    create_table_MSFT(microsoft, cur, conn)

    url = "http://api.marketstack.com/v1/eod?access_key=22947e825aca998520523a2f381a8a4d&symbols=DJIA"
    data = get_sp500_data(url)
    make_integer_SP500_table(data, cur, conn)

    orcl_data = get_rating_data("ORCL")
    make_oracle_rating_table(orcl_data, cur, conn)
    msft_data = get_rating_data("MSFT")
    make_msft_rating_table(msft_data, cur, conn)


if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)