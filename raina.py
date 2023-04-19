import requests 
import json
import os
import sqlite3

def get_sp500_data(url):
    r = requests.get(url)
    data = r.json()
    print(data)
    return data
url = "http://api.marketstack.com/v1/eod?access_key=2ce5cc310401da10b32ed56816d0c129&symbols=DJIA"
print(get_sp500_data(url))

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def make_integer_key_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS SP500_data (id INTEGER AUTO_INCREMENT PRIMARY KEY, open FLOAT, close FLOAT, high FLOAT, low FLOAT, date STRING)")
    
    count = 0 
    for x in data['data']:
        cur.execute("SELECT * FROM SP500_data WHERE date = ?", (x['date'],))
        first = cur.fetchall()
        
        if len(first) != 0:
            # print(first)
            continue 
        else:
            count += 1
            cur.execute("INSERT INTO SP500_data(open, close, high, low, date) VALUES (?, ?, ?, ?, ?)", (x['open'], x['close'], x['high'], x['low'], x['date'])) 
        
        if count == 25:
            break

    conn.commit()  

def main():
    url = "http://api.marketstack.com/v1/eod?access_key=2ce5cc310401da10b32ed56816d0c129&symbols=DJIA"
    data = get_sp500_data(url)
    curr, conn = open_database('raina.db')
    make_integer_key_table(data, curr, conn)

print(main())