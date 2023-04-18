import requests
import json
import os
import sqlite3

def make_empty_file():
    try:
        file = open("final206.db", 'r')
        file.close()
    except:
        file = open("final206.db", 'w')
        file.close()
    return "final206.db"

db_name_real = make_empty_file()

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

cur, conn = open_database(db_name_real)

def create_valid_dates(month_values):
    valid_dates = []
    for element in month_values:
        for i in range(1,26):
            if element<10:
                date = '0'+str(element)+"-"+str(i)
                valid_dates.append(date)
            else:
                date = str(element)+"/"+str(i)
                valid_dates.append(date)
    return valid_dates
dates = create_valid_dates([1,4,7,10])
       
def make_integer_key_table(dates, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Quarter_Id (id INTEGER PRIMARY KEY, month TEXT)")
    for element in dates:
        month = element.split("-")[0]
        if month == '01':
            cur.execute("INSERT OR IGNORE INTO Quarter_Id (id, month) VALUES (?,?)",(1, "January"))
        elif month == '04':
            cur.execute("INSERT OR IGNORE INTO Quarter_Id (id, month) VALUES (?,?)",(2, "April"))
        elif month == '07':
            cur.execute("INSERT OR IGNORE INTO Quarter_Id (id, month) VALUES (?,?)",(3, "July"))
        else:
            cur.execute("INSERT OR IGNORE INTO Quarter_Id (id, month) VALUES (?,?)",(4, 'October'))
    conn.commit()  
make_integer_key_table(dates, cur, conn)


def get_data(symbol):
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={}&apikey=BL1X42CD6CU2F397".format(symbol)
    r = requests.get(url)
    data = r.json()
    return data

ibm_data = get_data("IBM")
# print(ibm_data)
microsoft = get_data("MSFT")
oracle = get_data("ORCL")

def load_the_data(company, data, desired_year, wanted_quarter, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS {} (id TEXT, date TEXT, open INTEGER, high INTEGER, low INTEGER, close INTEGER, volume INTEGER)".format(company))
    conn.commit()

    cur.execute("SELECT * FROM Quarter_Id")
    quarter_data = cur.fetchall()
    quarters = {quarter[1]: quarter[0] for quarter in quarter_data}

    for element in data['Weekly Adjusted Time Series']:
        year = element.split("-")[0]
        month = element.split("-")[1]
        day = element.split("-")[2]
        valid_dates = create_valid_dates([1,4,7,10])
        date = '{}-{}'.format(month,day)
        open = data['Weekly Adjusted Time Series'][element]['1. open']
        high = data['Weekly Adjusted Time Series'][element]['2. high']
        low = data['Weekly Adjusted Time Series'][element]['3. low']
        close = data['Weekly Adjusted Time Series'][element]['4. close']
        volume = data['Weekly Adjusted Time Series'][element]['6. volume']
        cur.execute("INSERT INTO {} (id TEXT, date TEXT, open INTEGER, high INTEGER, low INTEGER, close INTEGER, volume INTEGER) VALUES (?, ?, ?, ?, ?, ?, ?)", (date, element, open, high, low, close, volume).format(company))
    conn.commit()

load_the_data("IBM", ibm_data, 2022, "01 ", cur, conn)


def find_open_close_avg(data):
    for element in data['Weekly Adjusted Time Series']:
        year = element.split("-")[0]
        month = element.split("-")[1]
        # print(element,"\n")
        open = data['Weekly Adjusted Time Series'][element]['1. open']
        close = data['Weekly Adjusted Time Series'][element]['4. close']
        # for value in data['Weekly Adjusted Time Series'][element]:
        #     for number in data['Weekly Adjusted Time Series'][element][value]
        #     print(value,'\n')
        # print('the element is:', element,'\n')


# print(find_open_close_avg(ibm_data))

# def get_sp500_data(url):
#     r = requests.get(url)
#     data = r.json()
#     print(data)
#     return data
# url = "http://api.marketstack.com/v1/eod?access_key=2ce5cc310401da10b32ed56816d0c129&symbols=DJIA"

# get_sp500_data(url)
