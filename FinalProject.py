import requests
import json

def get_data(symbol):
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={}&apikey=BL1X42CD6CU2F397".format(symbol)
    r = requests.get(url)
    data = r.json()
    return data

ibm_data = get_data("IBM")
microsoft = get_data("MSFT")
oracle = get_data("ORCL")

print(ibm_data)
