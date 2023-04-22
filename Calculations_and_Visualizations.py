import requests
import os
import sqlite3
import csv
import unittest
import plotly.graph_objects as go
import matplotlib.pyplot as plt

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# Data Average Calculation 

def avg_function(cur, conn):
    cur.execute("SELECT ROUND(AVG(openM),3), ROUND(AVG(closeM),3) FROM MSFT_data")
    fetched_MSFT_average = cur.fetchall()
    # print(fetched_MSFT_average)
    cur.execute("SELECT ROUND(AVG(openO),3), ROUND(AVG(closeO),3) FROM ORCL_data")
    fetched_ORCL_average = cur.fetchall()
    cur.execute("SELECT ROUND(AVG(open),3), ROUND(AVG(close),3) FROM SP500_data")
    fetched_SP500_average = cur.fetchall()
    cur.execute("SELECT ROUND(AVG(score), 3) FROM MSFT_rating")
    fetched_MSFT_rating_average = cur.fetchall()
    cur.execute("SELECT ROUND(AVG(score), 3) FROM ORCL_rating")
    fetched_ORCL_rating_average = cur.fetchall()
    conn.commit()
    return list([fetched_MSFT_average, fetched_SP500_average, fetched_ORCL_average, fetched_MSFT_rating_average, fetched_ORCL_rating_average])

# Writing to CSV file of all data 
def write_file(filename, average_data):
    file_open = open(filename , 'w')
    csv_writer = csv.writer(file_open)
    csv_writer.writerows(average_data)
    file_open.close()
    return None

# Joined Tables Calculation of MSFT_data and ORCL_data
def join_tables(cur, conn):
    cur.execute("SELECT MSFT_data.dateM, MSFT_data.openM, MSFT_data.closeM, ORCL_data.openO, ORCL_data.closeO FROM MSFT_data JOIN ORCL_data ON MSFT_data.dateM = ORCL_data.dateO")

    tuples2 = cur.fetchall()
    conn.commit()
    return tuples2

def joined_calculation(tuple_list):
    newlst = []
    for tuple in tuple_list:
        date = tuple[0]
        msft_open = tuple[1]
        msft_close = tuple[2]
        orcl_open = tuple[3]
        orcl_close = tuple[4]

        msft_avg = (msft_open + msft_close)/2
        orcl_avg = (orcl_open + orcl_close)/2
        overall_avg = round(((msft_avg + orcl_avg)/2), 2)
        newlst.append((date, overall_avg))
    return newlst


# SP500_data Scatterplot
def sp_scatter(cur, conn):
    cur.execute("SELECT open from SP500_data")
    fetched_open = cur.fetchall()
    open_list = list(fetched_open)
    plt.plot(open_list, color = 'green', linestyle = 'dotted')
    plt.title('Open Prices')
    plt.show()

# Plot of Joined Data 
def joined_plot(newlst_returned):
    dates = []
    averages = []
    for element in newlst_returned:
        dates.append(element[0])
        averages.append(element[1])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=averages, mode='lines'))
    fig.update_layout(title='Average Highs', xaxis_title='Dates', yaxis_title='Averages')
    fig.show()


def final_quarter(cur, conn):
    cur.execute("SELECT idM FROM MSFT_data")
    numbers = cur.fetchall()
    count1 = 0
    count2 = 0
    count3 = 0
    count4 = 0
    for element in numbers:
        if element[0] == 1:
            count1+=1
        if element[0] == 2:
            count2+= 1
        elif element[0] == 3:
            count3+= 1
        else:
            count4+= 1
    return [count1, count2, count3, count4]


# Quarters Bar Graph
def make_bar_graph(cur, conn):
    counts = final_quarter(cur, conn)
    fig = go.Figure(data=[
        go.Bar(name = 'Totals Per Quarter', x=[1,2,3,4], y=counts, marker_color = 'rgb(160, 32, 240)')])
    title_str = "Total Data Points Per Quarter"
    fig.update_layout(
        title = title_str,
        xaxis_title = "Quarters",
        yaxis_title = "Quantities"
    )
    fig.show()
    counts = final_quarter(cur, conn)
  
def get_high_data(cur, conn):
    cur.execute("SELECT highM FROM MSFT_data")
    data_high = cur.fetchall()
    listed_elements = []
    for element in data_high:
        listed_elements.append(element[0])
    cur.execute("SELECT dateM FROM MSFT_data")
    dates = cur.fetchall()
    listed_dates = []
    for value in dates:
        listed_dates.append(value[0])
    return listed_elements, listed_dates

# Scatterplot of Average High Values 
def make_scatter_plot(listed_elements , listed_dates):
    traces = go.Scatter(x=listed_dates, y=listed_elements, mode='markers')
    data = [traces]
    layout = go.Layout(
        title='Average High Value For Different Dates',
        xaxis=dict(title='Dates'),
        yaxis=dict(title='High Values')
    )
    fig = go.Figure(data=data, layout=layout)
    fig.show()


# Pie Graphs
def pie_chart(company):
    if company == "MSFT":
        labels = ['5 - Strong Invest']
        size = [100]
        colors = ['green']
        plt.pie(size, labels=labels, colors=colors)
        plt.axis('equal')
        plt.show()
    elif company == "ORCL":
        labels = ['3 - Neutral']
        size = [100]
        colors = ['red']
        plt.pie(size, labels=labels, colors=colors)
        plt.axis('equal')
        plt.show()
    

def main():
    cur, conn = open_database("final206.db")

    tuple_list = join_tables(cur, conn)
    joined_data = joined_calculation(tuple_list)
    write_file("joined_data.csv", joined_data)

    data = avg_function(cur, conn)
    write_file("averages.csv", data)
   
    listed_elements,listed_dates = get_high_data(cur, conn)

    make_scatter_plot(listed_elements , listed_dates)
    make_bar_graph(cur, conn)
    get_high_data(cur, conn)
    make_bar_graph(cur, conn)
    pie_chart("ORCL")
    pie_chart("MSFT")
    sp_scatter(cur, conn)

main()
