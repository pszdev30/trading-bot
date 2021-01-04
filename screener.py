import pandas as pd
import requests
import finviz
from globals import *


INDICATORS = 8


def generate_tickers():
    csv = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp = csv[0]['Symbol']
    sp.to_csv('S&P500.csv', columns=['Symbol'])


def valid(values):
    for value in values:
        if value == '-':
            return -1
    return 1


def volume_check(avg_volume):
    if 'K' in avg_volume:
        return False

    if float(avg_volume[:-1]) < 10:
        return False
    return True


def clean(value):
    if '%' in value:
        value = value[:-1]
    if '-' in value:
        stripped_value = ''.join(
            char for char in value if char != '-')
        value = float(stripped_value) * -1

    return float(value)


def screener():
    sp = pd.read_csv('S&P500.csv')
    tickers = sp['Symbol']
    points_list = dict()

    for ticker in tickers:
        ticker_info = finviz.get_stock(ticker)
        points = 0
        values = [ticker_info['EPS next 5Y'], ticker_info['EPS past 5Y'], ticker_info['Sales past 5Y'],
                  ticker_info['Debt/Eq'], ticker_info['Profit Margin'], ticker_info['PEG'], ticker_info['Avg Volume'], ticker_info['Recom']]

        if valid(values) != -1:
            next_five_year_eps = clean(
                values[0])

            if next_five_year_eps >= 5:
                points += 1

            past_five_year_eps = clean(
                values[1])

            if past_five_year_eps >= 5:
                points += 1

            five_year_sales = clean(
                values[2])

            if five_year_sales >= 5:
                points += 1

            debt_equity_ratio = clean(values[3])

            if debt_equity_ratio <= 1.25:
                points += 1

            profit_margin = clean(values[4])

            if profit_margin >= 3.5:
                points += 1

            peg = clean(values[5])

            if peg <= 2.5:
                points += 1

            avg_volume = values[6]

            if volume_check(avg_volume):
                points += 1

            mean_recommendation = values[7]

            if float(mean_recommendation) < 3:
                points += 1

            points_list[ticker] = points / INDICATORS

    points_list = sorted(points_list.items(),
                         key=lambda x: x[1], reverse=True)

    screened_tickers = [ticker for ticker,
                        rating in points_list if rating > .5]

    return 1


# generate_tickers()
print(screener())
