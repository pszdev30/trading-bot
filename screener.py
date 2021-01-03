import pandas as pd
import requests
import finviz
from globals import *


# fundamental indicators: Debt/Equity ratio, PEG, Sales past 5Y, EPS next 5Y, EPS past 5Y, Profit Margin


def generate_tickers():
    csv = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp = csv[0]['Symbol']
    sp.to_csv('S&P500.csv', columns=['Symbol'])


def valid(values):
    for value in values:
        if value == '-':
            return -1


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
                  ticker_info['Debt/Eq'], ticker_info['Profit Margin'], ticker_info['PEG']]

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

            points_list[ticker] = points / 6

    # if next_five_year_eps > 5 and past_five_year_eps > 5 and five_year_sales > 5 and debt_equity_ratio < 1.25 and profit_margin > 3.5 and peg < 2.5:

    points_list = sorted(points_list.items(),
                         key=lambda x: x[1], reverse=True)[:80]

    return len(points_list), points_list

    # for ticker, rating in points_list.items():
    #     screened_tickers.append(ticker)

    return 1

    # return [ticker for ticker in tickers if int(finviz.get_stock(ticker)['EPS next 5Y']) > 0]


# generate_tickers()
print(screener())
