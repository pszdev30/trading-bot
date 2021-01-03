import pandas as pd
import requests
import finviz


# fundamental indicators: Debt/Equity ratio, PEG, Sales past 5Y, EPS next 5Y, EPS past 5Y

def generate_tickers():
    csv = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp = csv[0]['Symbol']
    sp.to_csv('S&P500.csv', columns=['Symbol'])


def clean(value):
    if value == '-':
        return None

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
    screened_tickers = []

    for ticker in tickers:
        ticker_info = finviz.get_stock(ticker)
        next_five_year_eps = clean(
            ticker_info['EPS next 5Y'])
        past_five_year_eps = clean(
            ticker_info['EPS past 5Y'])
        five_year_sales = clean(
            ticker_info['Sales past 5Y'])
        debt_equity_ratio = clean(ticker_info['Debt/Eq'])
        peg = clean(ticker_info['PEG'])

        if next_five_year_eps != None and next_five_year_eps > 5 and past_five_year_eps != None and past_five_year_eps > 5 and five_year_sales != None and five_year_sales > 5 and debt_equity_ratio != None and debt_equity_ratio < 1.2 and peg != None and peg > 1:
            screened_tickers.append(ticker)

    return len(screened_tickers)

    # return [ticker for ticker in tickers if int(finviz.get_stock(ticker)['EPS next 5Y']) > 0]


# generate_tickers()
print(screener())
