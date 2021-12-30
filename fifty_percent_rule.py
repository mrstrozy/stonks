#!/usr/bin/env python3


import yfinance
import pandas
from argparse           import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from csv                import DictReader
from datetime           import date

class Ticker:
    def __init__(self,
                 symbol: str,
                 ):
        self.ticker  = yfinance.Ticker(symbol)
        self.history = None
    
    def __str__(self,
                ):
        return self.ticker.ticker
    
    def get_current_price(self,
                          ):
        return self.ticker.history()['Close'][-1]
    
    def get_daily_history(self,
                          period='1mo',
                          ):
        return self.get_history(interval='1d', period=period, refresh=True)
    
    def get_fifty_percent_level(self,
                                interval='wk',
                                ):
        
        if interval == 'mo':
            func = self.get_monthly_history
        if interval == 'wk':
            func = self.get_weekly_history
        else:
            func = self.get_daily_history

        data = func()

        if len(data) < 2:
            return 0

        return data['Low'][-2] + (data['High'][-2] - data['Low'][-2])/2

    def get_history(self,
                    interval: str  = '1d',
                    period:   str  = '1mo',
                    refresh:  bool = False,
                    ):
        if self.history is None or refresh:
            try:
                self.history = self.ticker.history(interval=interval, period=period)
            except Exception as e:
                msg = f'Err when fetching history for {self}'
                print(msg)
        return self.history

    def get_monthly_history(self,
                            period='6mo',
                            ):
        history = self.get_history(interval='1mo',
                                   period=period,
                                   refresh=True,
                                   )
        for date in list(history.index.values): # leave the last entry
            if pandas.to_datetime(date).day != 1:
                history = history.drop(date)

        return history

    def get_weekly_history(self,
                           period='2wk',
                           ):
        return self.get_history(interval='1wk',
                                period=period,
                                refresh=True,
                                )
    
    def is_in_fifty_percent_rule(self,
                                 interval: str = 'wk'):
        level = self.get_fifty_percent_level(interval=interval)

        if level:
            try:
                cur_price    = self.get_current_price()
            except Exception as e:
                return False
            week_history = self.get_weekly_history(weeks_ago=1)
            print(f'{self} - {len(week_history)}')
            if len(week_history) > 1:
                last_week    = week_history[list(week_history)[0]]
                this_week    = week_history[list(week_history)[1]]
                last_week_low  = last_week.get('Low')
                last_week_high = last_week.get('High')
                this_week_low  = this_week.get('Low')
                this_week_high = this_week.get('High')

                if any([
                    this_week_low < last_week_low and cur_price > level,
                    this_week_high > last_week_high and cur_price < level,
                ]):
                    return True
        return False

def parse_args():
    parser = ArgumentParser()

    parser.add_argument('-tf', '--ticker-file',
                        action='store',
                        help='CSV file with tickers',
                        required=True,
                        )

    return parser.parse_args()

def read_ticker_file(filename):
    if '.csv' in filename:
        with open(filename, 'r') as f:
            reader = DictReader(f)
            return [r['Symbol'] for r in reader]
    else: # expect a txt file with a list line by line
        with open(filename, 'r') as f:
            return [l.strip() for l in f.readlines()]

def main():
    args = parse_args()

    tickers = read_ticker_file(args.ticker_file)
    # fp = [t for t in tickers[:200] if Ticker(t).is_in_fifty_percent_rule()]
    # print(fp)
    with ThreadPoolExecutor() as executor:
        data = {
            t: executor.submit(Ticker(t).is_in_fifty_percent_rule)
            for t in tickers
        }
    
    data = {k: v.result() for k, v in data.items() if v.result()}
    print(data)
    
    


if __name__ == '__main__':
    main()
