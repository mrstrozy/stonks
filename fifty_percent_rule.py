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

    def _history_func_map(self,
                  interval: str,
                  ):
        if interval == 'mo':
            func = self.get_monthly_history
        elif interval == 'wk':
            func = self.get_weekly_history
        elif interval == 'd':
            func = self.get_daily_history
        else:
            raise Exception(f'{interval} not a valid interval')
        return func
    
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
        elif interval == 'wk':
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
        for date in list(history.index.values):
            if pandas.to_datetime(date).day != 1:
                history = history.drop(date)

        return history

    def get_weekly_history(self,
                           period='2wk',
                           ):
        history = self.get_history(interval='1wk',
                                   period=period,
                                   refresh=True,
                                   )
        for date in list(history.index.values):
            if pandas.to_datetime(date).day_of_week != 0:
                history = history.drop(date)

        return history

    
    def is_in_fifty_percent_rule(self,
                                 interval: str = 'wk'):
        level = self.get_fifty_percent_level(interval=interval)

        if level:
            history = self._history_func_map(interval)()
            if len(history) > 1:
                last_low  = history['Low'][-2]
                last_high = history['High'][-2]
                this_low  = history['Low'][-1]
                this_high = history['High'][-1]

                if any([
                    this_low < last_low and this_high > level,
                    this_high > last_high and this_low < level,
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
        weekly_data = {
            t: executor.submit(Ticker(t).is_in_fifty_percent_rule)
            for t in tickers
        }
        monthly_data = {
            t: executor.submit(Ticker(t).is_in_fifty_percent_rule, interval='mo')
            for t in tickers
        }
    
    weekly_data = {k: v.result() for k, v in weekly_data.items() if v.result()}
    monthly_data = {k: v.result() for k, v in monthly_data.items() if v.result()}
    both = list(set(weekly_data.keys()) & set(monthly_data.keys()))


    output = ''
    output += 'Weekly:\n  ' + '\n  '.join(weekly_data.keys())
    output += '\nMonthly:\n  ' + '\n  '.join(monthly_data.keys())
    output += '\nBoth:\n  ' + '\n  '.join(both)
    print(output)
    
    


if __name__ == '__main__':
    main()
