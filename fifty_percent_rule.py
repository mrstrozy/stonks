#!/usr/bin/env python3


import yfinance
from argparse           import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from csv                import DictReader
from datetime           import date

class Ticker:
    def __init__(self,
                 symbol: str,
                 ):
        print(symbol)
        self.ticker = yfinance.Ticker(symbol)
    
    def __str__(self,
                ):
        return self.ticker.ticker
    
    def get_current_price(self,
                          ):
        return self.ticker.history()['Close'][-1]
    
    def get_daily_history(self,
                          days_ago: None,
                          ):
        return self.get_history(days_ago=days_ago, interval='1d')
    
    def get_fifty_percent_level(self,
                                interval='wk',
                                ):
        
        if interval == 'wk':
            data = self.get_weekly_history(weeks_ago=1)
        else:
            data = self.get_daily_history()

        if len(data) < 2:
            return 0

        wk = data.get(list(data.keys())[0])
        return wk.get('Low') + (wk.get('High') - wk.get('Low'))/2


    def get_history(self,
                    days_ago:      int  = None,
                    ensure_weekly: bool = False,
                    interval:      str  = '1d',
                    ):
        data = {}
        try:
            history = self.ticker.history(interval=interval)
        except Exception as e:
            msg = f'Err when fetching history for {self}'
            print(msg)
            return data
        

        for category, datadict in history.items():
            for timestamp, metric in datadict.items():
                if any([
                    # weekly metric
                    ensure_weekly and timestamp.weekday() != 0,
                    # not within the day window
                    days_ago and (timestamp.now() - timestamp).days // days_ago != 0, 
                ]):
                    continue

                if timestamp in data:
                    data[timestamp][category] = metric
                else:
                    data[timestamp] = {category: metric}
        
        return data

    def get_weekly_history(self,
                           weeks_ago: int = None,
                           ):
        days_ago = (7*(weeks_ago+1))-1 if weeks_ago else None
        return self.get_history(days_ago=days_ago,
                                ensure_weekly=True,
                                interval='1wk',
                                )
    
    def is_in_fifty_percent_rule(self,
                                 interval: str = 'wk'):
        level = self.get_fifty_percent_level(interval=interval)

        if level:
            cur_price    = self.get_current_price()
            week_history = self.get_weekly_history(weeks_ago=1)
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
