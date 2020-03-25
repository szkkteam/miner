from datetime import date, timedelta

from miner.sofascore import SofaHandler

def fetch_match():
    match_id = 7828232

    handler = SofaHandler(config={'multithreading': False})
    result, _ = handler.fetch_matches(match_id)
    print(len(result.keys()))

def fetch_dates():
    # Scrapping from today untill tomorrow
    start_date = date(2019,5,2)

    handler = SofaHandler(config={'multithreading': False})
    result, _ = handler.fetch_dates(start=start_date)
    print(result)


def main():
    #fetch_match()
    fetch_dates()



if __name__ == '__main__':
    main()